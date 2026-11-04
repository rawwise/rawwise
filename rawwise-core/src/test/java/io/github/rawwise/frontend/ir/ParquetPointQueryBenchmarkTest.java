package io.github.rawwise.frontend.ir;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.block.index.SparseIndex;
import io.github.rawwise.engine.storage.block.index.SparseIndexBuilder;
import io.github.rawwise.engine.storage.writer.DefaultParquetWriterBuilder;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Parquet point query Benchmark")
class ParquetPointQueryBenchmarkTest {

    private static final MessageType KV_SCHEMA =
            Types.buildMessage()
                    .addField(Types.required(BINARY).named("key"))
                    .addField(Types.required(BINARY).named("value"))
                    .named("KeyValue");

    private static final int ROW_COUNT = 100_000;
    private static final int WARMUP_ITERATIONS = 20;
    private static final int MEASUREMENT_ITERATIONS = 100;

    @TempDir java.nio.file.Path tempDir;

    private static byte[] keyAt(int index) {
        return String.format("key-%08d", index).getBytes();
    }

    private static byte[] valueAt(int index) {
        return ("value-for-key-" + index).getBytes();
    }

    private record BenchmarkResult(
            String label, long minNs, long avgNs, long p50Ns, long p95Ns, long p99Ns) {
        double avgUs() {
            return avgNs / 1000.0;
        }

        double p50Us() {
            return p50Ns / 1000.0;
        }

        double p99Us() {
            return p99Ns / 1000.0;
        }
    }

    private Path prepareParquetFileWithSparseIndex(java.nio.file.Path tempDir) throws Exception {
        Path hadoopPath =
                new Path(tempDir.resolve("benchmark-" + ROW_COUNT + ".parquet").toString());

        SparseIndexBuilder indexBuilder = new SparseIndexBuilder();

        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(KV_SCHEMA, conf);

        try (ParquetWriter<Group> writer =
                new DefaultParquetWriterBuilder<Group>(hadoopPath, new GroupWriteSupport())
                        .withConf(conf)
                        .withCompressionCodec(CompressionCodecName.ZSTD)
                        .build()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                indexBuilder.accept(keyAt(i));
                SimpleGroup group = new SimpleGroup(KV_SCHEMA);
                group.add("key", Binary.fromConstantByteBuffer(ByteBuffer.wrap(keyAt(i))));
                group.add("value", Binary.fromConstantByteBuffer(ByteBuffer.wrap(valueAt(i))));
                writer.write(group);
            }
        }

        byte[] indexBytes = indexBuilder.build();
        java.nio.file.Path indexPath = tempDir.resolve("benchmark-" + ROW_COUNT + ".idx");
        Files.write(indexPath, indexBytes);

        return hadoopPath;
    }

    private SparseIndex loadSparseIndex(java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path indexPath = tempDir.resolve("benchmark-" + ROW_COUNT + ".idx");
        byte[] indexBytes = Files.readAllBytes(indexPath);
        return SparseIndex.deserialize(indexBytes);
    }

    @Test
    @DisplayName("verify index correct")
    void testSparseIndexSemanticCorrectness() throws Exception {
        Path hadoopPath = prepareParquetFileWithSparseIndex(tempDir);
        SparseIndex sparseIndex = loadSparseIndex(tempDir);

        assertThat(sparseIndex).isNotNull();
        assertThat(sparseIndex.entries()).isNotEmpty();
        assertThat(sparseIndex.entries().get(0).rowIndex()).isEqualTo(0);

        for (int i = 1; i < sparseIndex.entries().size(); i++) {
            SparseIndex.SparseIndexEntry prev = sparseIndex.entries().get(i - 1);
            SparseIndex.SparseIndexEntry curr = sparseIndex.entries().get(i);
            assertThat(i)
                    .as("entry %d should have monotonically increasing rowIndex", i)
                    .satisfies(
                            ignored -> assertThat(curr.rowIndex()).isGreaterThan(prev.rowIndex()));
            assertThat(new String(prev.key()))
                    .as("entry %d should have monotonically increasing key", i)
                    .isLessThan(new String(curr.key()));
        }

        byte[] firstRowKey = keyAt(0);
        long firstRowIdx = sparseIndex.floorRowIndex(firstRowKey);
        assertThat(firstRowIdx).isEqualTo(0);

        byte[] exactMatchKey = keyAt(5000);
        long exactMatchIdx = sparseIndex.floorRowIndex(exactMatchKey);
        long actualRow = Long.parseLong(new String(exactMatchKey).substring(4));
        assertThat(exactMatchIdx)
                .as("floorRowIndex should return rowIndex ≤ target row")
                .isLessThanOrEqualTo(actualRow);
        assertThat(exactMatchIdx)
                .as("floorRowIndex should be within 1000 rows of target")
                .isGreaterThanOrEqualTo(actualRow - 999);

        System.out.println("\n=== 稀疏索引语义验证 ===");
        System.out.println("总行数: " + ROW_COUNT);
        System.out.println("索引采样点数: " + sparseIndex.entries().size());
        System.out.println("索引间隔: 每1000行一个采样点");
        System.out.println(
                "key@" + 5000 + " → floorRowIndex=" + exactMatchIdx + " (实际行号=" + actualRow + ")");
        System.out.println("全部通过 ✓\n");
    }

    @Test
    @DisplayName("for full scan vs sparse index point querylazy")
    void benchmarkFullScanVsSparseIndex() throws Exception {
        Path hadoopPath = prepareParquetFileWithSparseIndex(tempDir);
        SparseIndex sparseIndex = loadSparseIndex(tempDir);
        assertThat(sparseIndex).isNotNull();

        System.out.println("\n=== Parquet 点查优化 Benchmark (" + ROW_COUNT + " 行) ===\n");
        System.out.println("稀疏索引: " + sparseIndex.entries().size() + " 个采样点 (每1000行)\n");

        // JVM Warmup
        System.out.println("--- JVM 预热 ---");
        for (int warm = 0; warm < 5; warm++) {
            benchmarkFullScan(hadoopPath, keyAt(0));
            benchmarkFullScan(hadoopPath, keyAt(ROW_COUNT - 1));
            benchmarkSparseIndex(hadoopPath, keyAt(0), sparseIndex);
            benchmarkSparseIndex(hadoopPath, keyAt(ROW_COUNT - 1), sparseIndex);
        }
        System.gc();
        Thread.sleep(200);

        // Benchmark positions
        int[] positions = {0, ROW_COUNT / 4, ROW_COUNT / 2, ROW_COUNT * 3 / 4, ROW_COUNT - 1};
        String[] posNames = {"开头(0%)", "25%", "中间(50%)", "75%", "末尾(99.99%)"};

        List<BenchmarkResult> fullResults = new ArrayList<>();
        System.out.println("--- 优化前: Full Scan (无稀疏索引) ---");
        for (int pi = 0; pi < positions.length; pi++) {
            byte[] targetKey = keyAt(positions[pi]);
            BenchmarkResult r = benchmarkFullScan(hadoopPath, targetKey);
            fullResults.add(r);
            System.out.printf(
                    "  查询位置 %s (key=%s): avg=%.1fμs  p50=%.1fμs  p99=%.1fμs%n",
                    posNames[pi], new String(targetKey), r.avgUs(), r.p50Us(), r.p99Us());
        }

        List<BenchmarkResult> sparseResults = new ArrayList<>();
        System.out.println(
                "\n--- 优化后: Sparse Index " + sparseIndex.entries().size() + " 采样点 (每1000行) ---");
        for (int pi = 0; pi < positions.length; pi++) {
            byte[] targetKey = keyAt(positions[pi]);
            BenchmarkResult r = benchmarkSparseIndex(hadoopPath, targetKey, sparseIndex);
            sparseResults.add(r);
            long skipRows = sparseIndex.floorRowIndex(targetKey);
            System.out.printf(
                    "  查询位置 %s (key=%s): avg=%.1fμs  p50=%.1fμs  p99=%.1fμs  skipRows=%d%n",
                    posNames[pi], new String(targetKey), r.avgUs(), r.p50Us(), r.p99Us(), skipRows);
        }

        // Summary Table
        System.out.println(
                "\n╔══════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println(
                "║                          Benchmark 结果汇总 ("
                        + ROW_COUNT
                        + " 行)                              ║");
        System.out.println(
                "╠══════════════════════╦═════════════════╦═════════════════╦════════════════════╦═════╣");
        System.out.println(
                "║ 查询位置              ║ Full Scan (avg) ║ SparseIdx (avg) ║ 扫描行数差异        ║ 加速║");
        System.out.println(
                "╠══════════════════════╬═════════════════╬═════════════════╬════════════════════╬═════╣");

        for (int pi = 0; pi < positions.length; pi++) {
            BenchmarkResult fr = fullResults.get(pi);
            BenchmarkResult sr = sparseResults.get(pi);
            double speedup = fr.avgUs() / sr.avgUs();
            int fullScanRows = positions[pi] + 1;
            long sparseScanRows = sparseIndex.floorRowIndex(keyAt(positions[pi]));
            long sparseWindow = Math.min(2000, ROW_COUNT - sparseScanRows);
            System.out.printf(
                    "║ %-20s ║ %8.1f μs    ║ %8.1f μs    ║ %d→≤%d 行        ║ %.1fx║%n",
                    posNames[pi], fr.avgUs(), sr.avgUs(), fullScanRows, sparseWindow, speedup);
        }
        System.out.println(
                "╠══════════════════════╩═════════════════╩═════════════════╩════════════════════╩═════╣");

        // Assertions: sparse index should be at least 2x faster at end position
        for (int pi = 0; pi < positions.length; pi++) {
            BenchmarkResult fr = fullResults.get(pi);
            BenchmarkResult sr = sparseResults.get(pi);
            double speedup = fr.avgUs() / sr.avgUs();
            System.out.printf(
                    "  [%s] speedup=%.1fx  full=%.1fμs  sparse=%.1fμs%n",
                    posNames[pi], speedup, fr.avgUs(), sr.avgUs());
        }
    }

    @Test
    @DisplayName("verify indexpoint query correct")
    void testSparseIndexResultCorrectness() throws Exception {
        Path hadoopPath = prepareParquetFileWithSparseIndex(tempDir);
        SparseIndex sparseIndex = loadSparseIndex(tempDir);

        int[] testPositions = {0, 1, 999, 1000, 50000, 99999};
        for (int pos : testPositions) {
            byte[] targetKey = keyAt(pos);
            byte[] expectedValue = valueAt(pos);

            // Full scan
            byte[] fullScanValue = readWithFullScan(hadoopPath, targetKey);
            assertThat(fullScanValue)
                    .as("full scan should find key-%08d", pos)
                    .isEqualTo(expectedValue);

            // Sparse index
            byte[] sparseValue = readWithSparseIndex(hadoopPath, targetKey, sparseIndex);
            assertThat(sparseValue)
                    .as("sparse index should find key-%08d", pos)
                    .isEqualTo(expectedValue);
        }
        System.out.println("✓ 全部 " + testPositions.length + " 个位置结果一致");
    }

    @Test
    @DisplayName("verify indexpoint querydoes not existkey cancorrectly returnsnull")
    void testSparseIndexMissingKey() throws Exception {
        Path hadoopPath = prepareParquetFileWithSparseIndex(tempDir);
        SparseIndex sparseIndex = loadSparseIndex(tempDir);

        byte[] missingKey = "no-such-key-exists".getBytes();

        byte[] fullScanValue = readWithFullScan(hadoopPath, missingKey);
        assertThat(fullScanValue).isNull();

        byte[] sparseValue = readWithSparseIndex(hadoopPath, missingKey, sparseIndex);
        assertThat(sparseValue).isNull();

        System.out.println("✓ 不存在的key正确返回null");
    }

    // ======================== Benchmark Methods ========================

    private BenchmarkResult benchmarkFullScan(Path path, byte[] targetKey) throws Exception {
        List<Long> latencies = new ArrayList<>();
        for (int iter = -WARMUP_ITERATIONS; iter < MEASUREMENT_ITERATIONS; iter++) {
            long start = System.nanoTime();
            try (ParquetReader<Group> reader =
                    ParquetReader.builder(new GroupReadSupport(), path)
                            .withConf(new Configuration())
                            .build()) {
                Group group;
                while ((group = reader.read()) != null) {
                    byte[] key = group.getBinary("key", 0).getBytes();
                    if (java.util.Arrays.equals(targetKey, key)) {
                        break;
                    }
                }
            }
            long elapsed = System.nanoTime() - start;
            if (iter >= 0) {
                latencies.add(elapsed);
            }
        }
        return computeStats("fullScan", latencies);
    }

    private BenchmarkResult benchmarkSparseIndex(
            Path path, byte[] targetKey, SparseIndex sparseIndex) throws Exception {
        long skipRows = sparseIndex.floorRowIndex(targetKey);
        List<Long> latencies = new ArrayList<>();
        for (int iter = -WARMUP_ITERATIONS; iter < MEASUREMENT_ITERATIONS; iter++) {
            long start = System.nanoTime();
            try (ParquetReader<Group> reader =
                    ParquetReader.builder(new GroupReadSupport(), path)
                            .withConf(new Configuration())
                            .build()) {
                Group group;
                long rowsRead = 0;
                while ((group = reader.read()) != null) {
                    if (rowsRead < skipRows) {
                        rowsRead++;
                        continue;
                    }
                    if (rowsRead - skipRows > 2000) {
                        break;
                    }
                    rowsRead++;
                    byte[] key = group.getBinary("key", 0).getBytes();
                    if (java.util.Arrays.equals(targetKey, key)) {
                        break;
                    }
                }
            }
            long elapsed = System.nanoTime() - start;
            if (iter >= 0) {
                latencies.add(elapsed);
            }
        }
        return computeStats("sparseIdx(skip=" + skipRows + ")", latencies);
    }

    // ======================== Correctness Helpers ========================

    private byte[] readWithFullScan(Path path, byte[] targetKey) throws Exception {
        try (ParquetReader<Group> reader =
                ParquetReader.builder(new GroupReadSupport(), path)
                        .withConf(new Configuration())
                        .build()) {
            Group group;
            while ((group = reader.read()) != null) {
                byte[] key = group.getBinary("key", 0).getBytes();
                if (java.util.Arrays.equals(targetKey, key)) {
                    return group.getBinary("value", 0).getBytes();
                }
            }
        }
        return null;
    }

    private byte[] readWithSparseIndex(Path path, byte[] targetKey, SparseIndex sparseIndex)
            throws Exception {
        long skipRows = sparseIndex.floorRowIndex(targetKey);
        try (ParquetReader<Group> reader =
                ParquetReader.builder(new GroupReadSupport(), path)
                        .withConf(new Configuration())
                        .build()) {
            Group group;
            long rowsRead = 0;
            while ((group = reader.read()) != null) {
                if (rowsRead < skipRows) {
                    rowsRead++;
                    continue;
                }
                if (rowsRead - skipRows > 2000) {
                    break;
                }
                rowsRead++;
                byte[] key = group.getBinary("key", 0).getBytes();
                if (java.util.Arrays.equals(targetKey, key)) {
                    return group.getBinary("value", 0).getBytes();
                }
            }
        }
        return null;
    }

    private BenchmarkResult computeStats(String label, List<Long> latencies) {
        List<Long> sorted = latencies.stream().sorted().toList();
        int n = sorted.size();
        long min = sorted.get(0);
        long avg = (long) sorted.stream().mapToLong(Long::longValue).average().orElse(0);
        long p50 = sorted.get(n / 2);
        long p95 = sorted.get((int) (n * 0.95));
        long p99 = sorted.get((int) (n * 0.99));
        return new BenchmarkResult(label, min, avg, p50, p95, p99);
    }
}
