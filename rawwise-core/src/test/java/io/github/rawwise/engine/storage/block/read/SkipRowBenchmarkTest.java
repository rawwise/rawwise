package io.github.rawwise.engine.storage.block.read;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.junit.jupiter.api.Assertions.*;

import io.github.rawwise.engine.storage.writer.DefaultParquetWriterBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.impl.RowSkipReadStore;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.ParquetPointReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("SkipRow vs FullScan point query Benchmark")
class SkipRowBenchmarkTest {

    private static final String PK_COL = "__pk__";
    private static final int ROW_COUNT = 50_000;
    private static final int WARMUP = 10;
    private static final int MEASURE = 50;

    private static final MessageType SCHEMA =
            Types.buildMessage()
                    .addField(Types.required(BINARY).named(PK_COL))
                    .addField(Types.optional(BINARY).named("value"))
                    .named("SkipRowBenchSchema");

    private static java.nio.file.Path dataDir;
    private static Path parquetPath;

    @BeforeAll
    static void setup(@TempDir java.nio.file.Path tempDir) throws Exception {
        dataDir = tempDir;
        parquetPath = new Path(dataDir.resolve("skiprow-bench.parquet").toString());
        writeData(parquetPath);
    }

    private static byte[] pkBytes(int index) {
        return String.format("key-%08d", index).getBytes(StandardCharsets.UTF_8);
    }

    private static void writeData(Path path) throws IOException {
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(SCHEMA, conf);
        try (ParquetWriter<Group> writer =
                new DefaultParquetWriterBuilder<>(path, new GroupWriteSupport())
                        .withConf(conf)
                        .build()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                SimpleGroup group = new SimpleGroup(SCHEMA);
                group.add(PK_COL, Binary.fromConstantByteArray(pkBytes(i)));
                group.add(
                        "value",
                        Binary.fromConstantByteArray(
                                ("val-" + i).getBytes(StandardCharsets.UTF_8)));
                writer.write(group);
            }
        }
    }

    private ParquetPointReader<Group> createSeekableReader(Path path) throws IOException {
        org.apache.parquet.hadoop.util.HadoopInputFile inputFile =
                org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(path, new Configuration());
        ParquetFileReader fileReader = ParquetFileReader.open(inputFile);
        MessageType fileSchema = fileReader.getFooter().getFileMetaData().getSchema();
        Map<String, String> extraMeta =
                fileReader.getFooter().getFileMetaData().getKeyValueMetaData();
        long rowCount = fileReader.getFooter().getBlocks().get(0).getRowCount();

        GroupReadSupport readSupport = new GroupReadSupport();
        ReadSupport.ReadContext readContext =
                readSupport.init(new Configuration(), extraMeta, fileSchema);
        RecordMaterializer<Group> materializer =
                readSupport.prepareForRead(new Configuration(), extraMeta, fileSchema, readContext);

        ColumnIOFactory columnIOFactory = new ColumnIOFactory(true);
        MessageColumnIO columnIO =
                columnIOFactory.getColumnIO(readContext.getRequestedSchema(), fileSchema);

        var pageStore = fileReader.readRowGroup(0);

        RowSkipReadStore seekableStore =
                new RowSkipReadStore(
                        pageStore,
                        materializer.getRootConverter(),
                        fileSchema,
                        fileReader.getFooter().getFileMetaData().getCreatedBy());

        return new ParquetPointReader<>(
                columnIO,
                materializer,
                false,
                seekableStore,
                ColumnPath.fromDotString(PK_COL),
                rowCount);
    }

    private static record BenchResult(
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

    private BenchResult computeStats(String label, List<Long> latencies) {
        List<Long> sorted = latencies.stream().sorted().toList();
        int n = sorted.size();
        long min = sorted.get(0);
        long avg = (long) sorted.stream().mapToLong(Long::longValue).average().orElse(0);
        long p50 = sorted.get(n / 2);
        long p95 = sorted.get((int) (n * 0.95));
        long p99 = sorted.get((int) (n * 0.99));
        return new BenchResult(label, min, avg, p50, p95, p99);
    }

    @Test
    @DisplayName("SkipRow vs FullScan point querylazyfor")
    void benchmarkSkipRowVsFullScan() throws Exception {
        int[] positions = {0, ROW_COUNT / 4, ROW_COUNT / 2, ROW_COUNT * 3 / 4, ROW_COUNT - 1};
        String[] posNames = {"0%(开头)", "25%", "50%(中间)", "75%", "99.99%(末尾)"};

        System.out.println(
                "\n╔════════════════════════════════════════════════════════════════════════════════╗");
        System.out.printf(
                "║          SkipRow vs FullScan 点查 Benchmark (%,d 行)                       ║%n",
                ROW_COUNT);
        System.out.println(
                "╚════════════════════════════════════════════════════════════════════════════════╝\n");

        // Warmup
        for (int w = 0; w < 5; w++) {
            for (int pos : positions) {
                benchFullScan(parquetPath, pkBytes(pos));
                benchSkipRow(parquetPath, pkBytes(pos));
            }
        }
        System.gc();
        Thread.sleep(300);

        System.out.println("--- FullScan (逐行解码 + continue) ---");
        List<BenchResult> fullResults = new ArrayList<>();
        for (int pi = 0; pi < positions.length; pi++) {
            BenchResult r = benchFullScan(parquetPath, pkBytes(positions[pi]));
            fullResults.add(r);
            System.out.printf(
                    "  %s: avg=%7.1fμs  p50=%7.1fμs  p99=%7.1fμs%n",
                    posNames[pi], r.avgUs(), r.p50Us(), r.p99Us());
        }

        System.out.println("\n--- SkipRow (ParquetPointReader.seekTo + skipRow) ---");
        List<BenchResult> skipResults = new ArrayList<>();
        for (int pi = 0; pi < positions.length; pi++) {
            BenchResult r = benchSkipRow(parquetPath, pkBytes(positions[pi]));
            skipResults.add(r);
            System.out.printf(
                    "  %s: avg=%7.1fμs  p50=%7.1fμs  p99=%7.1fμs%n",
                    posNames[pi], r.avgUs(), r.p50Us(), r.p99Us());
        }

        System.out.println(
                "\n╔════════════════════╦═════════════════╦═════════════════╦══════════╦══════════╗");
        System.out.println(
                "║ 查询位置            ║ FullScan (avg)  ║ SkipRow (avg)   ║ 跳行数   ║ 加速比   ║");
        System.out.println(
                "╠════════════════════╬═════════════════╬═════════════════╬══════════╬══════════╣");
        for (int pi = 0; pi < positions.length; pi++) {
            BenchResult fr = fullResults.get(pi);
            BenchResult sr = skipResults.get(pi);
            double speedup = fr.avgUs() / sr.avgUs();
            int skipCount = positions[pi];
            System.out.printf(
                    "║ %-18s ║ %8.1f μs    ║ %8.1f μs    ║ %8d ║ %6.1fx  ║%n",
                    posNames[pi], fr.avgUs(), sr.avgUs(), skipCount, speedup);
        }
        System.out.println(
                "╚════════════════════╩═════════════════╩═════════════════╩══════════╩══════════╝");
    }

    @Test
    @DisplayName("SkipRow correct verify")
    void verifySkipRowCorrectness() throws Exception {
        int[] testPositions = {0, 1, 100, ROW_COUNT / 2, ROW_COUNT - 1};
        for (int pos : testPositions) {
            byte[] targetKey = pkBytes(pos);

            String fullScanVal = readFullScan(parquetPath, targetKey);
            String skipRowVal = readSkipRow(parquetPath, targetKey);

            assertNotNull(fullScanVal, "FullScan should find key at position " + pos);
            assertNotNull(skipRowVal, "SkipRow should find key at position " + pos);
            assertEquals(
                    fullScanVal,
                    skipRowVal,
                    "SkipRow and FullScan should return same value at position " + pos);
        }
        System.out.println("✓ 全部 " + testPositions.length + " 个位置 SkipRow 与 FullScan 结果一致");
    }

    // ======================== Wide Table Benchmark ========================

    private static final int WIDE_COLS = 8;
    private static final String[] WIDE_COL_NAMES = {"v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7"};

    private static final MessageType WIDE_SCHEMA = buildWideSchema();

    private static MessageType buildWideSchema() {
        var builder = Types.buildMessage().addField(Types.required(BINARY).named(PK_COL));
        for (String col : WIDE_COL_NAMES) {
            builder = builder.addField(Types.optional(BINARY).named(col));
        }
        return builder.named("WideTableSchema");
    }

    @Test
    @DisplayName("table (PK+8column) SkipRow vs FullScan lazyfor")
    void wideTableBenchmark(@TempDir java.nio.file.Path tempDir) throws Exception {
        Path widePath = new Path(tempDir.resolve("wide-bench.parquet").toString());
        writeWideData(widePath);

        int[] positions = {0, ROW_COUNT / 4, ROW_COUNT / 2, ROW_COUNT * 3 / 4, ROW_COUNT - 1};
        String[] posNames = {"0%(开头)", "25%", "50%(中间)", "75%", "99.99%(末尾)"};

        System.out.println(
                "\n╔════════════════════════════════════════════════════════════════════════════════╗");
        System.out.printf(
                "║    宽表 SkipRow vs FullScan (%,d 行, %d 列: PK + %d values)         ║%n",
                ROW_COUNT, 1 + WIDE_COLS, WIDE_COLS);
        System.out.println(
                "╚════════════════════════════════════════════════════════════════════════════════╝\n");

        // Warmup
        for (int w = 0; w < 5; w++) {
            for (int pos : positions) {
                benchWideFullScan(widePath, pkBytes(pos));
                benchWideSkipRow(widePath, pkBytes(pos));
            }
        }
        System.gc();
        Thread.sleep(300);

        System.out.println("--- FullScan (逐行解码 " + (1 + WIDE_COLS) + " 列) ---");
        List<BenchResult> fullResults = new ArrayList<>();
        for (int pi = 0; pi < positions.length; pi++) {
            BenchResult r = benchWideFullScan(widePath, pkBytes(positions[pi]));
            fullResults.add(r);
            System.out.printf(
                    "  %s: avg=%7.1fμs  p50=%7.1fμs  p99=%7.1fμs%n",
                    posNames[pi], r.avgUs(), r.p50Us(), r.p99Us());
        }

        System.out.println("\n--- SkipRow (只解码 PK 列，跳过其他 " + WIDE_COLS + " 列) ---");
        List<BenchResult> skipResults = new ArrayList<>();
        for (int pi = 0; pi < positions.length; pi++) {
            BenchResult r = benchWideSkipRow(widePath, pkBytes(positions[pi]));
            skipResults.add(r);
            System.out.printf(
                    "  %s: avg=%7.1fμs  p50=%7.1fμs  p99=%7.1fμs%n",
                    posNames[pi], r.avgUs(), r.p50Us(), r.p99Us());
        }

        System.out.println(
                "\n╔════════════════════╦═════════════════╦═════════════════╦══════════╦══════════╗");
        System.out.println(
                "║ 查询位置            ║ FullScan (avg)  ║ SkipRow (avg)   ║ 跳行数   ║ 加速比   ║");
        System.out.println(
                "╠════════════════════╬═════════════════╬═════════════════╬══════════╬══════════╣");
        for (int pi = 0; pi < positions.length; pi++) {
            BenchResult fr = fullResults.get(pi);
            BenchResult sr = skipResults.get(pi);
            double speedup = fr.avgUs() / sr.avgUs();
            int skipCount = positions[pi];
            System.out.printf(
                    "║ %-18s ║ %8.1f μs    ║ %8.1f μs    ║ %8d ║ %6.1fx  ║%n",
                    posNames[pi], fr.avgUs(), sr.avgUs(), skipCount, speedup);
        }
        System.out.println(
                "╚════════════════════╩═════════════════╩═════════════════╩══════════╩══════════╝");

        // Breakdown at end position
        System.out.println("\n--- 末尾位置开销分解 (seekTo=" + (ROW_COUNT - 1) + ") ---");
        benchWideSkipRowBreakdown(widePath, pkBytes(ROW_COUNT - 1));
    }

    private void writeWideData(Path path) throws IOException {
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(WIDE_SCHEMA, conf);
        Random rng = new Random(42);
        try (ParquetWriter<Group> writer =
                new DefaultParquetWriterBuilder<>(path, new GroupWriteSupport())
                        .withConf(conf)
                        .build()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                SimpleGroup group = new SimpleGroup(WIDE_SCHEMA);
                group.add(PK_COL, Binary.fromConstantByteArray(pkBytes(i)));
                for (String col : WIDE_COL_NAMES) {
                    byte[] val = new byte[64];
                    rng.nextBytes(val);
                    group.add(col, Binary.fromConstantByteArray(val));
                }
                writer.write(group);
            }
        }
    }

    private ParquetPointReader<Group> createWideSeekableReader(Path path) throws IOException {
        org.apache.parquet.hadoop.util.HadoopInputFile inputFile =
                org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(path, new Configuration());
        ParquetFileReader fileReader = ParquetFileReader.open(inputFile);
        MessageType fileSchema = fileReader.getFooter().getFileMetaData().getSchema();
        Map<String, String> extraMeta =
                fileReader.getFooter().getFileMetaData().getKeyValueMetaData();
        long rowCount = fileReader.getFooter().getBlocks().get(0).getRowCount();

        GroupReadSupport readSupport = new GroupReadSupport();
        ReadSupport.ReadContext readContext =
                readSupport.init(new Configuration(), extraMeta, fileSchema);
        RecordMaterializer<Group> materializer =
                readSupport.prepareForRead(new Configuration(), extraMeta, fileSchema, readContext);

        ColumnIOFactory columnIOFactory = new ColumnIOFactory(true);
        MessageColumnIO columnIO =
                columnIOFactory.getColumnIO(readContext.getRequestedSchema(), fileSchema);

        var pageStore = fileReader.readRowGroup(0);

        RowSkipReadStore seekableStore =
                new RowSkipReadStore(
                        pageStore,
                        materializer.getRootConverter(),
                        fileSchema,
                        fileReader.getFooter().getFileMetaData().getCreatedBy());

        return new ParquetPointReader<>(
                columnIO,
                materializer,
                false,
                seekableStore,
                ColumnPath.fromDotString(PK_COL),
                rowCount);
    }

    private BenchResult benchWideFullScan(Path path, byte[] targetKey) throws Exception {
        List<Long> latencies = new ArrayList<>();
        for (int iter = -WARMUP; iter < MEASURE; iter++) {
            long start = System.nanoTime();
            try (ParquetReader<Group> reader =
                    ParquetReader.builder(new GroupReadSupport(), path)
                            .withConf(new Configuration())
                            .build()) {
                Group group;
                while ((group = reader.read()) != null) {
                    byte[] pk = group.getBinary(PK_COL, 0).getBytes();
                    if (java.util.Arrays.equals(targetKey, pk)) break;
                }
            }
            long elapsed = System.nanoTime() - start;
            if (iter >= 0) latencies.add(elapsed);
        }
        return computeStats("wideFullScan", latencies);
    }

    private BenchResult benchWideSkipRow(Path path, byte[] targetKey) throws Exception {
        List<Long> latencies = new ArrayList<>();
        for (int iter = -WARMUP; iter < MEASURE; iter++) {
            long start = System.nanoTime();
            ParquetPointReader<Group> reader = createWideSeekableReader(path);
            int result = reader.seekTo(Binary.fromReusedByteArray(targetKey));
            if (result == 0) reader.read();
            long elapsed = System.nanoTime() - start;
            if (iter >= 0) latencies.add(elapsed);
        }
        return computeStats("wideSkipRow", latencies);
    }

    private void benchWideSkipRowBreakdown(Path path, byte[] targetKey) throws Exception {
        long createTotal = 0, seekTotal = 0, readTotal = 0;
        int samples = 10;
        for (int i = 0; i < samples; i++) {
            long t0 = System.nanoTime();
            ParquetPointReader<Group> reader = createWideSeekableReader(path);
            long t1 = System.nanoTime();
            int result = reader.seekTo(Binary.fromReusedByteArray(targetKey));
            long t2 = System.nanoTime();
            if (result == 0) reader.read();
            long t3 = System.nanoTime();
            createTotal += (t1 - t0);
            seekTotal += (t2 - t1);
            readTotal += (t3 - t2);
        }
        System.out.printf("  Reader 创建:  %.1f μs%n", createTotal / 1000.0 / samples);
        System.out.printf(
                "  seekTo():     %.1f μs  (PK 列逐行比较 %d 行)%n",
                seekTotal / 1000.0 / samples, ROW_COUNT - 1);
        System.out.printf("  read():       %.1f μs%n", readTotal / 1000.0 / samples);
    }

    private BenchResult benchFullScan(Path path, byte[] targetKey) throws Exception {
        List<Long> latencies = new ArrayList<>();
        for (int iter = -WARMUP; iter < MEASURE; iter++) {
            long start = System.nanoTime();
            try (ParquetReader<Group> reader =
                    ParquetReader.builder(new GroupReadSupport(), path)
                            .withConf(new Configuration())
                            .build()) {
                Group group;
                while ((group = reader.read()) != null) {
                    byte[] pk = group.getBinary(PK_COL, 0).getBytes();
                    if (java.util.Arrays.equals(targetKey, pk)) {
                        break;
                    }
                }
            }
            long elapsed = System.nanoTime() - start;
            if (iter >= 0) latencies.add(elapsed);
        }
        return computeStats("fullScan", latencies);
    }

    private BenchResult benchSkipRow(Path path, byte[] targetKey) throws Exception {
        List<Long> latencies = new ArrayList<>();
        for (int iter = -WARMUP; iter < MEASURE; iter++) {
            long start = System.nanoTime();
            ParquetPointReader<Group> reader = createSeekableReader(path);
            int result = reader.seekTo(Binary.fromReusedByteArray(targetKey));
            if (result == 0) {
                reader.read();
            }
            long elapsed = System.nanoTime() - start;
            if (iter >= 0) latencies.add(elapsed);
        }
        return computeStats("skipRow", latencies);
    }

    // ======================== Cached PageReadStore ========================

    private static class CachedPageReadStore implements PageReadStore {
        private final long rowCount;
        private final java.util.Map<ColumnDescriptor, List<DataPage>> pageCache;
        private final java.util.Map<ColumnDescriptor, DictionaryPage> dictCache;

        CachedPageReadStore(PageReadStore source, MessageType schema) throws IOException {
            this.rowCount = source.getRowCount();
            this.pageCache = new HashMap<>();
            this.dictCache = new HashMap<>();
            for (ColumnDescriptor col : schema.getColumns()) {
                PageReader reader = source.getPageReader(col);
                List<DataPage> pages = new ArrayList<>();
                DataPage page;
                while ((page = reader.readPage()) != null) {
                    pages.add(page);
                }
                pageCache.put(col, pages);
                DictionaryPage dict = reader.readDictionaryPage();
                if (dict != null) {
                    dictCache.put(col, dict);
                }
            }
        }

        @Override
        public PageReader getPageReader(ColumnDescriptor column) {
            List<DataPage> pages = pageCache.get(column);
            DictionaryPage dict = dictCache.get(column);
            return new PageReader() {
                private int index = 0;
                private boolean dictRead = false;

                @Override
                public long getTotalValueCount() {
                    return pages.stream().mapToLong(DataPage::getValueCount).sum();
                }

                @Override
                public DataPage readPage() {
                    if (index < pages.size()) return pages.get(index++);
                    return null;
                }

                @Override
                public DictionaryPage readDictionaryPage() {
                    if (!dictRead) {
                        dictRead = true;
                        return dict;
                    }
                    return null;
                }
            };
        }

        @Override
        public long getRowCount() {
            return rowCount;
        }
    }

    // ======================== Cached Reader Benchmark ========================

    private static class SeekableReaderCache {
        final ParquetFileReader fileReader;
        final MessageColumnIO columnIO;
        final ReadSupport.ReadContext readContext;
        final MessageType fileSchema;
        final String createdBy;
        final long rowCount;
        final Map<String, String> extraMeta;
        final ColumnPath pkPath;
        final int blockIndex;

        SeekableReaderCache(
                ParquetFileReader fileReader,
                MessageColumnIO columnIO,
                ReadSupport.ReadContext readContext,
                MessageType fileSchema,
                String createdBy,
                long rowCount,
                Map<String, String> extraMeta,
                ColumnPath pkPath,
                int blockIndex) {
            this.fileReader = fileReader;
            this.columnIO = columnIO;
            this.readContext = readContext;
            this.fileSchema = fileSchema;
            this.createdBy = createdBy;
            this.rowCount = rowCount;
            this.extraMeta = extraMeta;
            this.pkPath = pkPath;
            this.blockIndex = blockIndex;
        }
    }

    private static SeekableReaderCache buildCache(Path path) throws IOException {
        org.apache.parquet.hadoop.util.HadoopInputFile inputFile =
                org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(path, new Configuration());
        ParquetFileReader fileReader = ParquetFileReader.open(inputFile);
        MessageType fileSchema = fileReader.getFooter().getFileMetaData().getSchema();
        Map<String, String> extraMeta =
                fileReader.getFooter().getFileMetaData().getKeyValueMetaData();
        long rowCount = fileReader.getFooter().getBlocks().get(0).getRowCount();

        GroupReadSupport readSupport = new GroupReadSupport();
        ReadSupport.ReadContext readContext =
                readSupport.init(new Configuration(), extraMeta, fileSchema);

        ColumnIOFactory columnIOFactory = new ColumnIOFactory(true);
        MessageColumnIO columnIO =
                columnIOFactory.getColumnIO(readContext.getRequestedSchema(), fileSchema);
        String createdBy = fileReader.getFooter().getFileMetaData().getCreatedBy();

        return new SeekableReaderCache(
                fileReader,
                columnIO,
                readContext,
                fileSchema,
                createdBy,
                rowCount,
                extraMeta,
                ColumnPath.fromDotString(PK_COL),
                0);
    }

    private ParquetPointReader<Group> createFromCache(SeekableReaderCache cache)
            throws IOException {
        GroupReadSupport readSupport = new GroupReadSupport();
        RecordMaterializer<Group> materializer =
                readSupport.prepareForRead(
                        new Configuration(), cache.extraMeta, cache.fileSchema, cache.readContext);

        PageReadStore pageStore = cache.fileReader.readRowGroup(cache.blockIndex);

        RowSkipReadStore seekableStore =
                new RowSkipReadStore(
                        pageStore,
                        materializer.getRootConverter(),
                        cache.fileSchema,
                        cache.createdBy);

        return new ParquetPointReader<>(
                cache.columnIO, materializer, false, seekableStore, cache.pkPath, cache.rowCount);
    }

    private BenchResult benchWarmSkipRow(SeekableReaderCache cache, byte[] targetKey)
            throws Exception {
        List<Long> latencies = new ArrayList<>();
        for (int iter = -WARMUP; iter < MEASURE; iter++) {
            long start = System.nanoTime();
            ParquetPointReader<Group> reader = createFromCache(cache);
            int result = reader.seekTo(Binary.fromReusedByteArray(targetKey));
            if (result == 0) reader.read();
            long elapsed = System.nanoTime() - start;
            if (iter >= 0) latencies.add(elapsed);
        }
        return computeStats("warmSkipRow", latencies);
    }

    @Test
    @DisplayName("Cache repeat vs Cold create vs FullScan for")
    void cachedReaderBenchmark(@TempDir java.nio.file.Path tempDir) throws Exception {
        Path widePath = new Path(tempDir.resolve("cache-bench.parquet").toString());
        writeWideData(widePath);

        int[] positions = {0, ROW_COUNT / 4, ROW_COUNT / 2, ROW_COUNT * 3 / 4, ROW_COUNT - 1};
        String[] posNames = {"0%(开头)", "25%", "50%(中间)", "75%", "99.99%(末尾)"};

        System.out.println(
                "\n╔════════════════════════════════════════════════════════════════════════════════╗");
        System.out.printf(
                "║  Cache vs Cold vs FullScan (%,d 行, %d 列)                          ║%n",
                ROW_COUNT, 1 + WIDE_COLS);
        System.out.println(
                "╚════════════════════════════════════════════════════════════════════════════════╝\n");

        // Build cache once
        SeekableReaderCache cache = buildCache(widePath);
        System.out.println("--- Cache 构建完成 (含 readRowGroup) ---\n");

        // Warmup
        for (int w = 0; w < 5; w++) {
            for (int pos : positions) {
                benchWideFullScan(widePath, pkBytes(pos));
                benchWideSkipRow(widePath, pkBytes(pos));
                benchWarmSkipRow(cache, pkBytes(pos));
            }
        }
        System.gc();
        Thread.sleep(300);

        System.out.println("--- FullScan (逐行解码 " + (1 + WIDE_COLS) + " 列) ---");
        List<BenchResult> fullResults = new ArrayList<>();
        for (int pi = 0; pi < positions.length; pi++) {
            BenchResult r = benchWideFullScan(widePath, pkBytes(positions[pi]));
            fullResults.add(r);
            System.out.printf(
                    "  %s: avg=%7.1fμs  p50=%7.1fμs  p99=%7.1fμs%n",
                    posNames[pi], r.avgUs(), r.p50Us(), r.p99Us());
        }

        System.out.println("\n--- Cold SkipRow (每次创建 FileReader + readRowGroup) ---");
        List<BenchResult> coldResults = new ArrayList<>();
        for (int pi = 0; pi < positions.length; pi++) {
            BenchResult r = benchWideSkipRow(widePath, pkBytes(positions[pi]));
            coldResults.add(r);
            System.out.printf(
                    "  %s: avg=%7.1fμs  p50=%7.1fμs  p99=%7.1fμs%n",
                    posNames[pi], r.avgUs(), r.p50Us(), r.p99Us());
        }

        System.out.println("\n--- Warm SkipRow (复用 PageReadStore 缓存) ---");
        List<BenchResult> warmResults = new ArrayList<>();
        for (int pi = 0; pi < positions.length; pi++) {
            BenchResult r = benchWarmSkipRow(cache, pkBytes(positions[pi]));
            warmResults.add(r);
            System.out.printf(
                    "  %s: avg=%7.1fμs  p50=%7.1fμs  p99=%7.1fμs%n",
                    posNames[pi], r.avgUs(), r.p50Us(), r.p99Us());
        }

        System.out.println(
                "\n╔════════════════════╦═════════════════╦═════════════════╦═════════════════╦══════════╗");
        System.out.println(
                "║ 查询位置            ║ FullScan (avg)  ║ Cold (avg)      ║ Warm (avg)      ║ 加速比   ║");
        System.out.println(
                "╠════════════════════╬═════════════════╬═════════════════╬═════════════════╬══════════╣");
        for (int pi = 0; pi < positions.length; pi++) {
            BenchResult fr = fullResults.get(pi);
            BenchResult cr = coldResults.get(pi);
            BenchResult wr = warmResults.get(pi);
            double speedup = fr.avgUs() / wr.avgUs();
            System.out.printf(
                    "║ %-18s ║ %8.1f μs    ║ %8.1f μs    ║ %8.1f μs    ║ %6.1fx  ║%n",
                    posNames[pi], fr.avgUs(), cr.avgUs(), wr.avgUs(), speedup);
        }
        System.out.println(
                "╚════════════════════╩═════════════════╩═════════════════╩═════════════════╩══════════╝");

        System.out.println("\n--- 末尾位置开销分解 (Warm) ---");
        benchWarmBreakdown(cache, pkBytes(ROW_COUNT - 1));
    }

    private void benchWarmBreakdown(SeekableReaderCache cache, byte[] targetKey) throws Exception {
        long createTotal = 0, seekTotal = 0, readTotal = 0;
        int samples = 10;
        for (int i = 0; i < samples; i++) {
            long t0 = System.nanoTime();
            ParquetPointReader<Group> reader = createFromCache(cache);
            long t1 = System.nanoTime();
            int result = reader.seekTo(Binary.fromReusedByteArray(targetKey));
            long t2 = System.nanoTime();
            if (result == 0) reader.read();
            long t3 = System.nanoTime();
            createTotal += (t1 - t0);
            seekTotal += (t2 - t1);
            readTotal += (t3 - t2);
        }
        System.out.printf(
                "  Reader 创建 (from cache): %.1f μs  (vs Cold: 12150.9 μs)%n",
                createTotal / 1000.0 / samples);
        System.out.printf(
                "  seekTo():                 %.1f μs  (PK 列逐行比较 %d 行)%n",
                seekTotal / 1000.0 / samples, ROW_COUNT - 1);
        System.out.printf("  read():                    %.1f μs%n", readTotal / 1000.0 / samples);
    }

    // ======================== Hot Cache Benchmark ========================

    private static class HotCache {
        final CachedPageReadStore cachedPageStore;
        final MessageColumnIO columnIO;
        final ReadSupport.ReadContext readContext;
        final MessageType fileSchema;
        final String createdBy;
        final long rowCount;
        final Map<String, String> extraMeta;
        final ColumnPath pkPath;

        HotCache(
                CachedPageReadStore cachedPageStore,
                MessageColumnIO columnIO,
                ReadSupport.ReadContext readContext,
                MessageType fileSchema,
                String createdBy,
                long rowCount,
                Map<String, String> extraMeta,
                ColumnPath pkPath) {
            this.cachedPageStore = cachedPageStore;
            this.columnIO = columnIO;
            this.readContext = readContext;
            this.fileSchema = fileSchema;
            this.createdBy = createdBy;
            this.rowCount = rowCount;
            this.extraMeta = extraMeta;
            this.pkPath = pkPath;
        }
    }

    private static HotCache buildHotCache(Path path) throws IOException {
        org.apache.parquet.hadoop.util.HadoopInputFile inputFile =
                org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(path, new Configuration());
        ParquetFileReader fileReader = ParquetFileReader.open(inputFile);
        MessageType fileSchema = fileReader.getFooter().getFileMetaData().getSchema();
        Map<String, String> extraMeta =
                fileReader.getFooter().getFileMetaData().getKeyValueMetaData();
        long rowCount = fileReader.getFooter().getBlocks().get(0).getRowCount();

        GroupReadSupport readSupport = new GroupReadSupport();
        ReadSupport.ReadContext readContext =
                readSupport.init(new Configuration(), extraMeta, fileSchema);

        ColumnIOFactory columnIOFactory = new ColumnIOFactory(true);
        MessageColumnIO columnIO =
                columnIOFactory.getColumnIO(readContext.getRequestedSchema(), fileSchema);
        String createdBy = fileReader.getFooter().getFileMetaData().getCreatedBy();

        CachedPageReadStore cachedPageStore =
                new CachedPageReadStore(fileReader.readRowGroup(0), fileSchema);

        return new HotCache(
                cachedPageStore,
                columnIO,
                readContext,
                fileSchema,
                createdBy,
                rowCount,
                extraMeta,
                ColumnPath.fromDotString(PK_COL));
    }

    private ParquetPointReader<Group> createFromHotCache(HotCache cache) {
        GroupReadSupport readSupport = new GroupReadSupport();
        RecordMaterializer<Group> materializer =
                readSupport.prepareForRead(
                        new Configuration(), cache.extraMeta, cache.fileSchema, cache.readContext);

        RowSkipReadStore seekableStore =
                new RowSkipReadStore(
                        cache.cachedPageStore,
                        materializer.getRootConverter(),
                        cache.fileSchema,
                        cache.createdBy);

        return new ParquetPointReader<>(
                cache.columnIO, materializer, false, seekableStore, cache.pkPath, cache.rowCount);
    }

    private BenchResult benchHotSkipRow(HotCache cache, byte[] targetKey) throws Exception {
        List<Long> latencies = new ArrayList<>();
        for (int iter = -WARMUP; iter < MEASURE; iter++) {
            long start = System.nanoTime();
            ParquetPointReader<Group> reader = createFromHotCache(cache);
            int result = reader.seekTo(Binary.fromReusedByteArray(targetKey));
            if (result == 0) reader.read();
            long elapsed = System.nanoTime() - start;
            if (iter >= 0) latencies.add(elapsed);
        }
        return computeStats("hotSkipRow", latencies);
    }

    @Test
    @DisplayName("Hot cache vs Warm vs Cold vs FullScan for")
    void hotReaderBenchmark(@TempDir java.nio.file.Path tempDir) throws Exception {
        Path widePath = new Path(tempDir.resolve("hot-bench.parquet").toString());
        writeWideData(widePath);

        int[] positions = {0, ROW_COUNT / 4, ROW_COUNT / 2, ROW_COUNT * 3 / 4, ROW_COUNT - 1};
        String[] posNames = {"0%(开头)", "25%", "50%(中间)", "75%", "99.99%(末尾)"};

        System.out.println(
                "\n╔════════════════════════════════════════════════════════════════════════════════╗");
        System.out.printf(
                "║  Hot vs Warm vs Cold vs FullScan (%,d 行, %d 列)                   ║%n",
                ROW_COUNT, 1 + WIDE_COLS);
        System.out.println(
                "╚════════════════════════════════════════════════════════════════════════════════╝\n");

        HotCache hotCache = buildHotCache(widePath);
        SeekableReaderCache warmCache = buildCache(widePath);
        System.out.println("--- Hot + Warm Cache 构建完成 ---\n");

        // Warmup
        for (int w = 0; w < 5; w++) {
            for (int pos : positions) {
                benchWideFullScan(widePath, pkBytes(pos));
                benchWideSkipRow(widePath, pkBytes(pos));
                benchWarmSkipRow(warmCache, pkBytes(pos));
                benchHotSkipRow(hotCache, pkBytes(pos));
            }
        }
        System.gc();
        Thread.sleep(300);

        System.out.println("--- FullScan (逐行解码 " + (1 + WIDE_COLS) + " 列) ---");
        List<BenchResult> fullResults = new ArrayList<>();
        for (int pi = 0; pi < positions.length; pi++) {
            BenchResult r = benchWideFullScan(widePath, pkBytes(positions[pi]));
            fullResults.add(r);
            System.out.printf("  %s: avg=%7.1fμs%n", posNames[pi], r.avgUs());
        }

        System.out.println("\n--- Cold (每次 open + readRowGroup) ---");
        List<BenchResult> coldResults = new ArrayList<>();
        for (int pi = 0; pi < positions.length; pi++) {
            BenchResult r = benchWideSkipRow(widePath, pkBytes(positions[pi]));
            coldResults.add(r);
            System.out.printf("  %s: avg=%7.1fμs%n", posNames[pi], r.avgUs());
        }

        System.out.println("\n--- Warm (复用 ParquetFileReader, 每次 readRowGroup) ---");
        List<BenchResult> warmResults = new ArrayList<>();
        for (int pi = 0; pi < positions.length; pi++) {
            BenchResult r = benchWarmSkipRow(warmCache, pkBytes(positions[pi]));
            warmResults.add(r);
            System.out.printf("  %s: avg=%7.1fμs%n", posNames[pi], r.avgUs());
        }

        System.out.println("\n--- Hot (PageReadStore 页面数据全内存缓存) ---");
        List<BenchResult> hotResults = new ArrayList<>();
        for (int pi = 0; pi < positions.length; pi++) {
            BenchResult r = benchHotSkipRow(hotCache, pkBytes(positions[pi]));
            hotResults.add(r);
            System.out.printf("  %s: avg=%7.1fμs%n", posNames[pi], r.avgUs());
        }

        System.out.println(
                "\n╔════════════════════╦═════════════════╦═════════════════╦═════════════════╦═════════════════╦══════════╗");
        System.out.println(
                "║ 查询位置            ║ FullScan (avg)  ║ Cold (avg)      ║ Warm (avg)      ║ Hot (avg)       ║ 加速比   ║");
        System.out.println(
                "╠════════════════════╬═════════════════╬═════════════════╬═════════════════╬═════════════════╬══════════╣");
        for (int pi = 0; pi < positions.length; pi++) {
            BenchResult fr = fullResults.get(pi);
            BenchResult cr = coldResults.get(pi);
            BenchResult wr = warmResults.get(pi);
            BenchResult hr = hotResults.get(pi);
            double speedup = fr.avgUs() / hr.avgUs();
            System.out.printf(
                    "║ %-18s ║ %8.1f μs    ║ %8.1f μs    ║ %8.1f μs    ║ %8.1f μs    ║ %6.1fx  ║%n",
                    posNames[pi], fr.avgUs(), cr.avgUs(), wr.avgUs(), hr.avgUs(), speedup);
        }
        System.out.println(
                "╚════════════════════╩═════════════════╩═════════════════╩═════════════════╩═════════════════╩══════════╝");

        System.out.println("\n--- 末尾位置开销分解 (Hot) ---");
        benchHotBreakdown(hotCache, pkBytes(ROW_COUNT - 1));
    }

    private void benchHotBreakdown(HotCache cache, byte[] targetKey) throws Exception {
        long createTotal = 0, seekTotal = 0, readTotal = 0;
        int samples = 10;
        for (int i = 0; i < samples; i++) {
            long t0 = System.nanoTime();
            ParquetPointReader<Group> reader = createFromHotCache(cache);
            long t1 = System.nanoTime();
            int result = reader.seekTo(Binary.fromReusedByteArray(targetKey));
            long t2 = System.nanoTime();
            if (result == 0) reader.read();
            long t3 = System.nanoTime();
            createTotal += (t1 - t0);
            seekTotal += (t2 - t1);
            readTotal += (t3 - t2);
        }
        System.out.printf(
                "  Reader 创建 (from hot cache): %.1f μs  (vs Cold: 12150.9 μs, Warm: 4624.0 μs)%n",
                createTotal / 1000.0 / samples);
        System.out.printf(
                "  seekTo():                     %.1f μs  (PK 列逐行比较 %d 行)%n",
                seekTotal / 1000.0 / samples, ROW_COUNT - 1);
        System.out.printf(
                "  read():                        %.1f μs%n", readTotal / 1000.0 / samples);
    }

    private String readFullScan(Path path, byte[] targetKey) throws Exception {
        try (ParquetReader<Group> reader =
                ParquetReader.builder(new GroupReadSupport(), path)
                        .withConf(new Configuration())
                        .build()) {
            Group group;
            while ((group = reader.read()) != null) {
                byte[] pk = group.getBinary(PK_COL, 0).getBytes();
                if (java.util.Arrays.equals(targetKey, pk)) {
                    return new String(
                            group.getBinary("value", 0).getBytes(), StandardCharsets.UTF_8);
                }
            }
        }
        return null;
    }

    private String readSkipRow(Path path, byte[] targetKey) throws Exception {
        ParquetPointReader<Group> reader = createSeekableReader(path);
        int result = reader.seekTo(Binary.fromReusedByteArray(targetKey));
        if (result != 0) return null;
        Group group = reader.read();
        if (group == null) return null;
        return new String(group.getBinary("value", 0).getBytes(), StandardCharsets.UTF_8);
    }
}
