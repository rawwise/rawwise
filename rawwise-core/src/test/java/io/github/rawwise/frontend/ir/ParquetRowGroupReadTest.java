package io.github.rawwise.frontend.ir;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.block.read.ParquetRowGroupReader;
import io.github.rawwise.engine.storage.writer.DefaultParquetWriterBuilder;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName(": Parquet Row Group / Column Index I/O read test")
class ParquetRowGroupReadTest {

    private static final MessageType KV_SCHEMA =
            Types.buildMessage()
                    .addField(Types.required(BINARY).named("key"))
                    .addField(Types.required(BINARY).named("value"))
                    .named("KeyValue");

    private static final int ROW_COUNT = 500_000;
    private static final long ROW_GROUP_SIZE = 512L * 1024;
    private static final int PAGE_SIZE = 256 * 1024;
    private static final int MEASUREMENT_ITERATIONS = 50;

    @TempDir java.nio.file.Path tempDir;

    private Configuration conf;
    private Path parquetPath;

    private static byte[] keyAt(int index) {
        return String.format("key-%08d", index).getBytes();
    }

    private static byte[] valueAt(int index) {
        return ("value-for-key-" + index).getBytes();
    }

    @BeforeEach
    void setUp() throws Exception {
        conf = new Configuration();
        java.nio.file.Path file = tempDir.resolve("multi_rg_test.parquet");
        parquetPath = new Path(file.toUri().toString());

        GroupWriteSupport writeSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(KV_SCHEMA, conf);

        DefaultParquetWriterBuilder<Group> builder =
                new DefaultParquetWriterBuilder<>(parquetPath, writeSupport);
        builder.withConf(conf)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(ROW_GROUP_SIZE)
                .withPageSize(PAGE_SIZE)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0);

        try (ParquetWriter<Group> writer = builder.build()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                SimpleGroup group = new SimpleGroup(KV_SCHEMA);
                group.add("key", Binary.fromConstantByteArray(keyAt(i)));
                group.add("value", Binary.fromConstantByteArray(valueAt(i)));
                writer.write(group);
            }
        }
    }

    @Test
    @DisplayName("verifymulti Row Group multiple Block")
    void shouldGenerateMultipleRowGroups() throws Exception {
        ParquetMetadata footer = ParquetFileReader.readFooter(conf, parquetPath);
        List<BlockMetaData> blocks = footer.getBlocks();
        long fileSize = Files.size(java.nio.file.Path.of(parquetPath.toUri()));

        for (int i = 0; i < blocks.size(); i++) {
            BlockMetaData bm = blocks.get(i);
            System.out.printf(
                    "  RG[%d]: rows=%d, totalByteSize=%d%n",
                    i, bm.getRowCount(), bm.getTotalByteSize());
        }
        System.out.printf(
                "File size: %d bytes, Row Groups: %d, Total rows: %d%n",
                fileSize,
                blocks.size(),
                blocks.stream().mapToLong(BlockMetaData::getRowCount).sum());

        assertThat(blocks.size())
                .as(
                        "Should generate multiple row groups with %d KB row group size and %d rows uncompressed",
                        ROW_GROUP_SIZE / 1024, ROW_COUNT)
                .isGreaterThan(1);
    }

    @Test
    @DisplayName("verifyeach Row Group allhas Column Index (PARQUET_2.0)")
    void shouldHaveColumnIndexOnEachRowGroup() throws Exception {
        ParquetMetadata footer = ParquetFileReader.readFooter(conf, parquetPath);
        List<BlockMetaData> blocks = footer.getBlocks();

        assertThat(blocks).isNotEmpty();
        for (int i = 0; i < blocks.size(); i++) {
            BlockMetaData bm = blocks.get(i);
            ColumnChunkMetaData keyCol = ParquetRowGroupReader.findKeyColumn(bm, "key");
            assertThat(keyCol).as("RG[%d] should have key column", i).isNotNull();
            assertThat(keyCol.getColumnIndexReference())
                    .as("RG[%d] should have ColumnIndexReference (PARQUET_2.0)", i)
                    .isNotNull();
        }
    }

    @Test
    @DisplayName("find Row Group")
    void shouldFindMatchingBlockByBinarySearch() throws Exception {
        ParquetMetadata footer = ParquetFileReader.readFooter(conf, parquetPath);
        List<BlockMetaData> allBlocks = footer.getBlocks();

        int[] testPositions = {0, ROW_COUNT / 4, ROW_COUNT / 2, 3 * ROW_COUNT / 4, ROW_COUNT - 1};
        for (int pos : testPositions) {
            byte[] targetKey = keyAt(pos);
            List<BlockMetaData> matching =
                    ParquetRowGroupReader.findMatchingBlocks(allBlocks, "key", targetKey);
            assertThat(matching).isNotEmpty();
        }
    }

    @Test
    @DisplayName("findcanexcludemismatched Row Group")
    void shouldExcludeNonMatchingBlocks() throws Exception {
        ParquetMetadata footer = ParquetFileReader.readFooter(conf, parquetPath);
        List<BlockMetaData> allBlocks = footer.getBlocks();

        if (allBlocks.size() <= 2) {
            System.out.println("Need >2 row groups for this test");
            return;
        }

        byte[] firstKey = keyAt(0);
        List<BlockMetaData> matching =
                ParquetRowGroupReader.findMatchingBlocks(allBlocks, "key", firstKey);

        assertThat(matching.size())
                .as("Only the first RG should match the first key")
                .isLessThan(allBlocks.size());
    }

    @Test
    @DisplayName("verifysingle Row Group readcan to key")
    void shouldFindTargetKeyInSingleRowGroup() throws Exception {
        ParquetMetadata footer = ParquetFileReader.readFooter(conf, parquetPath);
        List<BlockMetaData> allBlocks = footer.getBlocks();
        MessageType fileSchema = footer.getFileMetaData().getSchema();
        Map<String, String> extraMeta = footer.getFileMetaData().getKeyValueMetaData();

        int targetPos = ROW_COUNT / 3;
        byte[] targetKey = keyAt(targetPos);

        List<BlockMetaData> matching =
                ParquetRowGroupReader.findMatchingBlocks(allBlocks, "key", targetKey);
        assertThat(matching).isNotEmpty();

        int blockIndex = allBlocks.indexOf(matching.get(0));
        ParquetRowGroupReader.RowGroupReadResult result =
                ParquetRowGroupReader.readSingleRowGroup(
                        conf, parquetPath, blockIndex, fileSchema, extraMeta);

        try {
            Group group;
            boolean found = false;
            while ((group = result.recordReader().read()) != null) {
                byte[] key = group.getBinary("key", 0).getBytes();
                if (java.util.Arrays.equals(targetKey, key)) {
                    found = true;
                    byte[] value = group.getBinary("value", 0).getBytes();
                    assertThat(value).isEqualTo(valueAt(targetPos));
                    break;
                }
            }
            assertThat(found).isTrue();
        } finally {
            result.fileReader().close();
        }
    }

    @Test
    @DisplayName("verify Column Index readcancalculatecorrectof skipRows")
    void shouldComputeSkipRowsFromColumnIndex() throws Exception {
        ParquetMetadata footer = ParquetFileReader.readFooter(conf, parquetPath);
        List<BlockMetaData> allBlocks = footer.getBlocks();

        if (allBlocks.size() <= 1) {
            System.out.println("Only 1 row group, skipping column index test");
            return;
        }

        BlockMetaData lastBlock = allBlocks.get(allBlocks.size() - 1);
        byte[] firstKeyInLastBlock =
                keyAt(
                        (int)
                                ParquetRowGroupReader.computeRowGroupStartOffset(
                                        allBlocks, allBlocks.size() - 1));

        try (ParquetFileReader reader =
                ParquetFileReader.open(HadoopInputFile.fromPath(parquetPath, conf))) {
            long skipRows =
                    ParquetRowGroupReader.findSkipRowsByColumnIndex(
                            reader, lastBlock, "key", firstKeyInLastBlock);

            assertThat(skipRows).isGreaterThanOrEqualTo(0);
            System.out.println("ColumnIndex skipRows for first key in last RG: " + skipRows);
        }
    }

    @Test
    @DisplayName("verify Column Index forend key calculatenon- skipRows")
    void shouldComputeNonZeroSkipForLastKeyInBlock() throws Exception {
        ParquetMetadata footer = ParquetFileReader.readFooter(conf, parquetPath);
        List<BlockMetaData> allBlocks = footer.getBlocks();

        if (allBlocks.size() <= 2) {
            System.out.println("Need >2 row groups for this test");
            return;
        }

        BlockMetaData midBlock = allBlocks.get(allBlocks.size() / 2);
        long midBlockStart =
                ParquetRowGroupReader.computeRowGroupStartOffset(allBlocks, allBlocks.size() / 2);
        long midBlockEnd = midBlockStart + midBlock.getRowCount() - 1;
        byte[] lastKeyInMidBlock = keyAt((int) midBlockEnd);

        try (ParquetFileReader reader =
                ParquetFileReader.open(HadoopInputFile.fromPath(parquetPath, conf))) {
            long skipRows =
                    ParquetRowGroupReader.findSkipRowsByColumnIndex(
                            reader, midBlock, "key", lastKeyInMidBlock);
            assertThat(skipRows)
                    .as("ColumnIndex should skip most pages for last key in block")
                    .isGreaterThan(0);
        }
    }

    @Test
    @DisplayName("verifymergeof readSingleRowGroup correctly returns Column Index skip")
    void shouldReturnColumnIndexSkipInReadResult() throws Exception {
        ParquetMetadata footer = ParquetFileReader.readFooter(conf, parquetPath);
        List<BlockMetaData> allBlocks = footer.getBlocks();
        MessageType fileSchema = footer.getFileMetaData().getSchema();
        Map<String, String> extraMeta = footer.getFileMetaData().getKeyValueMetaData();

        if (allBlocks.size() <= 1) {
            System.out.println("Only 1 row group, skipping test");
            return;
        }

        int midBlockIdx = allBlocks.size() / 2;
        long lastRowInMidBlock =
                ParquetRowGroupReader.computeRowGroupStartOffset(allBlocks, midBlockIdx)
                        + allBlocks.get(midBlockIdx).getRowCount()
                        - 1;
        byte[] lastKey = keyAt((int) lastRowInMidBlock);

        ParquetRowGroupReader.RowGroupReadResult result =
                ParquetRowGroupReader.readSingleRowGroup(
                        conf, parquetPath, midBlockIdx, fileSchema, extraMeta, "key", lastKey);

        try {
            assertThat(result.columnIndexSkipRows())
                    .as("ColumnIndex skip should be >0 for last key in block")
                    .isGreaterThan(0);

            long skippedInReading = 0;
            Group group;
            while ((group = result.recordReader().read()) != null) {
                byte[] key = group.getBinary("key", 0).getBytes();
                if (skippedInReading < result.columnIndexSkipRows()) {
                    skippedInReading++;
                    continue;
                }
                if (java.util.Arrays.equals(lastKey, key)) {
                    return;
                }
                skippedInReading++;
            }
        } finally {
            result.fileReader().close();
        }
    }

    @Test
    @DisplayName("Row Group point query Benchmark for")
    void benchmarkRowGroupOptimizedPointQuery() throws Exception {
        ParquetMetadata footer = ParquetFileReader.readFooter(conf, parquetPath);
        List<BlockMetaData> allBlocks = footer.getBlocks();
        MessageType fileSchema = footer.getFileMetaData().getSchema();
        Map<String, String> extraMeta = footer.getFileMetaData().getKeyValueMetaData();

        System.out.println("=== P3 Row Group 优化 Benchmark ===");
        System.out.println("Row Groups: " + allBlocks.size());
        System.out.println("Total rows: " + ROW_COUNT);

        if (allBlocks.size() <= 1) {
            System.out.println("Only 1 row group, no RG-level skip possible");
            return;
        }

        int[] positions = {0, ROW_COUNT / 4, ROW_COUNT / 2, 3 * ROW_COUNT / 4, ROW_COUNT - 1};

        for (int pos : positions) {
            byte[] targetKey = keyAt(pos);

            List<BlockMetaData> matchingBlocks =
                    ParquetRowGroupReader.findMatchingBlocks(allBlocks, "key", targetKey);
            List<Integer> matchingIndices = new ArrayList<>();
            for (BlockMetaData bm : matchingBlocks) {
                matchingIndices.add(allBlocks.indexOf(bm));
            }

            System.out.printf(
                    "  查询 key=%s → 匹配 RG: %d/%d%n",
                    new String(targetKey), matchingBlocks.size(), allBlocks.size());

            double fullScanAvg = benchmarkFullScan(targetKey, allBlocks, fileSchema, extraMeta);
            double optimizedAvg =
                    benchmarkOptimizedRead(targetKey, matchingIndices, fileSchema, extraMeta);

            double speedup = fullScanAvg / Math.max(optimizedAvg, 1.0);
            System.out.printf(
                    "  Full Scan: %.1fμs  Optimized: %.1fμs  Speedup: %.1fx%n",
                    fullScanAvg, optimizedAvg, speedup);
        }
    }

    private double benchmarkFullScan(
            byte[] targetKey,
            List<BlockMetaData> allBlocks,
            MessageType fileSchema,
            Map<String, String> extraMeta)
            throws Exception {
        List<Long> times = new ArrayList<>();
        int warmup = 3;

        for (int iter = 0; iter < MEASUREMENT_ITERATIONS + warmup; iter++) {
            long start = System.nanoTime();
            try (ParquetReader<Group> reader =
                    ParquetReader.builder(new GroupReadSupport(), parquetPath)
                            .withConf(conf)
                            .build()) {
                Group group;
                while ((group = reader.read()) != null) {
                    byte[] key = group.getBinary("key", 0).getBytes();
                    if (java.util.Arrays.equals(targetKey, key)) {
                        break;
                    }
                }
            }
            if (iter >= warmup) {
                times.add(System.nanoTime() - start);
            }
        }

        return times.stream().mapToLong(Long::longValue).average().orElse(0) / 1000.0;
    }

    private double benchmarkOptimizedRead(
            byte[] targetKey,
            List<Integer> matchingIndices,
            MessageType fileSchema,
            Map<String, String> extraMeta)
            throws Exception {
        List<Long> times = new ArrayList<>();
        int warmup = 3;

        for (int iter = 0; iter < MEASUREMENT_ITERATIONS + warmup; iter++) {
            long start = System.nanoTime();
            for (int bi : matchingIndices) {
                ParquetRowGroupReader.RowGroupReadResult result =
                        ParquetRowGroupReader.readSingleRowGroup(
                                conf, parquetPath, bi, fileSchema, extraMeta);
                try {
                    Group group;
                    boolean found = false;
                    while ((group = result.recordReader().read()) != null) {
                        byte[] key = group.getBinary("key", 0).getBytes();
                        if (java.util.Arrays.equals(targetKey, key)) {
                            found = true;
                            break;
                        }
                    }
                    if (found) break;
                } finally {
                    result.fileReader().close();
                }
            }
            if (iter >= warmup) {
                times.add(System.nanoTime() - start);
            }
        }

        return times.stream().mapToLong(Long::longValue).average().orElse(0) / 1000.0;
    }
}
