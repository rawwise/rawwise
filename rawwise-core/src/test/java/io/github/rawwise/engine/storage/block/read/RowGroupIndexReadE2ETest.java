package io.github.rawwise.engine.storage.block.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.engine.storage.block.ColdDataBlock;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.block.index.RowGroupIndex;
import io.github.rawwise.engine.storage.block.index.SparseIndex;
import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.engine.storage.table.key.DefaultKeyCodec;
import io.github.rawwise.model.op.Find;
import io.github.rawwise.model.record.DefaultRecord;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.RecordCodec;
import io.github.rawwise.model.record.Records;
import io.github.rawwise.model.record.Schema;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RowGroupIndexReadE2ETest {

    private static final org.apache.avro.Schema KV_SCHEMA =
            new org.apache.avro.Schema.Parser()
                    .parse(
                            """
            {
              "type": "record",
              "name": "RawWiseKeyValueRecord",
              "fields": [
                {"name": "key", "type": "bytes"},
                {"name": "value", "type": "bytes"}
              ]
            }
            """);

    private DefaultKeyCodec keyCodec;

    @TempDir java.nio.file.Path tempDir;

    @BeforeEach
    void setUp() {
        keyCodec = new DefaultKeyCodec();
    }

    private static byte[] k(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] encodePk(String key) throws IOException {
        return keyCodec.encodePrimaryKey(Map.of("id", key));
    }

    private ColdDataBlock buildColdBlock(String parquetPath, byte[] rowGroupIndex, int rowCount) {
        long fileSize;
        try {
            fileSize =
                    java.nio.file.Files.size(java.nio.file.Path.of(new java.net.URI(parquetPath)));
        } catch (Exception e) {
            fileSize = 100L;
        }
        return new ColdDataBlock(
                "cold-block-rg-test",
                "test.db.rg_table",
                DataBlock.Kind.IMMUTABLE_COLD,
                DataBlock.Format.PARQUET,
                DataBlock.Format.PARQUET,
                new DataBlock.Location(
                        "local", new Storage.StoragePath("file", parquetPath), 0L, fileSize),
                KeyRange.FULL_RANGE,
                rowCount,
                fileSize,
                1L,
                -1L,
                Map.of(),
                new ColdDataBlock.Statistics(rowCount, fileSize, 0L, 0L),
                DataBlock.Visibility.ACTIVE,
                0L,
                0L,
                0,
                null,
                rowGroupIndex);
    }

    private String writeLegacyParquet(List<String> keys, String fileName) throws IOException {
        java.nio.file.Path parquetFile = tempDir.resolve(fileName);
        Path hadoopPath = new Path(parquetFile.toUri().toString());

        try (ParquetWriter<GenericRecord> writer =
                AvroParquetWriter.<GenericRecord>builder(hadoopPath)
                        .withSchema(KV_SCHEMA)
                        .withConf(new Configuration())
                        .withRowGroupSize(64L)
                        .build()) {
            for (String key : keys) {
                GenericRecord record = new GenericData.Record(KV_SCHEMA);
                record.put("key", java.nio.ByteBuffer.wrap(encodePk(key)));
                record.put("value", java.nio.ByteBuffer.wrap(k("val-" + key)));
                writer.write(record);
            }
        }

        return parquetFile.toUri().toString();
    }

    private RowGroupIndex buildRowGroupIndexFromParquet(String parquetPath) throws IOException {
        Configuration conf = new Configuration();
        Path hadoopPath = new Path(parquetPath);
        ParquetMetadata footer = ParquetFileReader.readFooter(conf, hadoopPath);
        List<BlockMetaData> blocks = footer.getBlocks();

        List<RowGroupIndex.RowGroupEntry> entries = new ArrayList<>();
        long startPos = 0;
        for (BlockMetaData block : blocks) {
            byte[] minKey = null;
            byte[] maxKey = null;
            for (var col : block.getColumns()) {
                if (col.getPath().toDotString().equals("key")
                        && col.getStatistics().hasNonNullValue()) {
                    minKey = col.getStatistics().getMinBytes();
                    maxKey = col.getStatistics().getMaxBytes();
                    break;
                }
            }
            if (minKey == null) minKey = new byte[0];
            if (maxKey == null) maxKey = new byte[0];
            entries.add(
                    new RowGroupIndex.RowGroupEntry(minKey, maxKey, block.getRowCount(), startPos));
            startPos += block.getRowCount();
        }
        return new RowGroupIndex(entries);
    }

    private Find createFind(String key) {
        Find find = mock(Find.class);
        when(find.primaryKey().values()).thenReturn(Map.of("id", key));
        when(find.filter()).thenReturn(null);
        when(find.projection()).thenReturn(null);
        when(find.snapshotId()).thenReturn(-1L);
        return find;
    }

    private RecordCodec createMockRecordCodec() {
        RecordCodec codec = mock(RecordCodec.class);
        when(codec.decode(any(byte[].class), any(RecordCodec.SchemaResolver.class)))
                .thenAnswer(
                        invocation -> {
                            byte[] data = invocation.getArgument(0);
                            Record.Key key = Records.keyOf(data);
                            Schema schema = mock(Schema.class);
                            when(schema.fields()).thenReturn(List.of());
                            Record.Body body = mock(Record.Body.class);
                            when(body.schema()).thenReturn(schema);
                            Record.Header header =
                                    Record.Header.of(key, 0L, 0L, 0L, 0L, Record.Kind.INSERT, true);
                            return new DefaultRecord(key, body, header);
                        });
        return codec;
    }

    private RecordCodec.SchemaResolver createMockSchemaResolver() {
        return mock(RecordCodec.SchemaResolver.class);
    }

    private ParquetBlockReader createReader() {
        ParquetBlockReader reader = new ParquetBlockReader(keyCodec);
        reader.withRecordCodec(createMockRecordCodec(), createMockSchemaResolver());
        return reader;
    }

    @Nested
    @DisplayName("RowGroupIndex read path E2E test")
    class ReadPathTests {

        @Test
        @DisplayName("RowGroupIndex filter -> read in RowGroup")
        void shouldFilterRowGroupsByKeyRange() throws IOException {
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                keys.add(String.format("key-%03d", i));
            }
            String parquetPath = writeLegacyParquet(keys, "rg-filter.parquet");

            RowGroupIndex rgIndex = buildRowGroupIndexFromParquet(parquetPath);
            assertThat(rgIndex.size()).isGreaterThan(1);

            byte[] rgIndexBytes = rgIndex.serialize();
            ColdDataBlock block = buildColdBlock(parquetPath, rgIndexBytes, keys.size());

            try (ParquetBlockReader reader = createReader()) {
                Find find = createFind("key-150");
                Find.Result result = reader.find(find, List.of(block));

                assertThat(result.success()).isTrue();
                assertThat(result.foundRecord()).isPresent();
            }

            List<Integer> candidates = rgIndex.candidateRowGroups(encodePk("key-150"));
            assertThat(candidates).isNotEmpty();

            List<Integer> missCandidates = rgIndex.candidateRowGroups(encodePk("zzz-not-exist"));
            assertThat(missCandidates).isEmpty();
        }

        @Test
        @DisplayName("no RowGroupIndex -> fallback")
        void shouldFallbackWhenNoRowGroupIndex() throws IOException {
            List<String> keys = List.of("key-001", "key-002", "key-003");
            String parquetPath = writeLegacyParquet(keys, "no-rg-index.parquet");

            ColdDataBlock block = buildColdBlock(parquetPath, null, keys.size());

            try (ParquetBlockReader reader = createReader()) {
                Find find = createFind("key-002");
                Find.Result result = reader.find(find, List.of(block));

                assertThat(result.success()).isTrue();
                assertThat(result.foundRecord()).isPresent();

                Find findMiss = createFind("key-999");
                Find.Result missResult = reader.find(findMiss, List.of(block));
                assertThat(missResult.success()).isTrue();
                assertThat(missResult.foundRecord()).isEmpty();
            }
        }

        @Test
        @DisplayName("RowGroupIndex + SparseIndex -> filter")
        void shouldCombineWithSparseIndex() throws IOException {
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                keys.add(String.format("key-%03d", i));
            }
            String parquetPath1 = writeLegacyParquet(keys, "rg-sparse-1.parquet");
            String parquetPath2 = writeLegacyParquet(keys, "rg-sparse-2.parquet");

            RowGroupIndex rgIndex = buildRowGroupIndexFromParquet(parquetPath1);
            assertThat(rgIndex.size()).isGreaterThan(1);

            byte[] rgIndexBytes = rgIndex.serialize();

            SparseIndex sparseIndex = new SparseIndex(buildSparseEntries(keys));
            byte[] sparseIndexBytes = SparseIndex.serialize(sparseIndex);

            ColdDataBlock blockWithRgIndex =
                    buildColdBlock(parquetPath1, rgIndexBytes, keys.size());
            ColdDataBlock blockWithSparseIndex =
                    buildColdBlock(parquetPath2, sparseIndexBytes, keys.size());

            try (ParquetBlockReader reader = createReader()) {
                Find findRg = createFind("key-150");
                Find.Result rgResult = reader.find(findRg, List.of(blockWithRgIndex));
                assertThat(rgResult.success()).isTrue();
                assertThat(rgResult.foundRecord()).isPresent();

                Find findSparse = createFind("key-150");
                Find.Result sparseResult = reader.find(findSparse, List.of(blockWithSparseIndex));
                assertThat(sparseResult.success()).isTrue();
                assertThat(sparseResult.foundRecord()).isPresent();

                Find findBoth = createFind("key-150");
                Find.Result bothResult =
                        reader.find(findBoth, List.of(blockWithRgIndex, blockWithSparseIndex));
                assertThat(bothResult.success()).isTrue();
                assertThat(bothResult.foundRecord()).isPresent();

                Find findMiss = createFind("zzz-not-exist");
                Find.Result missResult =
                        reader.find(findMiss, List.of(blockWithRgIndex, blockWithSparseIndex));
                assertThat(missResult.success()).isTrue();
                assertThat(missResult.foundRecord()).isEmpty();
            }
        }
    }

    private List<SparseIndex.SparseIndexEntry> buildSparseEntries(List<String> keys)
            throws IOException {
        List<SparseIndex.SparseIndexEntry> entries = new ArrayList<>();
        int step = Math.max(1, keys.size() / 10);
        for (int i = 0; i < keys.size(); i += step) {
            entries.add(new SparseIndex.SparseIndexEntry(encodePk(keys.get(i)), i));
        }
        if (entries.isEmpty() || entries.get(0).rowIndex() != 0) {
            entries.add(0, new SparseIndex.SparseIndexEntry(encodePk(keys.get(0)), 0));
        }
        return entries;
    }
}
