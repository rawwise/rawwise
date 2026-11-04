package io.github.rawwise.engine.storage.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.model.record.ColumnGroup;
import io.github.rawwise.model.record.PrimaryKey;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.TableMeta;
import io.github.rawwise.model.record.Types;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("ParquetSchemaAdapter E2Etest")
class ParquetSchemaAdapterTest {

    @TempDir Path tempDir;

    private ParquetSchemaAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new ParquetSchemaAdapter();
    }

    private org.apache.hadoop.fs.Path writeKeyValueParquet(String fileName) throws IOException {
        org.apache.avro.Schema kvSchema = ParquetSchemaAdapter.keyValueSchema();
        org.apache.hadoop.fs.Path path =
                new org.apache.hadoop.fs.Path(tempDir.resolve(fileName).toString());
        try (ParquetWriter<GenericRecord> writer =
                AvroParquetWriter.<GenericRecord>builder(path)
                        .withSchema(kvSchema)
                        .withConf(new org.apache.hadoop.conf.Configuration())
                        .build()) {
            GenericRecord record = new GenericData.Record(kvSchema);
            record.put("key", ByteBuffer.wrap("test-key".getBytes()));
            record.put("value", ByteBuffer.wrap("test-value".getBytes()));
            writer.write(record);
        }
        return path;
    }

    private org.apache.hadoop.fs.Path writeBodyCodecParquet(
            String fileName, org.apache.avro.Schema avroSchema, Map<String, String> extraMetaData)
            throws IOException {
        org.apache.hadoop.fs.Path path =
                new org.apache.hadoop.fs.Path(tempDir.resolve(fileName).toString());
        AvroParquetWriter.Builder<GenericRecord> builder =
                AvroParquetWriter.<GenericRecord>builder(path)
                        .withSchema(avroSchema)
                        .withConf(new org.apache.hadoop.conf.Configuration());
        if (extraMetaData != null && !extraMetaData.isEmpty()) {
            builder.withExtraMetaData(extraMetaData);
        }
        try (ParquetWriter<GenericRecord> writer = builder.build()) {
            GenericRecord record = new GenericData.Record(avroSchema);
            for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
                if ("body".equals(field.name())
                        || "key".equals(field.name())
                        || "value".equals(field.name())) {
                    record.put(field.name(), ByteBuffer.wrap(("data-" + field.name()).getBytes()));
                } else {
                    record.put(field.name(), ByteBuffer.wrap(("pk-" + field.name()).getBytes()));
                }
            }
            writer.write(record);
        }
        return path;
    }

    private TableMeta buildTableMeta(String bodyCodec, List<String> pkFields, Schema schema) {
        TableMeta.Builder builder =
                TableMeta.builder()
                        .withTableId(1L)
                        .withTableName("test_table")
                        .withTableType(TableMeta.TableType.PRIMARY_KEY)
                        .withSchema(schema);
        if (pkFields != null && !pkFields.isEmpty()) {
            builder.withPrimaryKey(new PrimaryKey(pkFields));
        }
        if (bodyCodec != null) {
            builder.withProperties(Map.of("body.codec", bodyCodec));
        }
        return builder.build();
    }

    @Nested
    @DisplayName("S1 Happy Path")
    class HappyPathTests {

        @Test
        @DisplayName(
                "Body-codecfull round-trip: writeSchema -> writeParquet -> readSchema -> verifyconsistency")
        void bodyCodecRoundTrip() throws IOException {
            Schema schema =
                    Schema.builder()
                            .field("user_id", Types.int64())
                            .field("name", Types.string())
                            .primaryKey("user_id")
                            .build();

            TableMeta meta = buildTableMeta("fixed-offset", List.of("user_id"), schema);

            ParquetSchemaAdapter.WriteSchemaInfo writeInfo = adapter.writeSchema(meta);
            assertThat(writeInfo.isBodyCodec()).isTrue();
            assertThat(writeInfo.bodyCodecName()).isEqualTo("fixed-offset");
            assertThat(writeInfo.writerSchema().getFields()).hasSize(2);
            assertThat(writeInfo.writerSchema().getField("user_id")).isNotNull();
            assertThat(writeInfo.writerSchema().getField("body")).isNotNull();

            Map<String, String> extraMeta =
                    Map.of(
                            "rawwise.bodyCodec",
                            writeInfo.bodyCodecName(),
                            "rawwise.schemaVersion",
                            "5");
            org.apache.hadoop.fs.Path parquetPath =
                    writeBodyCodecParquet(
                            "body-codec-roundtrip.parquet", writeInfo.writerSchema(), extraMeta);

            ParquetSchemaAdapter.ReadSchemaInfo readInfo = adapter.readSchema(parquetPath);
            assertThat(readInfo.isBodyCodec()).isTrue();
            assertThat(readInfo.bodyCodecName()).isEqualTo("fixed-offset");
            assertThat(readInfo.schemaVersion()).isEqualTo(5);
            assertThat(readInfo.pkFieldNames()).containsExactly("user_id");
        }

        @Test
        @DisplayName("Legacy : nobodyCodec dataofParquet fallbacktoKEY_VALUE_SCHEMA")
        void legacyRoundTrip() throws IOException {
            org.apache.hadoop.fs.Path parquetPath = writeKeyValueParquet("legacy.parquet");

            ParquetSchemaAdapter.ReadSchemaInfo readInfo = adapter.readSchema(parquetPath);
            assertThat(readInfo.isBodyCodec()).isFalse();
            assertThat(readInfo.readerSchema()).isEqualTo(ParquetSchemaAdapter.keyValueSchema());
            assertThat(readInfo.bodyCodecName()).isNull();
            assertThat(readInfo.schemaVersion()).isEqualTo(0);
            assertThat(readInfo.pkFieldNames()).isEmpty();
        }

        @Test
        @DisplayName(
                "composite primary key: writeSchemageneratepackage allPKcolumn+bodycolumnofAvro Schema")
        void compositePrimaryKeyWriteSchema() {
            Schema schema =
                    Schema.builder()
                            .field("region", Types.string())
                            .field("user_id", Types.int64())
                            .field("name", Types.string())
                            .primaryKeys("region", "user_id")
                            .build();

            TableMeta meta = buildTableMeta("tlv", List.of("region", "user_id"), schema);

            ParquetSchemaAdapter.WriteSchemaInfo writeInfo = adapter.writeSchema(meta);
            assertThat(writeInfo.isBodyCodec()).isTrue();
            assertThat(writeInfo.bodyCodecName()).isEqualTo("tlv");

            org.apache.avro.Schema avroSchema = writeInfo.writerSchema();
            assertThat(avroSchema.getFields()).hasSize(3);
            assertThat(avroSchema.getField("region")).isNotNull();
            assertThat(avroSchema.getField("user_id")).isNotNull();
            assertThat(avroSchema.getField("body")).isNotNull();

            List<String> fieldNames =
                    avroSchema.getFields().stream()
                            .map(org.apache.avro.Schema.Field::name)
                            .toList();
            assertThat(fieldNames).containsExactly("region", "user_id", "body");
        }
    }

    @Nested
    @DisplayName("S2 Concurrency")
    class ConcurrencyTests {

        @Test
        @DisplayName("multi-threaded concurrentreadSchemasameParquet consistent")
        void concurrentReadSchema() throws Exception {
            Schema schema =
                    Schema.builder()
                            .field("order_id", Types.int64())
                            .field("amount", Types.float64())
                            .primaryKey("order_id")
                            .build();

            TableMeta meta = buildTableMeta("fixed-offset", List.of("order_id"), schema);
            ParquetSchemaAdapter.WriteSchemaInfo writeInfo = adapter.writeSchema(meta);

            Map<String, String> extraMeta =
                    Map.of(
                            "rawwise.bodyCodec",
                            writeInfo.bodyCodecName(),
                            "rawwise.schemaVersion",
                            "10");
            org.apache.hadoop.fs.Path parquetPath =
                    writeBodyCodecParquet(
                            "concurrent.parquet", writeInfo.writerSchema(), extraMeta);

            int threadCount = 8;
            CyclicBarrier barrier = new CyclicBarrier(threadCount);
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger mismatchCount = new AtomicInteger(0);

            for (int i = 0; i < threadCount; i++) {
                executor.submit(
                        () -> {
                            try {
                                barrier.await(5, TimeUnit.SECONDS);
                                ParquetSchemaAdapter.ReadSchemaInfo readInfo =
                                        adapter.readSchema(parquetPath);
                                if (readInfo.isBodyCodec()
                                        && "fixed-offset".equals(readInfo.bodyCodecName())
                                        && readInfo.schemaVersion() == 10
                                        && readInfo.pkFieldNames().equals(List.of("order_id"))) {
                                    successCount.incrementAndGet();
                                } else {
                                    mismatchCount.incrementAndGet();
                                }
                            } catch (Exception e) {
                                mismatchCount.incrementAndGet();
                            }
                        });
            }

            executor.shutdown();
            boolean terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
            assertThat(terminated).isTrue();
            assertThat(successCount.get()).isEqualTo(threadCount);
            assertThat(mismatchCount.get()).isEqualTo(0);
        }
    }

    @Nested
    @DisplayName("S3 Fault Recovery")
    class FaultRecoveryTests {

        @Test
        @DisplayName("bodyCodec datafallbacktoKEY_VALUE_SCHEMA")
        void emptyBodyCodecMetadataFallsBack() throws IOException {
            org.apache.avro.Schema kvSchema = ParquetSchemaAdapter.keyValueSchema();
            org.apache.hadoop.fs.Path path =
                    new org.apache.hadoop.fs.Path(
                            tempDir.resolve("empty-codec.parquet").toString());

            Map<String, String> extraMeta = Map.of("rawwise.bodyCodec", "");
            try (ParquetWriter<GenericRecord> writer =
                    AvroParquetWriter.<GenericRecord>builder(path)
                            .withSchema(kvSchema)
                            .withConf(new org.apache.hadoop.conf.Configuration())
                            .withExtraMetaData(extraMeta)
                            .build()) {
                GenericRecord record = new GenericData.Record(kvSchema);
                record.put("key", ByteBuffer.wrap("k".getBytes()));
                record.put("value", ByteBuffer.wrap("v".getBytes()));
                writer.write(record);
            }

            ParquetSchemaAdapter.ReadSchemaInfo readInfo = adapter.readSchema(path);
            assertThat(readInfo.isBodyCodec()).isFalse();
            assertThat(readInfo.readerSchema()).isEqualTo(ParquetSchemaAdapter.keyValueSchema());
        }

        @Test
        @DisplayName("schemaVersion data defaultversionis0")
        void missingSchemaVersionDefaultsToZero() throws IOException {
            Schema schema =
                    Schema.builder()
                            .field("id", Types.int64())
                            .field("data", Types.string())
                            .primaryKey("id")
                            .build();

            TableMeta meta = buildTableMeta("fixed-offset", List.of("id"), schema);
            ParquetSchemaAdapter.WriteSchemaInfo writeInfo = adapter.writeSchema(meta);

            Map<String, String> extraMeta = Map.of("rawwise.bodyCodec", writeInfo.bodyCodecName());
            org.apache.hadoop.fs.Path parquetPath =
                    writeBodyCodecParquet(
                            "no-version.parquet", writeInfo.writerSchema(), extraMeta);

            ParquetSchemaAdapter.ReadSchemaInfo readInfo = adapter.readSchema(parquetPath);
            assertThat(readInfo.isBodyCodec()).isTrue();
            assertThat(readInfo.schemaVersion()).isEqualTo(0);
        }

        @Test
        @DisplayName("readSchemafornon-existent IOException non- errordata")
        void readSchemaFromNonExistentFileThrowsIOException() {
            org.apache.hadoop.fs.Path ghostPath =
                    new org.apache.hadoop.fs.Path(
                            tempDir.resolve("nonexistent.parquet").toString());
            assertThatThrownBy(() -> adapter.readSchema(ghostPath)).isInstanceOf(IOException.class);
        }
    }

    @Nested
    @DisplayName("S4 Boundary Conditions")
    class BoundaryConditionTests {

        @Test
        @DisplayName("writeSchema null TableMeta KEY_VALUE_SCHEMA")
        void writeSchemaWithNullTableMeta() {
            ParquetSchemaAdapter.WriteSchemaInfo writeInfo = adapter.writeSchema(null);
            assertThat(writeInfo.writerSchema()).isEqualTo(ParquetSchemaAdapter.keyValueSchema());
            assertThat(writeInfo.isBodyCodec()).isFalse();
            assertThat(writeInfo.bodyCodecName()).isNull();
            assertThat(writeInfo.columnGroups()).isNull();
        }

        @Test
        @DisplayName("writeSchema schemaisnullofTableMeta KEY_VALUE_SCHEMA")
        void writeSchemaWithNullSchema() throws Exception {
            Schema realSchema = Schema.builder().field("id", Types.int64()).build();
            TableMeta meta =
                    TableMeta.builder()
                            .withTableId(2L)
                            .withTableName("null_schema_table")
                            .withTableType(TableMeta.TableType.APPEND_ONLY)
                            .withSchema(realSchema)
                            .build();

            java.lang.reflect.Field schemaField = TableMeta.class.getDeclaredField("schema");
            schemaField.setAccessible(true);
            schemaField.set(meta, null);

            ParquetSchemaAdapter.WriteSchemaInfo writeInfo = adapter.writeSchema(meta);
            assertThat(writeInfo.writerSchema()).isEqualTo(ParquetSchemaAdapter.keyValueSchema());
            assertThat(writeInfo.isBodyCodec()).isFalse();
        }

        @Test
        @DisplayName("writeSchemanobody.codec default fixed-offset")
        void writeSchemaDefaultBodyCodec() {
            Schema schema =
                    Schema.builder()
                            .field("id", Types.int64())
                            .field("val", Types.string())
                            .primaryKey("id")
                            .build();

            TableMeta meta =
                    TableMeta.builder()
                            .withTableId(3L)
                            .withTableName("default_codec_table")
                            .withTableType(TableMeta.TableType.PRIMARY_KEY)
                            .withSchema(schema)
                            .withPrimaryKey(new PrimaryKey(List.of("id")))
                            .build();

            ParquetSchemaAdapter.WriteSchemaInfo writeInfo = adapter.writeSchema(meta);
            assertThat(writeInfo.isBodyCodec()).isTrue();
            assertThat(writeInfo.bodyCodecName()).isEqualTo("fixed-offset");
        }

        @Test
        @DisplayName("writeSchemanoprimary key Avro Schema package bodycolumn")
        void writeSchemaWithNoPrimaryKey() {
            Schema schema = Schema.builder().field("val", Types.string()).build();

            TableMeta meta =
                    TableMeta.builder()
                            .withTableId(4L)
                            .withTableName("no_pk_table")
                            .withTableType(TableMeta.TableType.APPEND_ONLY)
                            .withSchema(schema)
                            .withProperties(Map.of("body.codec", "tlv"))
                            .build();

            ParquetSchemaAdapter.WriteSchemaInfo writeInfo = adapter.writeSchema(meta);
            assertThat(writeInfo.isBodyCodec()).isTrue();
            assertThat(writeInfo.writerSchema().getFields()).hasSize(1);
            assertThat(writeInfo.writerSchema().getField("body")).isNotNull();
        }

        @Test
        @DisplayName("readSchemaparseschemaVersion=0 correctly returnsdefaultversion")
        void readSchemaWithExplicitZeroVersion() throws IOException {
            Schema schema =
                    Schema.builder()
                            .field("uid", Types.int64())
                            .field("score", Types.float64())
                            .primaryKey("uid")
                            .build();

            TableMeta meta = buildTableMeta("fixed-offset", List.of("uid"), schema);
            ParquetSchemaAdapter.WriteSchemaInfo writeInfo = adapter.writeSchema(meta);

            Map<String, String> extraMeta =
                    Map.of(
                            "rawwise.bodyCodec",
                            writeInfo.bodyCodecName(),
                            "rawwise.schemaVersion",
                            "0");
            org.apache.hadoop.fs.Path parquetPath =
                    writeBodyCodecParquet(
                            "zero-version.parquet", writeInfo.writerSchema(), extraMeta);

            ParquetSchemaAdapter.ReadSchemaInfo readInfo = adapter.readSchema(parquetPath);
            assertThat(readInfo.isBodyCodec()).isTrue();
            assertThat(readInfo.schemaVersion()).isEqualTo(0);
        }

        @Test
        @DisplayName("writeSchema ColumnGroup")
        void writeSchemaPreservesColumnGroups() {
            Schema schema =
                    Schema.builder()
                            .field("id", Types.int64())
                            .field("name", Types.string())
                            .field("age", Types.int32())
                            .primaryKey("id")
                            .build();

            ColumnGroup cg =
                    new ColumnGroup(
                            "cg_profile",
                            List.of("name", "age"),
                            List.of(Types.string(), Types.int32()),
                            ColumnGroup.Encoding.FIXED_OFFSET,
                            1);
            Schema schemaWithCg = schema.withColumnGroups(List.of(cg));

            TableMeta meta =
                    TableMeta.builder()
                            .withTableId(5L)
                            .withTableName("cg_table")
                            .withTableType(TableMeta.TableType.PRIMARY_KEY)
                            .withSchema(schemaWithCg)
                            .withPrimaryKey(new PrimaryKey(List.of("id")))
                            .withProperties(Map.of("body.codec", "fixed-offset"))
                            .build();

            ParquetSchemaAdapter.WriteSchemaInfo writeInfo = adapter.writeSchema(meta);
            assertThat(writeInfo.columnGroups()).hasSize(1);
            assertThat(writeInfo.columnGroups().getFirst().name()).isEqualTo("cg_profile");
            assertThat(writeInfo.columnGroups().getFirst().columnNames())
                    .containsExactly("name", "age");
        }
    }
}
