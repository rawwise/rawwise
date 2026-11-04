package io.github.rawwise.engine.storage.block.read;

import static org.junit.jupiter.api.Assertions.*;

import io.github.rawwise.engine.storage.ParquetFormatConstants;
import io.github.rawwise.engine.storage.writer.DefaultParquetWriterBuilder;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies commit_timestamp round-trip through Parquet write/read, including legacy compatibility
 * (column missing -> Long.MIN_VALUE).
 */
class ParquetCommitTsE2ETest {

    private static final String COMMIT_TS_COL = ParquetFormatConstants.COMMIT_TIMESTAMP_COLUMN_NAME;

    private static final MessageType SCHEMA_WITH_COMMIT_TS =
            Types.buildMessage()
                    .addField(
                            Types.required(
                                            org.apache.parquet.schema.PrimitiveType
                                                    .PrimitiveTypeName.BINARY)
                                    .named("key"))
                    .addField(
                            Types.required(
                                            org.apache.parquet.schema.PrimitiveType
                                                    .PrimitiveTypeName.INT64)
                                    .named(COMMIT_TS_COL))
                    .named("RawWiseCommitTsRecord");

    private static final MessageType SCHEMA_WITHOUT_COMMIT_TS =
            Types.buildMessage()
                    .addField(
                            Types.required(
                                            org.apache.parquet.schema.PrimitiveType
                                                    .PrimitiveTypeName.BINARY)
                                    .named("key"))
                    .named("RawWiseLegacyRecord");

    @Test
    void writeReadCommitTsRoundTrip(@TempDir java.nio.file.Path tempDir) throws Exception {
        Path filePath = new Path(tempDir.resolve("committs.parquet").toString());

        long expectedCommitTs = 42L;
        byte[] keyBytes = "test-key".getBytes();

        GroupWriteSupport.setSchema(SCHEMA_WITH_COMMIT_TS, new Configuration());
        DefaultParquetWriterBuilder<Group> builder =
                new DefaultParquetWriterBuilder<>(filePath, new GroupWriteSupport());
        try (ParquetWriter<Group> writer =
                builder.withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE)
                        .build()) {

            SimpleGroup group = new SimpleGroup(SCHEMA_WITH_COMMIT_TS);
            group.append("key", Binary.fromConstantByteArray(keyBytes));
            group.append(COMMIT_TS_COL, expectedCommitTs);
            writer.write(group);
        }

        ParquetMetadata meta = ParquetFileReader.readFooter(new Configuration(), filePath);
        assertFalse(meta.getBlocks().isEmpty(), "Parquet file must have at least one row group");

        GroupReadSupport readSupport = new GroupReadSupport();
        readSupport.init(new Configuration(), null, SCHEMA_WITH_COMMIT_TS);
        try (ParquetReader<Group> reader = ParquetReader.builder(readSupport, filePath).build()) {
            Group group = reader.read();
            assertNotNull(group, "Must read a record from Parquet");

            long actualCommitTs = group.getLong(COMMIT_TS_COL, 0);
            assertEquals(
                    expectedCommitTs,
                    actualCommitTs,
                    "commit_timestamp in Parquet must equal original value");
        }

        long readCommitTs = readCommitTsFromGroup(filePath, SCHEMA_WITH_COMMIT_TS);
        assertEquals(
                expectedCommitTs,
                readCommitTs,
                "readCommitTsFromGroup must return original commitTs");
    }

    @Test
    void legacyParquetWithoutCommitTsColumnReturnsMinValue(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        Path filePath = new Path(tempDir.resolve("legacy.parquet").toString());

        GroupWriteSupport.setSchema(SCHEMA_WITHOUT_COMMIT_TS, new Configuration());
        DefaultParquetWriterBuilder<Group> builder =
                new DefaultParquetWriterBuilder<>(filePath, new GroupWriteSupport());
        try (ParquetWriter<Group> writer =
                builder.withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE)
                        .build()) {

            SimpleGroup group = new SimpleGroup(SCHEMA_WITHOUT_COMMIT_TS);
            group.append("key", Binary.fromConstantByteArray("legacy-key".getBytes()));
            writer.write(group);
        }

        long readCommitTs = readCommitTsFromGroup(filePath, SCHEMA_WITHOUT_COMMIT_TS);
        assertEquals(
                Long.MIN_VALUE,
                readCommitTs,
                "Legacy Parquet without commit_timestamp column must return Long.MIN_VALUE sentinel");
    }

    @Test
    void multipleRowsAllHaveCorrectCommitTs(@TempDir java.nio.file.Path tempDir) throws Exception {
        Path filePath = new Path(tempDir.resolve("multi.parquet").toString());

        GroupWriteSupport.setSchema(SCHEMA_WITH_COMMIT_TS, new Configuration());
        DefaultParquetWriterBuilder<Group> builder =
                new DefaultParquetWriterBuilder<>(filePath, new GroupWriteSupport());
        try (ParquetWriter<Group> writer =
                builder.withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE)
                        .build()) {
            for (int i = 1; i <= 5; i++) {
                SimpleGroup group = new SimpleGroup(SCHEMA_WITH_COMMIT_TS);
                group.append("key", Binary.fromConstantByteArray(("key-" + i).getBytes()));
                group.append(COMMIT_TS_COL, (long) i * 10);
                writer.write(group);
            }
        }

        List<Long> commitTsList = new ArrayList<>();
        GroupReadSupport readSupport = new GroupReadSupport();
        readSupport.init(new Configuration(), null, SCHEMA_WITH_COMMIT_TS);
        try (ParquetReader<Group> reader = ParquetReader.builder(readSupport, filePath).build()) {
            Group group;
            while ((group = reader.read()) != null) {
                commitTsList.add(group.getLong(COMMIT_TS_COL, 0));
            }
        }

        assertEquals(5, commitTsList.size(), "Must read all 5 rows");
        assertEquals(
                List.of(10L, 20L, 30L, 40L, 50L),
                commitTsList,
                "All rows must have correct commitTs values");
    }

    @Test
    void commitTsZeroIsPreservedNotMistakenForSentinel(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        Path filePath = new Path(tempDir.resolve("zero_committs.parquet").toString());

        GroupWriteSupport.setSchema(SCHEMA_WITH_COMMIT_TS, new Configuration());
        DefaultParquetWriterBuilder<Group> builder =
                new DefaultParquetWriterBuilder<>(filePath, new GroupWriteSupport());
        try (ParquetWriter<Group> writer =
                builder.withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE)
                        .build()) {

            SimpleGroup group = new SimpleGroup(SCHEMA_WITH_COMMIT_TS);
            group.append("key", Binary.fromConstantByteArray("zero-ts-key".getBytes()));
            group.append(COMMIT_TS_COL, 0L);
            writer.write(group);
        }

        long readCommitTs = readCommitTsFromGroup(filePath, SCHEMA_WITH_COMMIT_TS);
        assertEquals(
                0L,
                readCommitTs,
                "commitTs=0 must be preserved (not mistaken for Long.MIN_VALUE sentinel)");
    }

    private static long readCommitTsFromGroup(Path filePath, MessageType schema) throws Exception {
        GroupReadSupport readSupport = new GroupReadSupport();
        readSupport.init(new Configuration(), null, schema);
        try (ParquetReader<Group> reader = ParquetReader.builder(readSupport, filePath).build()) {
            Group group = reader.read();
            if (group == null) return Long.MIN_VALUE;
            try {
                return group.getLong(COMMIT_TS_COL, 0);
            } catch (RuntimeException e) {
                return Long.MIN_VALUE;
            }
        }
    }
}
