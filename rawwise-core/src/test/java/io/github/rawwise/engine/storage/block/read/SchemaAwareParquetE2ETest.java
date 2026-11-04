package io.github.rawwise.engine.storage.block.read;

import static org.junit.jupiter.api.Assertions.*;

import io.github.rawwise.engine.storage.ParquetFormatConstants;
import io.github.rawwise.engine.storage.codec.BodyCodec;
import io.github.rawwise.engine.storage.codec.BodyCodecRegistry;
import io.github.rawwise.engine.storage.writer.DefaultParquetWriterBuilder;
import io.github.rawwise.model.record.ColumnGroup;
import io.github.rawwise.model.record.ColumnGroupCodec;
import io.github.rawwise.model.record.ColumnGroupCodecFactory;
import io.github.rawwise.model.record.PrimitiveType;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.StructField;
import io.github.rawwise.model.record.StructType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SchemaAwareParquetE2ETest {

    private static final String PK_COLUMN_NAME = "__pk__";

    @Test
    void writeMultiColumnThenReadBackAllFieldsCorrect(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        Schema schema =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("user_id", PrimitiveType.STRING),
                                        new StructField("name", PrimitiveType.STRING),
                                        new StructField("email", PrimitiveType.STRING),
                                        new StructField("age", PrimitiveType.LONG),
                                        new StructField("score", PrimitiveType.DOUBLE),
                                        new StructField("active", PrimitiveType.BOOLEAN),
                                        new StructField("address", PrimitiveType.STRING))),
                        Map.of(),
                        1L,
                        List.of(),
                        List.of(
                                new ColumnGroup(
                                        "cg_basic",
                                        List.of("name", "email"),
                                        List.of(PrimitiveType.STRING, PrimitiveType.STRING),
                                        ColumnGroup.Encoding.FIXED_OFFSET,
                                        1),
                                new ColumnGroup(
                                        "cg_stats",
                                        List.of("age", "score"),
                                        List.of(PrimitiveType.LONG, PrimitiveType.DOUBLE),
                                        ColumnGroup.Encoding.FIXED_OFFSET,
                                        1)));

        List<String> pkNames = List.of("user_id");
        List<String> nonCgFieldNames =
                ParquetFormatConstants.computeNonCgFieldNames(schema, schema.columnGroups());

        ColumnGroup cgBasic = schema.columnGroups().get(0);
        ColumnGroup cgStats = schema.columnGroups().get(1);

        MessageType parquetSchema =
                buildParquetSchema(pkNames, schema.columnGroups(), nonCgFieldNames);

        BodyCodec bodyCodec = BodyCodecRegistry.create("fixed-offset", schema);
        ColumnGroupCodec cgBasicCodec = ColumnGroupCodecFactory.getCodec(cgBasic);
        ColumnGroupCodec cgStatsCodec = ColumnGroupCodecFactory.getCodec(cgStats);

        Map<String, Object> row1 = new LinkedHashMap<>();
        row1.put("user_id", "001");
        row1.put("name", "Alice");
        row1.put("email", "alice@example.com");
        row1.put("age", 30L);
        row1.put("score", 95.5);
        row1.put("active", true);
        row1.put("address", "123 Main St");

        Map<String, Object> row2 = new LinkedHashMap<>();
        row2.put("user_id", "002");
        row2.put("name", "Bob");
        row2.put("email", "bob@example.com");
        row2.put("age", 25L);
        row2.put("score", 88.0);
        row2.put("active", false);
        row2.put("address", "456 Oak Ave");

        List<Map<String, Object>> testRows = List.of(row1, row2);

        java.nio.file.Path parquetFile = tempDir.resolve("multi-column-test.parquet");
        Path hadoopPath = new Path(parquetFile.toUri().toString());
        Configuration conf = new Configuration();

        GroupWriteSupport writeSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(parquetSchema, conf);

        try (ParquetWriter<Group> writer =
                new DefaultParquetWriterBuilder<>(hadoopPath, writeSupport)
                        .withConf(conf)
                        .build()) {

            for (Map<String, Object> row : testRows) {
                SimpleGroup group = new SimpleGroup(parquetSchema);

                byte[] pkBytes = ((String) row.get("user_id")).getBytes(StandardCharsets.UTF_8);
                group.add(PK_COLUMN_NAME, Binary.fromConstantByteBuffer(ByteBuffer.wrap(pkBytes)));

                Map<String, Object> cgBasicValues = new LinkedHashMap<>();
                cgBasicValues.put("name", row.get("name"));
                cgBasicValues.put("email", row.get("email"));
                byte[] cgBasicBytes = cgBasicCodec.encode(cgBasic, cgBasicValues);
                group.add("cg_basic", Binary.fromConstantByteBuffer(ByteBuffer.wrap(cgBasicBytes)));

                Map<String, Object> cgStatsValues = new LinkedHashMap<>();
                cgStatsValues.put("age", row.get("age"));
                cgStatsValues.put("score", row.get("score"));
                byte[] cgStatsBytes = cgStatsCodec.encode(cgStats, cgStatsValues);
                group.add("cg_stats", Binary.fromConstantByteBuffer(ByteBuffer.wrap(cgStatsBytes)));

                group.add(
                        "user_id",
                        Binary.fromConstantByteBuffer(
                                ByteBuffer.wrap(
                                        ((String) row.get("user_id"))
                                                .getBytes(StandardCharsets.UTF_8))));
                group.add(
                        "active",
                        Binary.fromConstantByteBuffer(
                                ByteBuffer.wrap(
                                        new byte[] {
                                            (byte) (((Boolean) row.get("active")) ? 1 : 0)
                                        })));
                group.add(
                        "address",
                        Binary.fromConstantByteBuffer(
                                ByteBuffer.wrap(
                                        ((String) row.get("address"))
                                                .getBytes(StandardCharsets.UTF_8))));

                writer.write(group);
            }
        }

        ParquetMetadata footer = ParquetFileReader.readFooter(conf, hadoopPath);
        MessageType readSchema = footer.getFileMetaData().getSchema();

        List<String> expectedColumns =
                List.of(PK_COLUMN_NAME, "cg_basic", "cg_stats", "user_id", "active", "address");
        for (String col : expectedColumns) {
            assertNotNull(readSchema.getType(col), "Schema should contain column: " + col);
        }
        assertEquals(
                expectedColumns.size(),
                readSchema.getFields().size(),
                "Schema should have exactly " + expectedColumns.size() + " columns");

        List<BlockMetaData> blocks = footer.getBlocks();
        assertEquals(1, blocks.size());
        BlockMetaData block = blocks.get(0);
        assertEquals(2, block.getRowCount());

        try (ParquetReader<Group> reader =
                ParquetReader.builder(new GroupReadSupport(), hadoopPath).withConf(conf).build()) {

            for (int i = 0; i < testRows.size(); i++) {
                Group readGroup = reader.read();
                assertNotNull(readGroup, "Row " + i + " should not be null");

                Map<String, Object> expected = testRows.get(i);

                byte[] readPkBytes = readGroup.getBinary(PK_COLUMN_NAME, 0).getBytes();
                assertEquals(
                        expected.get("user_id"), new String(readPkBytes, StandardCharsets.UTF_8));

                byte[] readUserIdBytes = readGroup.getBinary("user_id", 0).getBytes();
                assertEquals(
                        expected.get("user_id"),
                        new String(readUserIdBytes, StandardCharsets.UTF_8));

                byte[] readCgBasicBytes = readGroup.getBinary("cg_basic", 0).getBytes();
                Map<String, Object> decodedCgBasic = cgBasicCodec.decode(cgBasic, readCgBasicBytes);
                assertEquals(expected.get("name"), decodedCgBasic.get("name"));
                assertEquals(expected.get("email"), decodedCgBasic.get("email"));

                byte[] readCgStatsBytes = readGroup.getBinary("cg_stats", 0).getBytes();
                Map<String, Object> decodedCgStats = cgStatsCodec.decode(cgStats, readCgStatsBytes);
                assertEquals(expected.get("age"), decodedCgStats.get("age"));
                assertEquals(expected.get("score"), decodedCgStats.get("score"));

                byte[] activeBytes = readGroup.getBinary("active", 0).getBytes();
                assertEquals(expected.get("active"), activeBytes.length > 0 && activeBytes[0] != 0);

                byte[] addressBytes = readGroup.getBinary("address", 0).getBytes();
                assertEquals(
                        expected.get("address"), new String(addressBytes, StandardCharsets.UTF_8));
            }

            assertNull(reader.read(), "No more rows expected");
        }
    }

    @Test
    void multiColumnFormatWithBodyCodecRoundTrip(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        Schema schema =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("id", PrimitiveType.STRING),
                                        new StructField("val_a", PrimitiveType.STRING),
                                        new StructField("val_b", PrimitiveType.LONG),
                                        new StructField("val_c", PrimitiveType.DOUBLE),
                                        new StructField("val_d", PrimitiveType.BOOLEAN))),
                        Map.of(),
                        1L,
                        List.of(),
                        List.of(
                                new ColumnGroup(
                                        "cg1",
                                        List.of("val_a", "val_b"),
                                        List.of(PrimitiveType.STRING, PrimitiveType.LONG),
                                        ColumnGroup.Encoding.FIXED_OFFSET,
                                        1)));

        List<String> pkNames = List.of("id");
        List<String> nonCgFieldNames =
                ParquetFormatConstants.computeNonCgFieldNames(schema, schema.columnGroups());
        assertEquals(List.of("id", "val_c", "val_d"), nonCgFieldNames);

        MessageType parquetSchema =
                buildParquetSchema(pkNames, schema.columnGroups(), nonCgFieldNames);

        BodyCodec bodyCodec = BodyCodecRegistry.create("fixed-offset", schema);
        ColumnGroup cg1 = schema.columnGroups().get(0);
        ColumnGroupCodec cgCodec = ColumnGroupCodecFactory.getCodec(cg1);

        Map<String, Object> allFields = new LinkedHashMap<>();
        allFields.put("id", "key-1");
        allFields.put("val_a", "hello");
        allFields.put("val_b", 99L);
        allFields.put("val_c", 3.14);
        allFields.put("val_d", true);

        byte[] bodyBytes = bodyCodec.encode(schema, allFields);
        Map<String, Object> decoded = bodyCodec.decode(schema, bodyBytes);
        assertEquals(allFields.get("id"), decoded.get("id"));
        assertEquals(allFields.get("val_a"), decoded.get("val_a"));
        assertEquals(allFields.get("val_b"), decoded.get("val_b"));
        assertEquals(allFields.get("val_c"), decoded.get("val_c"));
        Object boolVal = decoded.get("val_d");
        if (boolVal instanceof Number n) {
            assertTrue(n.intValue() != 0, "val_d should be truthy");
        } else {
            assertEquals(allFields.get("val_d"), boolVal);
        }

        java.nio.file.Path parquetFile = tempDir.resolve("roundtrip.parquet");
        Path hadoopPath = new Path(parquetFile.toUri().toString());
        Configuration conf = new Configuration();

        GroupWriteSupport writeSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(parquetSchema, conf);

        try (ParquetWriter<Group> writer =
                new DefaultParquetWriterBuilder<>(hadoopPath, writeSupport)
                        .withConf(conf)
                        .build()) {

            SimpleGroup group = new SimpleGroup(parquetSchema);
            group.add(
                    PK_COLUMN_NAME,
                    Binary.fromConstantByteBuffer(
                            ByteBuffer.wrap(
                                    ((String) allFields.get("id"))
                                            .getBytes(StandardCharsets.UTF_8))));

            Map<String, Object> cgVals = new LinkedHashMap<>();
            cgVals.put("val_a", allFields.get("val_a"));
            cgVals.put("val_b", allFields.get("val_b"));
            byte[] cgBytes = cgCodec.encode(cg1, cgVals);
            group.add("cg1", Binary.fromConstantByteBuffer(ByteBuffer.wrap(cgBytes)));

            group.add(
                    "id",
                    Binary.fromConstantByteBuffer(
                            ByteBuffer.wrap(
                                    ((String) allFields.get("id"))
                                            .getBytes(StandardCharsets.UTF_8))));

            byte[] valCBytes = doubleToBytes((Double) allFields.get("val_c"));
            group.add("val_c", Binary.fromConstantByteBuffer(ByteBuffer.wrap(valCBytes)));

            byte[] valDBytes = new byte[] {(byte) (((Boolean) allFields.get("val_d")) ? 1 : 0)};
            group.add("val_d", Binary.fromConstantByteBuffer(ByteBuffer.wrap(valDBytes)));

            writer.write(group);
        }

        ParquetMetadata footer = ParquetFileReader.readFooter(conf, hadoopPath);
        MessageType readSchema = footer.getFileMetaData().getSchema();

        assertNotNull(readSchema.getType(PK_COLUMN_NAME));
        assertNotNull(readSchema.getType("cg1"));
        assertNotNull(readSchema.getType("id"));
        assertNotNull(readSchema.getType("val_c"));
        assertNotNull(readSchema.getType("val_d"));

        try (ParquetReader<Group> reader =
                ParquetReader.builder(new GroupReadSupport(), hadoopPath).withConf(conf).build()) {

            Group readGroup = reader.read();
            assertNotNull(readGroup);

            byte[] readId = readGroup.getBinary(PK_COLUMN_NAME, 0).getBytes();
            assertEquals("key-1", new String(readId, StandardCharsets.UTF_8));

            byte[] readCgBytes = readGroup.getBinary("cg1", 0).getBytes();
            Map<String, Object> decodedCg = cgCodec.decode(cg1, readCgBytes);
            assertEquals("hello", decodedCg.get("val_a"));
            assertEquals(99L, decodedCg.get("val_b"));

            byte[] readValC = readGroup.getBinary("val_c", 0).getBytes();
            assertEquals(3.14, bytesToDouble(readValC), 0.001);

            byte[] readValD = readGroup.getBinary("val_d", 0).getBytes();
            assertTrue(readValD.length > 0 && readValD[0] != 0);
        }
    }

    @Test
    void nonCgFieldsExcludePkAndCgCorrectly() {
        Schema schema =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("pk", PrimitiveType.STRING),
                                        new StructField("field_a", PrimitiveType.STRING),
                                        new StructField("field_b", PrimitiveType.STRING),
                                        new StructField("field_c", PrimitiveType.LONG))),
                        Map.of(),
                        1L,
                        List.of(),
                        List.of(
                                new ColumnGroup(
                                        "cg1",
                                        List.of("field_a", "field_b"),
                                        List.of(PrimitiveType.STRING, PrimitiveType.STRING),
                                        ColumnGroup.Encoding.FIXED_OFFSET,
                                        1)));

        List<String> nonCg =
                ParquetFormatConstants.computeNonCgFieldNames(schema, schema.columnGroups());
        assertEquals(
                List.of("pk", "field_c"),
                nonCg,
                "PK and non-CG fields should be in nonCg (only CG fields excluded)");
    }

    private static MessageType buildParquetSchema(
            List<String> pkNames, List<ColumnGroup> columnGroups, List<String> nonCgFieldNames) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        builder.addField(
                Types.required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
                        .named(PK_COLUMN_NAME));
        if (columnGroups != null) {
            for (ColumnGroup cg : columnGroups) {
                builder.addField(
                        Types.required(
                                        org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
                                                .BINARY)
                                .named(cg.name()));
            }
        }
        for (String fieldName : nonCgFieldNames) {
            builder.addField(
                    Types.optional(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
                            .named(fieldName));
        }
        return builder.named("RawWiseSchemaAwareRecord");
    }

    private static byte[] doubleToBytes(double value) {
        ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        buf.putDouble(value);
        return buf.array();
    }

    private static double bytesToDouble(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getDouble();
    }
}
