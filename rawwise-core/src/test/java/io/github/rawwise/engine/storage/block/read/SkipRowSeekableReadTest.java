package io.github.rawwise.engine.storage.block.read;

import static org.junit.jupiter.api.Assertions.*;

import io.github.rawwise.engine.storage.writer.DefaultParquetWriterBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.impl.RowSkipReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SkipRowSeekableReadTest {

    private static final String PK_COL = "__pk__";

    private static MessageType buildSchema() {
        return Types.buildMessage()
                .addField(
                        Types.required(
                                        org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
                                                .BINARY)
                                .named(PK_COL))
                .addField(
                        Types.optional(
                                        org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
                                                .BINARY)
                                .named("value"))
                .named("SkipRowTestSchema");
    }

    private static byte[] pkBytes(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }

    private static void writeRows(Path path, MessageType schema, String... keys)
            throws IOException {
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        GroupWriteSupport writeSupport = new GroupWriteSupport();

        try (ParquetWriter<Group> writer =
                new DefaultParquetWriterBuilder<>(path, writeSupport).withConf(conf).build()) {
            for (String key : keys) {
                SimpleGroup group = new SimpleGroup(schema);
                group.add(PK_COL, Binary.fromConstantByteArray(pkBytes(key)));
                group.add(
                        "value",
                        Binary.fromConstantByteArray(
                                ("val_" + key).getBytes(StandardCharsets.UTF_8)));
                writer.write(group);
            }
        }
    }

    private ParquetPointReader<Group> createSeekableReader(
            Configuration conf, Path path, int blockIndex, String keyColumnName)
            throws IOException {
        org.apache.parquet.hadoop.util.HadoopInputFile inputFile =
                org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(path, conf);
        ParquetFileReader fileReader = ParquetFileReader.open(inputFile);
        MessageType fileSchema = fileReader.getFooter().getFileMetaData().getSchema();
        Map<String, String> extraMeta =
                fileReader.getFooter().getFileMetaData().getKeyValueMetaData();
        long rowCount = fileReader.getFooter().getBlocks().get(blockIndex).getRowCount();

        GroupReadSupport readSupport = new GroupReadSupport();
        var readContext = readSupport.init(conf, extraMeta, fileSchema);
        RecordMaterializer<Group> materializer =
                readSupport.prepareForRead(conf, extraMeta, fileSchema, readContext);

        ColumnIOFactory columnIOFactory = new ColumnIOFactory(true);
        MessageColumnIO columnIO =
                columnIOFactory.getColumnIO(readContext.getRequestedSchema(), fileSchema);

        var pageStore = fileReader.readRowGroup(blockIndex);

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
                ColumnPath.fromDotString(keyColumnName),
                rowCount);
    }

    @Test
    void seekToFirstRowOfMultipleRows(@TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path file = tempDir.resolve("seek-first.parquet");
        Path hadoopPath = new Path(file.toUri().toString());
        MessageType schema = buildSchema();
        writeRows(hadoopPath, schema, "key_001", "key_002", "key_003", "key_004", "key_005");

        Configuration conf = new Configuration();
        ParquetPointReader<Group> reader = createSeekableReader(conf, hadoopPath, 0, PK_COL);
        int result = reader.seekTo(Binary.fromReusedByteArray(pkBytes("key_001")));
        assertEquals(0, result, "seekTo should find the first key");

        Group group = reader.read();
        assertNotNull(group, "read() should return the target row, not null");
        assertArrayEquals(pkBytes("key_001"), group.getBinary(PK_COL, 0).getBytes());
        assertEquals(
                "val_key_001",
                new String(group.getBinary("value", 0).getBytes(), StandardCharsets.UTF_8));
    }

    @Test
    void seekToMiddleRowSkipsCorrectly(@TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path file = tempDir.resolve("seek-middle.parquet");
        Path hadoopPath = new Path(file.toUri().toString());
        MessageType schema = buildSchema();
        writeRows(hadoopPath, schema, "key_001", "key_002", "key_003", "key_004", "key_005");

        Configuration conf = new Configuration();
        ParquetPointReader<Group> reader = createSeekableReader(conf, hadoopPath, 0, PK_COL);
        int result = reader.seekTo(Binary.fromReusedByteArray(pkBytes("key_003")));
        assertEquals(0, result, "seekTo should find the middle key");

        Group group = reader.read();
        assertNotNull(group, "read() should return the target row after skipping 2 rows, not null");
        assertArrayEquals(pkBytes("key_003"), group.getBinary(PK_COL, 0).getBytes());
        assertEquals(
                "val_key_003",
                new String(group.getBinary("value", 0).getBytes(), StandardCharsets.UTF_8));
    }

    @Test
    void seekToLastRowSkipsAllPreceding(@TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path file = tempDir.resolve("seek-last.parquet");
        Path hadoopPath = new Path(file.toUri().toString());
        MessageType schema = buildSchema();
        writeRows(hadoopPath, schema, "key_001", "key_002", "key_003", "key_004", "key_005");

        Configuration conf = new Configuration();
        ParquetPointReader<Group> reader = createSeekableReader(conf, hadoopPath, 0, PK_COL);
        int result = reader.seekTo(Binary.fromReusedByteArray(pkBytes("key_005")));
        assertEquals(0, result, "seekTo should find the last key");

        Group group = reader.read();
        assertNotNull(group, "read() should return the target row after skipping 4 rows, not null");
        assertArrayEquals(pkBytes("key_005"), group.getBinary(PK_COL, 0).getBytes());
        assertEquals(
                "val_key_005",
                new String(group.getBinary("value", 0).getBytes(), StandardCharsets.UTF_8));
    }

    @Test
    void seekToNonExistentKeyBetweenExistingReturnsMinusOne(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        java.nio.file.Path file = tempDir.resolve("seek-missing.parquet");
        Path hadoopPath = new Path(file.toUri().toString());
        MessageType schema = buildSchema();
        writeRows(hadoopPath, schema, "key_001", "key_003", "key_005");

        Configuration conf = new Configuration();
        ParquetPointReader<Group> reader = createSeekableReader(conf, hadoopPath, 0, PK_COL);
        int result = reader.seekTo(Binary.fromReusedByteArray(pkBytes("key_004")));
        assertEquals(-1, result, "seekTo should return -1 when target is between keys (overshot)");
    }

    @Test
    void seekToNonExistentKeyAfterAllReturnsOne(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        java.nio.file.Path file = tempDir.resolve("seek-after.parquet");
        Path hadoopPath = new Path(file.toUri().toString());
        MessageType schema = buildSchema();
        writeRows(hadoopPath, schema, "key_001", "key_003", "key_005");

        Configuration conf = new Configuration();
        ParquetPointReader<Group> reader = createSeekableReader(conf, hadoopPath, 0, PK_COL);
        int result = reader.seekTo(Binary.fromReusedByteArray(pkBytes("key_009")));
        assertEquals(1, result, "seekTo should return 1 when target > all keys");
    }

    @Test
    void seekToKeyBeforeAllReturnsMinusOne(@TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path file = tempDir.resolve("seek-before.parquet");
        Path hadoopPath = new Path(file.toUri().toString());
        MessageType schema = buildSchema();
        writeRows(hadoopPath, schema, "key_003", "key_005", "key_007");

        Configuration conf = new Configuration();
        ParquetPointReader<Group> reader = createSeekableReader(conf, hadoopPath, 0, PK_COL);
        int result = reader.seekTo(Binary.fromReusedByteArray(pkBytes("key_001")));
        assertEquals(-1, result, "seekTo should return -1 when target < current position");
    }

    @Test
    void seekToSingleRowFile(@TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path file = tempDir.resolve("seek-single.parquet");
        Path hadoopPath = new Path(file.toUri().toString());
        MessageType schema = buildSchema();
        writeRows(hadoopPath, schema, "key_only");

        Configuration conf = new Configuration();
        ParquetPointReader<Group> reader = createSeekableReader(conf, hadoopPath, 0, PK_COL);
        int result = reader.seekTo(Binary.fromReusedByteArray(pkBytes("key_only")));
        assertEquals(0, result);

        Group group = reader.read();
        assertNotNull(group);
        assertArrayEquals(pkBytes("key_only"), group.getBinary(PK_COL, 0).getBytes());
        assertEquals(
                "val_key_only",
                new String(group.getBinary("value", 0).getBytes(), StandardCharsets.UTF_8));
    }

    @Test
    void seekToWithLargeSkipCount(@TempDir java.nio.file.Path tempDir) throws Exception {
        java.nio.file.Path file = tempDir.resolve("seek-large-skip.parquet");
        Path hadoopPath = new Path(file.toUri().toString());
        MessageType schema = buildSchema();

        String[] keys = new String[100];
        for (int i = 0; i < 100; i++) {
            keys[i] = String.format("key_%03d", i);
        }
        writeRows(hadoopPath, schema, keys);

        Configuration conf = new Configuration();
        ParquetPointReader<Group> reader = createSeekableReader(conf, hadoopPath, 0, PK_COL);
        int result = reader.seekTo(Binary.fromReusedByteArray(pkBytes("key_099")));
        assertEquals(0, result, "seekTo should find the last key in 100-row file");

        Group group = reader.read();
        assertNotNull(group, "read() should return the target after skipping 99 rows");
        assertArrayEquals(pkBytes("key_099"), group.getBinary(PK_COL, 0).getBytes());
        assertEquals(
                "val_key_099",
                new String(group.getBinary("value", 0).getBytes(), StandardCharsets.UTF_8));
    }
}
