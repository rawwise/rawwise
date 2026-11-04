package io.github.rawwise.engine.compaction;

import io.github.rawwise.engine.compaction.merger.OutputWriter;
import io.github.rawwise.engine.storage.ParquetFormatConstants;
import io.github.rawwise.engine.storage.block.index.SparseIndexBuilder;
import io.github.rawwise.engine.storage.codec.BodyCodec;
import io.github.rawwise.model.record.ColumnGroup;
import io.github.rawwise.model.record.ColumnGroupCodec;
import io.github.rawwise.model.record.ColumnGroupCodecFactory;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.StructField;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

class ParquetOutputWriter implements OutputWriter {

    private static final org.apache.avro.Schema SCHEMA =
            new org.apache.avro.Schema.Parser()
                    .parse(
                            "{\"type\":\"record\",\"name\":\"Kv\",\"fields\":["
                                    + "{\"name\":\"key\",\"type\":\"bytes\"},"
                                    + "{\"name\":\"value\",\"type\":\"bytes\"},"
                                    + "{\"name\":\"sequence_number\",\"type\":\"long\",\"default\":0},"
                                    + "{\"name\":\"commit_timestamp\",\"type\":\"long\",\"default\":0},"
                                    + "{\"name\":\"is_tombstone\",\"type\":\"boolean\",\"default\":false}]}");

    private final ParquetWriter<GenericRecord> writer;
    private long count;
    private final SparseIndexBuilder indexBuilder = new SparseIndexBuilder();
    private final boolean multiColumn;
    private final BodyCodec bodyCodec;
    private final Schema schema;
    private final List<ColumnGroup> columnGroups;
    private final List<String> nonCgFieldNames;
    private final org.apache.avro.Schema multiColumnAvroSchema;

    ParquetOutputWriter(String path) throws IOException {
        this.writer =
                AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(path))
                        .withSchema(SCHEMA)
                        .withConf(HadoopConfHolder.get())
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .build();
        this.multiColumn = false;
        this.bodyCodec = null;
        this.schema = null;
        this.columnGroups = null;
        this.nonCgFieldNames = null;
        this.multiColumnAvroSchema = null;
    }

    ParquetOutputWriter(
            String path, BodyCodec bodyCodec, Schema schema, List<ColumnGroup> columnGroups)
            throws IOException {
        this.multiColumn = true;
        this.bodyCodec = bodyCodec;
        this.schema = schema;
        this.columnGroups = columnGroups;

        java.util.Set<String> cgCols = new java.util.HashSet<>();
        if (columnGroups != null) {
            for (ColumnGroup cg : columnGroups) cgCols.addAll(cg.columnNames());
        }
        this.nonCgFieldNames = new ArrayList<>();
        for (StructField f : schema.structType().fields()) {
            if (!cgCols.contains(f.name()) && !"key".equals(f.name())) {
                nonCgFieldNames.add(f.name());
            }
        }

        this.multiColumnAvroSchema = buildMultiColumnAvroSchema(columnGroups, nonCgFieldNames);
        this.writer =
                AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(path))
                        .withSchema(multiColumnAvroSchema)
                        .withConf(HadoopConfHolder.get())
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .build();
    }

    private static org.apache.avro.Schema buildMultiColumnAvroSchema(
            List<ColumnGroup> cgs, List<String> nonCgNames) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"type\":\"record\",\"name\":\"CompactionOutput\",\"fields\":[");
        sb.append("{\"name\":\"")
                .append(ParquetFormatConstants.PK_COLUMN_NAME)
                .append("\",\"type\":\"bytes\"}");
        if (cgs != null) {
            for (ColumnGroup cg : cgs) {
                sb.append(",{\"name\":\"").append(cg.name()).append("\",\"type\":\"bytes\"}");
            }
        }
        for (String name : nonCgNames) {
            sb.append(",{\"name\":\"")
                    .append(name)
                    .append("\",\"type\":[\"null\",\"bytes\"],\"default\":null}");
        }
        sb.append(",{\"name\":\"sequence_number\",\"type\":\"long\",\"default\":0}");
        sb.append(",{\"name\":\"commit_timestamp\",\"type\":\"long\",\"default\":0}");
        sb.append(",{\"name\":\"is_tombstone\",\"type\":\"boolean\",\"default\":false}");
        sb.append("]}");
        return new org.apache.avro.Schema.Parser().parse(sb.toString());
    }

    @Override
    public void write(byte[] key, byte[] value) throws IOException {
        write(key, value, 0L, 0L, false);
    }

    @Override
    public void write(
            byte[] key,
            byte[] value,
            long sequenceNumber,
            long commitTimestamp,
            boolean isTombstone)
            throws IOException {
        if (multiColumn && bodyCodec != null && schema != null) {
            writeMultiColumn(key, value);
            return;
        }
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("key", ByteBuffer.wrap(key));
        record.put("value", ByteBuffer.wrap(value));
        record.put("sequence_number", sequenceNumber);
        record.put("commit_timestamp", commitTimestamp);
        record.put("is_tombstone", isTombstone);
        writer.write(record);
        indexBuilder.accept(key);
        count++;
    }

    private void writeMultiColumn(byte[] key, byte[] value) throws IOException {
        GenericRecord record = new GenericData.Record(multiColumnAvroSchema);
        record.put(ParquetFormatConstants.PK_COLUMN_NAME, ByteBuffer.wrap(key));
        record.put("sequence_number", 0L);
        record.put("commit_timestamp", 0L);
        record.put("is_tombstone", false);

        Map<String, Object> allValues = bodyCodec.decode(schema, value);

        if (columnGroups != null) {
            for (ColumnGroup cg : columnGroups) {
                Map<String, Object> cgValues = new LinkedHashMap<>();
                for (String colName : cg.columnNames()) {
                    cgValues.put(colName, allValues.get(colName));
                }
                ColumnGroupCodec cgCodec = ColumnGroupCodecFactory.getCodec(cg);
                byte[] cgBytes = cgCodec.encode(cg, cgValues);
                record.put(cg.name(), ByteBuffer.wrap(cgBytes));
            }
        }

        for (String fieldName : nonCgFieldNames) {
            Object fieldValue = allValues.get(fieldName);
            if (fieldValue != null) {
                record.put(fieldName, ByteBuffer.wrap(encodeNonCgValue(fieldValue)));
            }
        }

        writer.write(record);
        indexBuilder.accept(key);
        count++;
    }

    private static byte[] encodeNonCgValue(Object value) {
        if (value instanceof byte[] b) return b;
        if (value instanceof String s) return s.getBytes(StandardCharsets.UTF_8);
        if (value instanceof Long l) {
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.putLong(l);
            return buf.array();
        }
        if (value instanceof Integer i) {
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.putInt(i);
            return buf.array();
        }
        if (value instanceof Double d) {
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.putDouble(d);
            return buf.array();
        }
        if (value instanceof Float f) {
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.putFloat(f);
            return buf.array();
        }
        if (value instanceof Boolean b) {
            return new byte[] {(byte) (b ? 1 : 0)};
        }
        throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName());
    }

    @Override
    public long writtenRows() {
        return count;
    }

    public byte[] sparseIndex() {
        return indexBuilder.build();
    }

    @Override
    public void close() throws Exception {
        writer.close();
    }
}
