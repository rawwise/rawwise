package io.github.rawwise.engine.compaction;

import io.github.rawwise.engine.compaction.merger.SortedFileReader;
import io.github.rawwise.engine.storage.ParquetFormatConstants;
import io.github.rawwise.engine.storage.codec.BodyCodec;
import io.github.rawwise.engine.storage.reader.AbstractReader;
import io.github.rawwise.model.record.ColumnGroup;
import io.github.rawwise.model.record.ColumnGroupCodec;
import io.github.rawwise.model.record.ColumnGroupCodecFactory;
import io.github.rawwise.model.record.RawWiseType;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.StructField;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;

class ParquetSortedFileReader extends AbstractReader implements SortedFileReader {

    private final ParquetReader<GenericRecord> reader;
    private GenericRecord current;
    private GenericRecord peeked;
    private boolean hasPeeked = false;
    private byte[] cachedKey = null;
    private long recordsRead = 0;
    private final Long externalCommitTimestamp;
    private final boolean multiColumn;
    private final BodyCodec bodyCodec;
    private final Schema schema;
    private final List<ColumnGroup> columnGroups;
    private final List<String> nonCgFieldNames;

    ParquetSortedFileReader(String path) throws IOException {
        this(path, null);
    }

    ParquetSortedFileReader(String path, Long externalCommitTimestamp) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path);
        this.reader =
                AvroParquetReader.<GenericRecord>builder(
                                HadoopInputFile.fromPath(hadoopPath, HadoopConfHolder.get()))
                        .build();
        this.externalCommitTimestamp = externalCommitTimestamp;
        this.multiColumn = false;
        this.bodyCodec = null;
        this.schema = null;
        this.columnGroups = null;
        this.nonCgFieldNames = null;
    }

    ParquetSortedFileReader(
            String path, BodyCodec bodyCodec, Schema schema, List<ColumnGroup> columnGroups)
            throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(path);
        this.reader =
                AvroParquetReader.<GenericRecord>builder(
                                HadoopInputFile.fromPath(hadoopPath, HadoopConfHolder.get()))
                        .build();
        this.externalCommitTimestamp = null;
        this.multiColumn = true;
        this.bodyCodec = bodyCodec;
        this.schema = schema;
        this.columnGroups = columnGroups;

        java.util.Set<String> cgCols = new java.util.HashSet<>();
        if (columnGroups != null) {
            for (ColumnGroup cg : columnGroups) {
                cgCols.addAll(cg.columnNames());
            }
        }
        this.nonCgFieldNames = new ArrayList<>();
        for (StructField f : schema.structType().fields()) {
            if (!cgCols.contains(f.name())) {
                nonCgFieldNames.add(f.name());
            }
        }
    }

    static boolean isMultiColumnFormat(org.apache.hadoop.fs.Path path) {
        try {
            ParquetMetadata meta =
                    ParquetFileReader.readFooter(new org.apache.hadoop.conf.Configuration(), path);
            String formatVer =
                    meta.getFileMetaData().getKeyValueMetaData().get("rawwise.formatVersion");
            return "2".equals(formatVer);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public byte[] currentKey() {
        String keyCol = multiColumn ? ParquetFormatConstants.PK_COLUMN_NAME : "key";
        ByteBuffer buf = (ByteBuffer) current.get(keyCol);
        byte[] bytes = new byte[buf.remaining()];
        buf.duplicate().get(bytes);
        return bytes;
    }

    @Override
    public byte[] currentValue() {
        if (multiColumn && bodyCodec != null && schema != null) {
            Map<String, Object> fields = new LinkedHashMap<>();

            if (columnGroups != null) {
                for (ColumnGroup cg : columnGroups) {
                    ByteBuffer buf = (ByteBuffer) current.get(cg.name());
                    if (buf != null) {
                        byte[] cgBytes = new byte[buf.remaining()];
                        buf.duplicate().get(cgBytes);
                        ColumnGroupCodec cgCodec = ColumnGroupCodecFactory.getCodec(cg);
                        fields.putAll(cgCodec.decode(cg, cgBytes));
                    }
                }
            }

            for (String fieldName : nonCgFieldNames) {
                ByteBuffer buf = (ByteBuffer) current.get(fieldName);
                if (buf != null) {
                    byte[] rawBytes = new byte[buf.remaining()];
                    buf.duplicate().get(rawBytes);
                    schema.structType()
                            .field(fieldName)
                            .ifPresent(
                                    sf -> {
                                        fields.put(
                                                fieldName,
                                                decodeNonCgFieldValue(rawBytes, sf.dataType()));
                                    });
                }
            }

            return bodyCodec.encode(schema, fields);
        }
        ByteBuffer buf = (ByteBuffer) current.get("value");
        byte[] bytes = new byte[buf.remaining()];
        buf.duplicate().get(bytes);
        return bytes;
    }

    private static Object decodeNonCgFieldValue(byte[] bytes, RawWiseType type) {
        return switch (type.name()) {
            case "int64" -> ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getLong();
            case "int32" -> ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt();
            case "float64" -> ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getDouble();
            case "float32" -> ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getFloat();
            case "boolean" -> bytes.length > 0 && bytes[0] != 0;
            case "string" -> new String(bytes, StandardCharsets.UTF_8);
            case "binary" -> bytes;
            default -> bytes;
        };
    }

    @Override
    public long currentSequenceNumber() {
        Object val = current.get("sequence_number");
        return val != null ? (Long) val : 0L;
    }

    @Override
    public long currentCommitTimestamp() {
        if (externalCommitTimestamp != null) return externalCommitTimestamp;
        Object val = current.get("commit_timestamp");
        return val != null ? (Long) val : 0L;
    }

    @Override
    public boolean currentIsTombstone() {
        Object val = current.get("is_tombstone");
        return val != null && (Boolean) val;
    }

    @Override
    public byte[] cachedKey() {
        return cachedKey;
    }

    @Override
    public boolean hasNext() throws IOException {
        ensureOpen();
        if (hasPeeked) return peeked != null;
        peeked = reader.read();
        hasPeeked = true;
        return peeked != null;
    }

    @Override
    public void advance() throws IOException {
        ensureOpen();
        if (!hasPeeked) {
            peeked = reader.read();
            hasPeeked = true;
        }
        current = peeked;
        peeked = null;
        hasPeeked = false;
        if (current != null) {
            cachedKey = currentKey();
            recordsRead++;
        }
    }

    @Override
    public long recordsRead() {
        return recordsRead;
    }

    @Override
    protected void doClose() throws IOException {
        try {
            reader.close();
        } catch (Exception e) {
            if (e instanceof IOException ioe) throw ioe;
            throw new IOException("Failed to close ParquetReader", e);
        }
    }
}
