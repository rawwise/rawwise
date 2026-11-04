package io.github.rawwise.model.op;

import io.github.rawwise.engine.storage.codec.BodyCodec;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import io.github.rawwise.engine.storage.writer.BatchWriter;
import io.github.rawwise.engine.storage.writer.RecordWriter;
import io.github.rawwise.model.record.RawWiseTable;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.StructField;
import io.github.rawwise.model.record.WriteEntry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KvWrite implements Write {
    private static final OpType TYPE = OpType.KV_WRITE;
    private final RawWiseTable table;
    private final List<Map<String, Object>> data;
    private final byte[] key;
    private final byte[] value;
    private final boolean valuePreEncoded;
    @Getter private final Integer partitionHint;
    private final RecordWriter<WriteEntry> writer;
    private Schema schema;
    private KeyCodec keyCodec;
    private BodyCodec bodyCodec;

    public KvWrite(
            RawWiseTable table, List<Map<String, Object>> data, RecordWriter<WriteEntry> writer) {
        this(table, data, null, null, false, null, writer);
    }

    public KvWrite(
            RawWiseTable table,
            List<Map<String, Object>> data,
            Integer partitionHint,
            RecordWriter<WriteEntry> writer) {
        this(table, data, null, null, false, partitionHint, writer);
    }

    public KvWrite(
            RawWiseTable table,
            byte[] key,
            byte[] value,
            Integer partitionHint,
            RecordWriter<WriteEntry> writer) {
        this(table, null, key, value, false, partitionHint, writer);
    }

    public KvWrite(
            RawWiseTable table,
            byte[] key,
            byte[] value,
            boolean valuePreEncoded,
            Integer partitionHint,
            RecordWriter<WriteEntry> writer) {
        this(table, null, key, value, valuePreEncoded, partitionHint, writer);
    }

    public KvWrite(
            RawWiseTable table,
            List<Map<String, Object>> data,
            Integer partitionHint,
            RecordWriter<WriteEntry> writer,
            Schema schema,
            KeyCodec keyCodec,
            BodyCodec bodyCodec) {
        this(table, data, null, null, false, partitionHint, writer);
        this.schema = schema;
        this.keyCodec = keyCodec;
        this.bodyCodec = bodyCodec;
    }

    private KvWrite(
            RawWiseTable table,
            List<Map<String, Object>> data,
            byte[] key,
            byte[] value,
            boolean valuePreEncoded,
            Integer partitionHint,
            RecordWriter<WriteEntry> writer) {
        this.table = table;
        this.data = data;
        this.key = key;
        this.value = value;
        this.valuePreEncoded = valuePreEncoded;
        this.partitionHint = partitionHint;
        this.writer = writer;
    }

    @Override
    public Write.Result exec() {
        if (isDirectKvOperation()) {
            return execDirectKvWrite();
        }
        return execMapDataWrite();
    }

    public Insert.Result execAsInsert() {
        Write.Result result = exec();
        return new Insert.Result(result.success(), result.count(), result.message().orElse(null));
    }

    private boolean isDirectKvOperation() {
        return key != null && value != null;
    }

    private Write.Result execDirectKvWrite() {
        try {
            WriteEntry entry = new WriteEntry(key, value, valuePreEncoded);
            writer.write(entry);
            return Write.Result.ok(1);
        } catch (Exception e) {
            return Write.Result.error("Insert failed: " + e.getMessage());
        }
    }

    private Write.Result execMapDataWrite() {
        if (data == null || data.isEmpty()) {
            return Write.Result.ok(0);
        }

        List<Map<String, Object>> nonEmptyData =
                data.stream().filter(Objects::nonNull).filter(m -> !m.isEmpty()).toList();

        if (nonEmptyData.isEmpty()) {
            return Write.Result.ok(0);
        }

        List<WriteEntry> entries = convertToWriteEntries(nonEmptyData);
        if (entries.isEmpty()) {
            return Write.Result.ok(0);
        }

        if (writer instanceof BatchWriter) {
            @SuppressWarnings("unchecked")
            BatchWriter<WriteEntry> batchWriter = (BatchWriter<WriteEntry>) writer;
            try {
                var batchResult = batchWriter.batchWrite(entries);
                return Write.Result.ok(batchResult.recordsWritten());
            } catch (Exception e) {
                log.error("Batch write failed", e);
                return Write.Result.error("Batch write failed: " + e.getMessage());
            }
        }

        try {
            int count = 0;
            for (WriteEntry entry : entries) {
                writer.write(entry);
                count++;
            }
            return Write.Result.ok(count);
        } catch (Exception e) {
            log.error("Write failed", e);
            return Write.Result.error("Write failed: " + e.getMessage());
        }
    }

    private List<WriteEntry> convertToWriteEntries(List<Map<String, Object>> nonEmptyData) {
        WriteEntryConverter converter = new WriteEntryConverter();
        List<WriteEntry> entries = new ArrayList<>();
        for (Map<String, Object> rowData : nonEmptyData) {
            if (schema != null && keyCodec != null) {
                try {
                    WriteEntry entry = converter.convert(rowData, keyCodec, bodyCodec, schema);
                    if (entry != null) entries.add(entry);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to convert row data", e);
                }
            } else {
                byte[] keyBytes = serializeToBytes(rowData.get("key"));
                byte[] valueBytes = serializeToBytes(rowData.get("value"));
                if (keyBytes != null && valueBytes != null) {
                    entries.add(new WriteEntry(keyBytes, valueBytes, false));
                }
            }
        }
        return entries;
    }

    @Override
    public long writeSize() {
        if (isDirectKvOperation()) {
            return 1;
        }
        return data != null ? data.size() : 0;
    }

    @Override
    public OpType type() {
        return TYPE;
    }

    private static byte[] serializeToBytes(Object obj) {
        if (obj == null) return null;
        if (obj instanceof byte[] ba) return ba;
        if (obj instanceof String s) return s.getBytes(StandardCharsets.UTF_8);
        if (obj instanceof Number n) return n.toString().getBytes(StandardCharsets.UTF_8);
        return obj.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static class WriteEntryConverter {
        private final Map<String, Object> fieldValues = new HashMap<>();
        private final Map<String, Comparable> pkValues = new LinkedHashMap<>();

        WriteEntry convert(
                Map<String, Object> mapEntry, KeyCodec keyCodec, BodyCodec bodyCodec, Schema schema)
                throws IOException {
            fieldValues.clear();
            pkValues.clear();
            if (mapEntry == null || mapEntry.isEmpty()) return null;
            if (schema == null || keyCodec == null) return null;
            for (StructField field : schema.fields()) {
                Object value = null;
                if (field.hasAssignedFieldId()) {
                    String fieldIdKey = String.valueOf(field.fieldId());
                    if (mapEntry.containsKey(fieldIdKey)) {
                        value = mapEntry.get(fieldIdKey);
                    } else {
                        value = mapEntry.get(field.name());
                    }
                } else {
                    value = mapEntry.get(field.name());
                }
                if (value != null) {
                    fieldValues.put(field.name(), value);
                }
            }
            if (fieldValues.isEmpty()) return null;
            for (String pkField : schema.primaryKeys()) {
                Object val = fieldValues.get(pkField);
                if (val == null) {
                    throw new IllegalArgumentException(
                            "Primary key field '" + pkField + "' must not be null");
                }
                if (!(val instanceof Comparable)) {
                    throw new IllegalArgumentException(
                            "Primary key field '"
                                    + pkField
                                    + "' value must be Comparable, got: "
                                    + val.getClass().getName());
                }
                pkValues.put(pkField, (Comparable) val);
            }
            byte[] keyBytes;
            if (!pkValues.isEmpty()) {
                keyBytes = keyCodec.encodePrimaryKey(pkValues);
            } else {
                Object firstValue = fieldValues.values().iterator().next();
                keyBytes = serializeToBytes(firstValue);
            }
            byte[] valueBytes;
            if (bodyCodec != null) {
                valueBytes = bodyCodec.encode(schema, fieldValues);
            } else {
                valueBytes = serializeToBytes(fieldValues);
            }
            return new WriteEntry(keyBytes, valueBytes, false);
        }
    }
}
