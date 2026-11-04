package io.github.rawwise.model.record;

import io.github.rawwise.engine.storage.codec.ValueCodec;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class Records {

    public record EncodedKey(byte[] encoded) implements Record.Key, Serializable {
        public EncodedKey {
            encoded = encoded.clone();
        }

        @Override
        public byte[] encoded() {
            return encoded.clone();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof EncodedKey that)) return false;
            return Arrays.equals(encoded, that.encoded);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(encoded);
        }
    }

    public static Record.Key keyOf(byte[] encoded) {
        return new EncodedKey(encoded);
    }

    private Records() {}

    public static Record create(
            Record.Key key,
            Record.Body body,
            long tableId,
            long sequenceNumber,
            long commitTimestamp,
            Record.Kind kind,
            boolean visible) {
        return create(
                key,
                body,
                tableId,
                sequenceNumber,
                commitTimestamp,
                kind,
                visible,
                true,
                ImmutabilitySource.DEFAULT);
    }

    public static Record create(
            Record.Key key,
            Record.Body body,
            long tableId,
            long sequenceNumber,
            long commitTimestamp,
            Record.Kind kind,
            boolean visible,
            boolean immutable,
            ImmutabilitySource immutableSource) {
        return new DefaultRecord(
                key,
                body,
                new Record.Header(
                        key,
                        tableId,
                        body.schema().version(),
                        sequenceNumber,
                        commitTimestamp,
                        kind,
                        visible,
                        immutable,
                        immutableSource));
    }

    public static Record ofSchema(
            Schema schema,
            Map<String, Object> fields,
            Record.Kind kind,
            ValueCodec codec,
            long tableId,
            long sequenceNumber,
            long commitTimestamp) {
        List<String> pkFields = schema.primaryKeys();
        if (!pkFields.isEmpty() && codec == null) {
            throw new IllegalArgumentException(
                    "ValueCodec must not be null when schema has primary keys: " + pkFields);
        }

        Object[] values = new Object[schema.fields().size()];
        for (int i = 0; i < schema.fields().size(); i++) {
            StructField field = schema.fields().get(i);
            Object value = fields.get(field.name());
            if (value != null && !isTypeCompatible(field.dataType(), value)) {
                throw new IllegalArgumentException(
                        "Type mismatch for field '"
                                + field.name()
                                + "': expected "
                                + field.dataType().name()
                                + ", got "
                                + value.getClass().getSimpleName());
            }
            values[i] = value;
        }

        Record.Key key;
        if (pkFields.isEmpty()) {
            byte[] syntheticKey = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
            key = new EncodedKey(syntheticKey);
        } else {
            Map<String, Object> pkValues = new LinkedHashMap<>();
            for (String pk : pkFields) {
                pkValues.put(pk, fields.get(pk));
            }
            byte[] encodedKey = codec.encodePrimaryKey(schema, pkValues);
            key = new EncodedKey(encodedKey);
        }

        Record.Body body = new DefaultBody(schema, values);
        return create(key, body, tableId, sequenceNumber, commitTimestamp, kind, true);
    }

    public static Record compactDelete(
            Record.Key key,
            Schema schema,
            long tableId,
            long sequenceNumber,
            long commitTimestamp) {
        Record.Body body = new DefaultBody(schema, new Object[0]);
        return create(
                key, body, tableId, sequenceNumber, commitTimestamp, Record.Kind.DELETE, true);
    }

    public static Record projectCopy(Record record, List<String> projections) {
        Schema fullSchema = record.body().schema();
        if (projections == null || projections.isEmpty()) {
            Object[] values = new Object[fullSchema.fields().size()];
            for (int i = 0; i < fullSchema.fields().size(); i++) {
                Record.Value v = record.body().valueAt(i);
                values[i] = v.isNull() ? null : v.raw();
            }
            Record.Body copiedBody = new DefaultBody(fullSchema, values);
            return new DefaultRecord(record.key(), copiedBody, record.header());
        }
        Schema projectedSchema = fullSchema.project(projections);
        Object[] projectedValues = new Object[projectedSchema.fields().size()];
        for (int i = 0; i < projectedSchema.fields().size(); i++) {
            String fieldName = projectedSchema.fields().get(i).name();
            Record.Value v = record.body().value(fieldName);
            projectedValues[i] = v.isNull() ? null : v.raw();
        }
        Record.Body projectedBody = new DefaultBody(projectedSchema, projectedValues);
        return new DefaultRecord(record.key(), projectedBody, record.header());
    }

    public static void evictAll(Collection<Record> records) {
        if (records == null) return;
        for (Record r : records) {
            if (r != null && r.body() != null) {
                r.body().evict();
            }
        }
    }

    private static boolean isTypeCompatible(RawWiseType type, Object value) {
        return switch (type.name()) {
            case "int8" -> value instanceof Byte;
            case "int32" -> value instanceof Integer;
            case "int64" -> value instanceof Long || value instanceof Integer;
            case "float32" -> value instanceof Float;
            case "float64" -> value instanceof Double || value instanceof Float;
            case "string" -> value instanceof String;
            case "boolean" -> value instanceof Boolean;
            case "binary" -> value instanceof byte[];
            default -> {
                if (type.name().startsWith("decimal")) yield value instanceof java.math.BigDecimal;
                if (type.name().equals("date")) yield value instanceof java.time.LocalDate;
                if (type.name().equals("timestamp")) yield value instanceof java.time.Instant;
                yield false;
            }
        };
    }
}
