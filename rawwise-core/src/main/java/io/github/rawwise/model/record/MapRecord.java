package io.github.rawwise.model.record;

import java.util.*;

public final class MapRecord implements Record {
    private final Map<String, Object> data;
    private final Schema schema;
    private final Record.Key key;
    private final Record.Header header;

    private MapRecord(Map<String, ?> data, Schema schema, Record.Key key, Record.Header header) {
        this.data = new LinkedHashMap<>(data);
        this.schema = schema;
        this.key = key;
        this.header = header;
    }

    public static Record fromMap(Map<String, ?> data) {
        var schemaBuilder = Schema.builder();
        for (var entry : data.entrySet()) {
            RawWiseType type = inferType(entry.getValue());
            schemaBuilder.field(new StructField(entry.getKey(), type));
        }
        Schema schema = schemaBuilder.build();
        Record.Key key = Records.keyOf(new byte[0]);
        Record.Header hdr = Record.Header.of(key, 0, 0, 0, 0, Record.Kind.INSERT, true);
        return new MapRecord(data, schema, key, hdr);
    }

    @Override
    public Record.Key key() {
        return key;
    }

    @Override
    public Record.Header header() {
        return header;
    }

    @Override
    public Record.Body body() {
        return new MapBody(data, schema);
    }

    private static RawWiseType inferType(Object value) {
        if (value == null) return NullType.INSTANCE;
        if (value instanceof Boolean) return Types.bool();
        if (value instanceof Integer) return Types.int32();
        if (value instanceof Long) return Types.int64();
        if (value instanceof Float) return Types.float32();
        if (value instanceof Double) return Types.float64();
        if (value instanceof String) return Types.string();
        if (value instanceof byte[]) return Types.binary();
        return Types.string();
    }

    private record MapBody(Map<String, Object> data, Schema schema) implements Record.Body {
        @Override
        public Schema schema() {
            return schema;
        }

        @Override
        public Record.Value value(String fieldName) {
            Object v = data.get(fieldName);
            if (v == null && !data.containsKey(fieldName)) return DefaultValue.NULL;
            RawWiseType type = inferType(v);
            return new DefaultValue(type, v);
        }

        @Override
        public Record.Value value(Record.FieldId fieldId) {
            return value(fieldId.name());
        }

        @Override
        public boolean has(String fieldName) {
            return data.containsKey(fieldName) && data.get(fieldName) != null;
        }

        @Override
        public boolean has(Record.FieldId fieldId) {
            return has(fieldId.name());
        }

        @Override
        public Map<String, Object> toMap() {
            return new LinkedHashMap<>(data);
        }

        private static RawWiseType inferType(Object value) {
            if (value == null) return NullType.INSTANCE;
            if (value instanceof Boolean) return Types.bool();
            if (value instanceof Integer) return Types.int32();
            if (value instanceof Long) return Types.int64();
            if (value instanceof Float) return Types.float32();
            if (value instanceof Double) return Types.float64();
            if (value instanceof String) return Types.string();
            if (value instanceof byte[]) return Types.binary();
            return Types.string();
        }
    }
}
