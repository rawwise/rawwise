package io.github.rawwise.model.record;

import io.github.rawwise.engine.storage.codec.ValueCodec;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

public class DefaultBody implements Record.Body, Serializable {
    private static final long serialVersionUID = 1L;

    private final Schema schema;
    private final transient ValueCodec codec;
    private final Map<String, Integer> fieldNameToIndex;

    private byte[] encodedPayload;
    private Object[] rawValues;
    private transient DefaultValue[] valueCache;
    private volatile boolean evicted = false;

    public DefaultBody(Schema schema, byte[] encodedPayload, ValueCodec codec) {
        this.schema = schema;
        this.encodedPayload = encodedPayload;
        this.codec = codec;
        this.fieldNameToIndex = buildFieldNameIndex(schema);
    }

    public DefaultBody(Schema schema, Object[] resolvedValues) {
        this.schema = schema;
        this.rawValues = resolvedValues;
        this.codec = null;
        this.encodedPayload = null;
        this.valueCache = new DefaultValue[schema.fields().size()];
        this.fieldNameToIndex = buildFieldNameIndex(schema);
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public synchronized Record.Value value(String fieldName) {
        checkEvicted();
        int idx = fieldNameToIndex.getOrDefault(fieldName, -1);
        if (idx < 0) return DefaultValue.NULL;
        ensureDecoded();
        if (valueCache[idx] != null) return valueCache[idx];
        StructField field = schema.fields().get(idx);
        Object raw = (idx < rawValues.length) ? rawValues[idx] : null;
        DefaultValue dv = new DefaultValue(field.dataType(), raw);
        valueCache[idx] = dv;
        return dv;
    }

    @Override
    public synchronized Record.Value value(Record.FieldId fieldId) {
        return value(fieldId.name());
    }

    @Override
    public synchronized Record.Value valueAt(int index) {
        checkEvicted();
        if (index < 0 || index >= schema.fields().size()) return DefaultValue.NULL;
        ensureDecoded();
        if (valueCache[index] != null) return valueCache[index];
        StructField field = schema.fields().get(index);
        Object raw = (index < rawValues.length) ? rawValues[index] : null;
        DefaultValue dv = new DefaultValue(field.dataType(), raw);
        valueCache[index] = dv;
        return dv;
    }

    @Override
    public boolean has(String fieldName) {
        synchronized (this) {
            checkEvicted();
            int idx = fieldNameToIndex.getOrDefault(fieldName, -1);
            if (idx < 0) return false;
            ensureDecoded();
            return idx < rawValues.length && rawValues[idx] != null;
        }
    }

    @Override
    public boolean has(Record.FieldId fieldId) {
        return has(fieldId.name());
    }

    @Override
    public synchronized void evict() {
        evicted = true;
        encodedPayload = null;
        rawValues = null;
        valueCache = null;
    }

    @Override
    public synchronized Map<String, Object> toMap() {
        checkEvicted();
        ensureDecoded();
        Map<String, Object> map = new LinkedHashMap<>();
        List<StructField> fields = schema.fields();
        for (int i = 0; i < fields.size(); i++) {
            map.put(fields.get(i).name(), i < rawValues.length ? rawValues[i] : null);
        }
        return map;
    }

    public synchronized Object[] getRawValues() {
        checkEvicted();
        ensureDecoded();
        return rawValues.clone();
    }

    public synchronized boolean isPayloadReleased() {
        return encodedPayload == null && rawValues != null;
    }

    private void checkEvicted() {
        if (evicted) {
            throw new IllegalStateException(
                    "Body has been evicted, field access is no longer valid");
        }
    }

    private void ensureDecoded() {
        if (rawValues != null) {
            if (valueCache == null || valueCache.length != schema.fields().size()) {
                valueCache = new DefaultValue[schema.fields().size()];
            }
            return;
        }
        if (encodedPayload == null) {
            rawValues = new Object[0];
            valueCache = new DefaultValue[schema.fields().size()];
            return;
        }
        rawValues = codec.decodeFieldsToArray(schema, encodedPayload);
        valueCache = new DefaultValue[schema.fields().size()];
        encodedPayload = null;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        synchronized (this) {
            ensureDecoded();
            encodedPayload = null;
            out.defaultWriteObject();
        }
    }

    private static Map<String, Integer> buildFieldNameIndex(Schema schema) {
        Map<String, Integer> index = new HashMap<>();
        List<StructField> fields = schema.fields();
        for (int i = 0; i < fields.size(); i++) {
            index.put(fields.get(i).name(), i);
        }
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DefaultBody that)) return false;
        ensureDecoded();
        that.ensureDecoded();
        return Objects.equals(schema, that.schema) && Arrays.equals(rawValues, that.rawValues);
    }

    @Override
    public int hashCode() {
        ensureDecoded();
        return Objects.hash(schema, Arrays.hashCode(rawValues));
    }
}
