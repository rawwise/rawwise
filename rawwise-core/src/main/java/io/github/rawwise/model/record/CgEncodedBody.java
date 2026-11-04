package io.github.rawwise.model.record;

import io.github.rawwise.engine.storage.codec.BodyCodec;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link Record.Body} implementation backed by a pluggable {@link Body Codec}.
 *
 * <p>Unlike {@link Default Body} which decodes ALL fields eagerly on first access, {@code Cg
 * Encoded Body} uses a lazy LRU decode cache that decodes individual fields on demand via {@link
 * Body Codec#decode Field}. This dramatically reduces decode overhead when only a subset of columns
 * are accessed (projection pushdown).
 *
 * <p>When the cache is full (default: 64 entries), the least-recently-used entry is evicted. A
 * subsequent access to the evicted field re-decodes it.
 *
 * <h3>Thread Safety</h3>
 *
 * <ul>
 *   <li>The {@code evicted} flag is volatile; once set, all accessors throw {@link Illegal State
 *       Exception}.
 *   <li>The LRU cache is synchronized via {@link Collections#synchronized Map}.
 *   <li>Full decode (triggered by {@code to Map()} or {@code value At()}) uses double-checked
 *       locking on {@code decoded Values}.
 *   <li>Serialization ensures all values are decoded before writing.
 * </ul>
 */
public class CgEncodedBody implements Record.Body, Serializable {
    private static final long serialVersionUID = 1L;
    private static final int MAX_CACHE_ENTRIES = 64;

    private final Schema schema;
    private final transient BodyCodec bodyCodec;
    private final Map<String, Integer> fieldNameToIndex;
    private final Map<Integer, Integer> fieldIdToIndex;

    private volatile byte[] encodedPayload;
    private volatile boolean evicted;
    private volatile Object[] decodedValues;

    private final Map<String, Object> decodeCache;

    /**
     * Construct a new {@code Cg Encoded Body}.
     *
     * @param schema the table schema
     * @param body Codec the codec used for lazy field decoding
     * @param payload encoded body bytes (not null)
     */
    public CgEncodedBody(Schema schema, BodyCodec bodyCodec, byte[] payload) {
        this.schema = schema;
        this.bodyCodec = bodyCodec;
        this.encodedPayload = payload;
        this.fieldNameToIndex = buildFieldNameIndex(schema);
        this.fieldIdToIndex = buildFieldIdIndex(schema);
        this.decodeCache =
                Collections.synchronizedMap(
                        new LinkedHashMap<>(16, 0.75f, true) {
                            @Override
                            protected boolean removeEldestEntry(Map.Entry<String, Object> eldest) {
                                return size() > MAX_CACHE_ENTRIES;
                            }
                        });
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Record.Value value(String fieldName) {
        checkEvicted();
        int idx = fieldNameToIndex.getOrDefault(fieldName, -1);
        if (idx < 0) {
            StructField sf = schema.field(fieldName).orElse(null);
            if (sf != null && sf.hasAssignedFieldId()) {
                idx = fieldIdToIndex.getOrDefault(sf.fieldId(), -1);
            }
        }
        if (idx < 0) return DefaultValue.NULL;
        Object[] dv = decodedValues; // single volatile read
        if (dv != null) {
            if (idx >= dv.length) return DefaultValue.NULL;
            return new DefaultValue(schema.fields().get(idx).dataType(), dv[idx]);
        }
        Object cached = decodeCache.get(fieldName);
        if (cached != null) {
            return new DefaultValue(schema.fields().get(idx).dataType(), cached);
        }
        byte[] payload = encodedPayload; // single volatile read, local capture
        if (payload == null) {
            dv = decodedValues;
            if (dv != null && idx < dv.length) {
                return new DefaultValue(schema.fields().get(idx).dataType(), dv[idx]);
            }
            return DefaultValue.NULL;
        }
        checkEvicted(); // re-check after volatile read
        Object raw = bodyCodec.decodeField(schema, payload, fieldName);
        if (raw != null) decodeCache.put(fieldName, raw);
        return new DefaultValue(schema.fields().get(idx).dataType(), raw);
    }

    @Override
    public Record.Value value(Record.FieldId fieldId) {
        return value(fieldId.name());
    }

    @Override
    public Record.Value valueAt(int index) {
        checkEvicted();
        if (index < 0 || index >= schema.fields().size()) return DefaultValue.NULL;
        ensureAllDecoded();
        Object[] dv = decodedValues; // local capture, single volatile read
        checkEvicted(); // re-check, aligned with value() pattern
        if (dv == null || index >= dv.length) return DefaultValue.NULL;
        return new DefaultValue(schema.fields().get(index).dataType(), dv[index]);
    }

    @Override
    public boolean has(String fieldName) {
        if (!fieldNameToIndex.containsKey(fieldName)) return false;
        Record.Value v = value(fieldName);
        return v != null && !v.isNull();
    }

    @Override
    public boolean has(Record.FieldId fieldId) {
        return has(fieldId.name());
    }

    @Override
    public synchronized void evict() {
        evicted = true;
        encodedPayload = null;
        decodedValues = null;
        decodeCache.clear();
    }

    @Override
    public synchronized Map<String, Object> toMap() {
        checkEvicted();
        ensureAllDecoded();
        Map<String, Object> map = new LinkedHashMap<>();
        List<StructField> fields = schema.fields();
        for (int i = 0; i < fields.size(); i++) {
            map.put(fields.get(i).name(), i < decodedValues.length ? decodedValues[i] : null);
        }
        return map;
    }

    /**
     * Returns a clone of the raw values array. Used by encoders that need direct access to all
     * values.
     */
    public synchronized Object[] getRawValues() {
        checkEvicted();
        ensureAllDecoded();
        return decodedValues.clone();
    }

    /** Returns whether the encoded payload has been released (decoded into array). */
    public synchronized boolean isPayloadReleased() {
        return encodedPayload == null && decodedValues != null;
    }

    private void checkEvicted() {
        if (evicted) {
            throw new IllegalStateException(
                    "Body has been evicted, field access is no longer valid");
        }
    }

    /**
     * Double-checked locking to decode all fields lazily. Called when all values are needed (to
     * Map, value At, get Raw Values).
     */
    private void ensureAllDecoded() {
        Object[] dv = decodedValues;
        if (dv != null) return;

        synchronized (this) {
            if (decodedValues != null) return;
            checkEvicted();
            if (encodedPayload == null) {
                decodedValues = new Object[0];
                return;
            }
            Map<String, Object> all = bodyCodec.decode(schema, encodedPayload);
            Object[] values = new Object[schema.fields().size()];
            List<StructField> fields = schema.fields();
            for (int i = 0; i < fields.size(); i++) {
                Object v = all.get(fields.get(i).name());
                values[i] = v;
                if (v != null) decodeCache.put(fields.get(i).name(), v);
            }
            decodedValues = values;
            encodedPayload = null;
        }
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        synchronized (this) {
            ensureAllDecoded();
            encodedPayload = null;
            out.defaultWriteObject();
        }
    }

    private static Map<String, Integer> buildFieldNameIndex(Schema schema) {
        Map<String, Integer> index = new LinkedHashMap<>();
        List<StructField> fields = schema.fields();
        for (int i = 0; i < fields.size(); i++) {
            index.put(fields.get(i).name(), i);
        }
        return index;
    }

    private static Map<Integer, Integer> buildFieldIdIndex(Schema schema) {
        Map<Integer, Integer> index = new LinkedHashMap<>();
        List<StructField> fields = schema.fields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).hasAssignedFieldId()) {
                index.put(fields.get(i).fieldId(), i);
            }
        }
        return index;
    }
}
