package io.github.rawwise.engine.storage.codec;

import io.github.rawwise.engine.storage.table.key.KeyCodec;
import io.github.rawwise.model.record.*;
import io.github.rawwise.model.record.DefaultValue;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.TypeId;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

public class DefaultValueCodec implements ValueCodec {
    private final KeyCodec keyCodec;

    public DefaultValueCodec(KeyCodec keyCodec) {
        this.keyCodec = keyCodec;
    }

    @Override
    public byte[] encodeNull() {
        return new byte[] {TAG_NULL};
    }

    @Override
    public byte[] encodeTombstone(long commitTimestamp) {
        ByteBuffer buf = ByteBuffer.allocate(9);
        buf.put(TAG_TOMBSTONE);
        buf.putLong(commitTimestamp);
        return buf.array();
    }

    @Override
    public byte[] encodeValue(byte[] payload) {
        byte[] out = new byte[payload.length + 1];
        out[0] = TAG_VALUE;
        System.arraycopy(payload, 0, out, 1, payload.length);
        return out;
    }

    @Override
    public byte extractTag(byte[] stored) {
        return stored != null && stored.length > 0 ? stored[0] : TAG_NULL;
    }

    @Override
    public byte[] extractPayload(byte[] stored) {
        if (stored == null || stored.length <= 1) return new byte[0];
        return Arrays.copyOfRange(stored, 1, stored.length);
    }

    @Override
    public boolean isNull(byte[] stored) {
        return stored != null && stored.length == 1 && stored[0] == TAG_NULL;
    }

    @Override
    public boolean isTombstone(byte[] stored) {
        return stored != null && stored.length >= 9 && stored[0] == TAG_TOMBSTONE;
    }

    @Override
    public long decodeTombstoneTimestamp(byte[] stored) {
        if (!isTombstone(stored))
            throw new IllegalArgumentException("Not a valid tombstone encoding");
        return ByteBuffer.wrap(stored, 1, 8).getLong();
    }

    @Override
    public byte[] encodeFields(Schema schema, Map<String, Object> fields) {
        List<StructField> schemaFields = schema.fields();
        Object[] values = new Object[schemaFields.size()];
        for (int i = 0; i < schemaFields.size(); i++) {
            values[i] = fields.get(schemaFields.get(i).name());
        }
        return encodeFieldsArray(schema, values);
    }

    @Override
    public byte[] encodeFieldsArray(Schema schema, Object[] values) {
        List<StructField> schemaFields = schema.fields();
        int fieldCount = Math.min(schemaFields.size(), values.length);
        if (fieldCount > 32767) {
            throw new IllegalArgumentException(
                    "Schema field count " + fieldCount + " exceeds short max (32767)");
        }

        int nullBitmapSize = (fieldCount + 7) / 8;
        byte[] nullBitmap = new byte[nullBitmapSize];

        ByteArrayOutputStream payloads = new ByteArrayOutputStream();
        for (int i = 0; i < fieldCount; i++) {
            Object value = values[i];
            if (value == null) {
                nullBitmap[i / 8] |= (1 << (i % 8));
            } else {
                byte typeId = typeIdFor(value);
                byte[] encoded = encodeValueByType(typeId, value);
                ByteBuffer lenBuf = ByteBuffer.allocate(5);
                lenBuf.put(typeId);
                lenBuf.putInt(encoded.length);
                try {
                    payloads.write(lenBuf.array());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                try {
                    payloads.write(encoded);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        ByteBuffer header = ByteBuffer.allocate(2 + nullBitmapSize);
        header.putShort((short) fieldCount);
        header.put(nullBitmap);

        byte[] headerBytes = header.array();
        byte[] payloadBytes = payloads.toByteArray();
        byte[] result = new byte[headerBytes.length + payloadBytes.length];
        System.arraycopy(headerBytes, 0, result, 0, headerBytes.length);
        System.arraycopy(payloadBytes, 0, result, headerBytes.length, payloadBytes.length);
        return result;
    }

    @Override
    public Map<String, Record.Value> decodeFields(Schema schema, byte[] payload) {
        Object[] values = decodeFieldsToArray(schema, payload);
        Map<String, Record.Value> result = new LinkedHashMap<>();
        List<StructField> fields = schema.fields();
        for (int i = 0; i < fields.size() && i < values.length; i++) {
            result.put(fields.get(i).name(), new DefaultValue(fields.get(i).dataType(), values[i]));
        }
        for (int i = values.length; i < fields.size(); i++) {
            result.put(fields.get(i).name(), new DefaultValue(fields.get(i).dataType(), null));
        }
        return result;
    }

    @Override
    public Object[] decodeFieldsToArray(Schema schema, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        short fieldCount = buf.getShort();
        List<StructField> schemaFields = schema.fields();

        int nullBitmapSize = (fieldCount + 7) / 8;
        byte[] nullBitmap = new byte[nullBitmapSize];
        buf.get(nullBitmap);

        Object[] result = new Object[schemaFields.size()];
        for (int i = 0; i < fieldCount && i < schemaFields.size(); i++) {
            boolean isNull = (nullBitmap[i / 8] & (1 << (i % 8))) != 0;
            if (isNull) {
                result[i] = null;
            } else {
                byte typeId = buf.get();
                int len = buf.getInt();
                if (len < 0 || len > buf.remaining()) {
                    throw new IllegalArgumentException(
                            "Invalid field length: " + len + ", remaining: " + buf.remaining());
                }
                byte[] data = new byte[len];
                buf.get(data);
                result[i] = decodeValueByType(typeId, data, schemaFields.get(i).dataType());
            }
        }
        for (int i = fieldCount; i < schemaFields.size(); i++) {
            result[i] = null;
        }
        return result;
    }

    @Override
    public byte[] encodePrimaryKey(Schema schema, Map<String, Object> primaryKeyValues) {
        if (keyCodec == null) throw new IllegalStateException("KeyCodec not configured");
        Map<String, Comparable> converted = new LinkedHashMap<>();
        for (var entry : primaryKeyValues.entrySet()) {
            Object v = entry.getValue();
            if (!(v instanceof Comparable)) {
                throw new IllegalArgumentException(
                        "Primary key value for '" + entry.getKey() + "' is not Comparable");
            }
            converted.put(entry.getKey(), (Comparable) v);
        }
        try {
            return keyCodec.encodePrimaryKey(converted);
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode primary key", e);
        }
    }

    private byte typeIdFor(Object value) {
        if (value == null) return TypeId.TYPE_NULL;
        if (value instanceof byte[]) return TypeId.TYPE_BINARY;
        if (value instanceof String) return TypeId.TYPE_STRING;
        if (value instanceof Integer) return TypeId.TYPE_INT;
        if (value instanceof Long) return TypeId.TYPE_LONG;
        if (value instanceof Float) return TypeId.TYPE_FLOAT;
        if (value instanceof Double) return TypeId.TYPE_DOUBLE;
        if (value instanceof Boolean) return TypeId.TYPE_BOOL;
        if (value instanceof BigDecimal) return TypeId.TYPE_DECIMAL;
        if (value instanceof LocalDate) return TypeId.TYPE_DATE;
        if (value instanceof Instant) return TypeId.TYPE_TIMESTAMP;
        return TypeId.TYPE_FALLBACK;
    }

    private byte[] encodeValueByType(byte typeId, Object value) {
        return switch (typeId) {
            case TypeId.TYPE_BINARY -> {
                if (!(value instanceof byte[] b)) throw new ClassCastException("Expected byte[]");
                yield b;
            }
            case TypeId.TYPE_STRING -> {
                if (!(value instanceof String s)) throw new ClassCastException("Expected String");
                yield s.getBytes(StandardCharsets.UTF_8);
            }
            case TypeId.TYPE_INT -> {
                if (!(value instanceof Integer i)) throw new ClassCastException("Expected Integer");
                yield ByteBuffer.allocate(4).putInt(i).array();
            }
            case TypeId.TYPE_LONG -> {
                if (!(value instanceof Long l)) throw new ClassCastException("Expected Long");
                yield ByteBuffer.allocate(8).putLong(l).array();
            }
            case TypeId.TYPE_FLOAT -> {
                if (!(value instanceof Float f)) throw new ClassCastException("Expected Float");
                yield ByteBuffer.allocate(4).putFloat(f).array();
            }
            case TypeId.TYPE_DOUBLE -> {
                if (!(value instanceof Double d)) throw new ClassCastException("Expected Double");
                yield ByteBuffer.allocate(8).putDouble(d).array();
            }
            case TypeId.TYPE_BOOL -> {
                if (!(value instanceof Boolean b)) throw new ClassCastException("Expected Boolean");
                yield new byte[] {b ? (byte) 1 : (byte) 0};
            }
            case TypeId.TYPE_DECIMAL -> {
                if (!(value instanceof BigDecimal bd))
                    throw new ClassCastException("Expected BigDecimal");
                yield bd.toPlainString().getBytes(StandardCharsets.UTF_8);
            }
            case TypeId.TYPE_DATE -> {
                if (!(value instanceof LocalDate ld))
                    throw new ClassCastException("Expected LocalDate");
                yield ld.toString().getBytes(StandardCharsets.UTF_8);
            }
            case TypeId.TYPE_TIMESTAMP -> {
                if (!(value instanceof Instant inst))
                    throw new ClassCastException("Expected Instant");
                yield inst.toString().getBytes(StandardCharsets.UTF_8);
            }
            default -> value.toString().getBytes(StandardCharsets.UTF_8);
        };
    }

    private Object decodeValueByType(byte typeId, byte[] data, RawWiseType schemaType) {
        return switch (typeId) {
            case TypeId.TYPE_NULL -> null;
            case TypeId.TYPE_BINARY -> data;
            case TypeId.TYPE_STRING -> new String(data, StandardCharsets.UTF_8);
            case TypeId.TYPE_INT -> ByteBuffer.wrap(data).getInt();
            case TypeId.TYPE_LONG -> ByteBuffer.wrap(data).getLong();
            case TypeId.TYPE_FLOAT -> ByteBuffer.wrap(data).getFloat();
            case TypeId.TYPE_DOUBLE -> ByteBuffer.wrap(data).getDouble();
            case TypeId.TYPE_BOOL -> data[0] != 0;
            case TypeId.TYPE_DECIMAL -> new BigDecimal(new String(data, StandardCharsets.UTF_8));
            case TypeId.TYPE_DATE -> LocalDate.parse(new String(data, StandardCharsets.UTF_8));
            case TypeId.TYPE_TIMESTAMP -> Instant.parse(new String(data, StandardCharsets.UTF_8));
            default -> new String(data, StandardCharsets.UTF_8);
        };
    }
}
