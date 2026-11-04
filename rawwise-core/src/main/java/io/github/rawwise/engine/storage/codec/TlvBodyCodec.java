package io.github.rawwise.engine.storage.codec;

import io.github.rawwise.model.record.ArrayType;
import io.github.rawwise.model.record.ColumnTypeTag;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.SchemaCompatibility;
import io.github.rawwise.model.record.StructField;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class TlvBodyCodec implements BodyCodec {

    static final byte MAGIC_V2 = (byte) 0xF1;
    private static final int TYPE_SIZE = 1;
    private static final int LENGTH_SIZE = 4;
    private static final int HEADER_SIZE = TYPE_SIZE + LENGTH_SIZE;
    private static final ByteOrder BYTE_ORDER = ByteOrder.BIG_ENDIAN;

    @Override
    public String name() {
        return "tlv";
    }

    @Override
    public byte[] encode(Schema schema, Map<String, Object> fields) {
        return encodeV2(schema, fields);
    }

    @Override
    public Map<String, Object> decode(Schema schema, byte[] data) {
        if (data.length > 0 && data[0] == MAGIC_V2) {
            return decodeV2(schema, data);
        }
        return decodeV1(schema, data);
    }

    @Override
    public Object decodeField(Schema schema, byte[] data, String fieldName) {
        if (data.length > 0 && data[0] == MAGIC_V2) {
            return decodeFieldV2(schema, data, fieldName);
        }
        return decodeFieldV1(schema, data, fieldName);
    }

    byte[] encodeV2(Schema schema, Map<String, Object> fields) {
        int columnCount = schema.fields().size();
        int totalSize = 1 + 8 + varintSize(columnCount);
        byte[][] encodedEntries = new byte[columnCount][];

        for (int i = 0; i < columnCount; i++) {
            StructField sf = schema.fields().get(i);
            int fieldId = sf.hasAssignedFieldId() ? sf.fieldId() : (i + 1);
            Object value = fields.get(sf.name());

            if (value == null) {
                int entrySize = varintSize(fieldId) + TYPE_SIZE + LENGTH_SIZE;
                encodedEntries[i] = new byte[entrySize];
                ByteBuffer eb = ByteBuffer.wrap(encodedEntries[i]).order(BYTE_ORDER);
                writeVarint(eb, fieldId);
                eb.put(ColumnTypeTag.NULL);
                eb.putInt(0);
            } else {
                if (sf.dataType() instanceof ArrayType at && at.fixedSize() > 0) {
                    if (value instanceof java.util.List<?> list && list.size() != at.fixedSize()) {
                        throw new IllegalArgumentException(
                                "Field '"
                                        + sf.name()
                                        + "' requires exactly "
                                        + at.fixedSize()
                                        + " elements but got "
                                        + list.size());
                    } else if (value.getClass().isArray()) {
                        int arrayLen = java.lang.reflect.Array.getLength(value);
                        if (arrayLen != at.fixedSize()) {
                            throw new IllegalArgumentException(
                                    "Field '"
                                            + sf.name()
                                            + "' requires exactly "
                                            + at.fixedSize()
                                            + " elements but got "
                                            + arrayLen);
                        }
                    }
                }
                byte tag = ColumnTypeTag.fromRawWiseType(sf.dataType());
                byte[] valBytes = encodeValue(value, tag);
                int entrySize = varintSize(fieldId) + TYPE_SIZE + LENGTH_SIZE + valBytes.length;
                encodedEntries[i] = new byte[entrySize];
                ByteBuffer eb = ByteBuffer.wrap(encodedEntries[i]).order(BYTE_ORDER);
                writeVarint(eb, fieldId);
                eb.put(tag);
                eb.putInt(valBytes.length);
                if (valBytes.length > 0) {
                    eb.put(valBytes);
                }
            }
            totalSize += encodedEntries[i].length;
        }

        ByteBuffer buf = ByteBuffer.allocate(totalSize).order(BYTE_ORDER);
        buf.put(MAGIC_V2);
        buf.putLong(schema.version());
        writeVarint(buf, columnCount);
        for (byte[] entry : encodedEntries) {
            buf.put(entry);
        }
        return buf.array();
    }

    Map<String, Object> decodeV2(Schema schema, byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(BYTE_ORDER);
        buf.get();
        long storedVersion = buf.getLong();
        if (storedVersion > schema.version()) {
            throw new IllegalStateException(
                    "TLV V2 data version "
                            + storedVersion
                            + " is newer than schema version "
                            + schema.version()
                            + "; cannot read forward-incompatible data");
        }
        int columnCount = readVarint(buf);

        Map<Integer, Object> fieldIdToValue = new HashMap<>();
        for (int i = 0; i < columnCount; i++) {
            if (!buf.hasRemaining()) break;
            int fieldId = readVarint(buf);
            byte typeId = buf.get();
            int length = buf.getInt();
            Object value = null;
            if (typeId != ColumnTypeTag.NULL && length > 0) {
                byte[] fieldData = new byte[length];
                buf.get(fieldData);
                value = decodeValue(fieldData, typeId);
            }
            fieldIdToValue.put(fieldId, value);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        List<StructField> fields = schema.fields();
        for (int i = 0; i < fields.size(); i++) {
            StructField sf = fields.get(i);
            int fieldId = sf.hasAssignedFieldId() ? sf.fieldId() : (i + 1);
            if (fieldId > 0 && fieldIdToValue.containsKey(fieldId)) {
                result.put(sf.name(), fieldIdToValue.get(fieldId));
            } else {
                result.put(
                        sf.name(),
                        sf.defaultValue()
                                .map(v -> SchemaCompatibility.coerceDefault(v, sf.dataType()))
                                .orElse(null));
            }
        }
        return result;
    }

    Object decodeFieldV2(Schema schema, byte[] data, String fieldName) {
        StructField found = null;
        for (StructField sf : schema.fields()) {
            if (sf.name().equals(fieldName)) {
                found = sf;
                break;
            }
        }
        if (found == null) return null;
        StructField target = found;

        int targetFieldId = target.hasAssignedFieldId() ? target.fieldId() : 0;

        ByteBuffer buf = ByteBuffer.wrap(data).order(BYTE_ORDER);
        buf.get();
        long storedVersion = buf.getLong();
        if (storedVersion > schema.version()) {
            throw new IllegalStateException(
                    "TLV V2 data version "
                            + storedVersion
                            + " is newer than schema version "
                            + schema.version()
                            + "; cannot read forward-incompatible data");
        }
        int columnCount = readVarint(buf);

        for (int i = 0; i < columnCount; i++) {
            if (!buf.hasRemaining()) break;
            int fieldId = readVarint(buf);
            byte typeId = buf.get();
            int length = buf.getInt();

            if (fieldId == targetFieldId && targetFieldId > 0) {
                if (typeId == ColumnTypeTag.NULL || length == 0) return null;
                byte[] fieldData = new byte[length];
                buf.get(fieldData);
                return decodeValue(fieldData, typeId);
            }
            if (length > 0) {
                buf.position(buf.position() + length);
            }
        }

        var dv = target.defaultValue();
        return dv.isPresent()
                ? SchemaCompatibility.coerceDefault(dv.get(), target.dataType())
                : null;
    }

    Map<String, Object> decodeV1(Schema schema, byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(BYTE_ORDER);
        Map<String, Object> result = new LinkedHashMap<>();

        for (StructField sf : schema.fields()) {
            if (!buf.hasRemaining()) break;
            byte typeId = buf.get();
            int length = buf.getInt();
            Object value = null;
            if (typeId != ColumnTypeTag.NULL && length > 0) {
                byte[] fieldData = new byte[length];
                buf.get(fieldData);
                value = decodeValue(fieldData, typeId);
            }
            result.put(sf.name(), value);
        }
        for (StructField sf : schema.fields()) {
            if (!result.containsKey(sf.name())) {
                result.put(
                        sf.name(),
                        sf.defaultValue()
                                .map(v -> SchemaCompatibility.coerceDefault(v, sf.dataType()))
                                .orElse(null));
            }
        }
        return result;
    }

    Object decodeFieldV1(Schema schema, byte[] data, String fieldName) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(BYTE_ORDER);

        for (StructField sf : schema.fields()) {
            if (!buf.hasRemaining()) break;

            byte typeId = buf.get();
            int length = buf.getInt();

            if (sf.name().equals(fieldName)) {
                if (typeId == ColumnTypeTag.NULL || length == 0) return null;
                byte[] fieldData = new byte[length];
                buf.get(fieldData);
                return decodeValue(fieldData, typeId);
            }

            if (length > 0) {
                buf.position(buf.position() + length);
            }
        }

        for (StructField sf : schema.fields()) {
            if (sf.name().equals(fieldName)) {
                return sf.defaultValue()
                        .map(v -> SchemaCompatibility.coerceDefault(v, sf.dataType()))
                        .orElse(null);
            }
        }
        return null;
    }

    static int readVarint(ByteBuffer buf) {
        int result = 0;
        int shift = 0;
        while (buf.hasRemaining()) {
            byte b = buf.get();
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        return result;
    }

    static void writeVarint(ByteBuffer buf, int value) {
        while (true) {
            byte b = (byte) (value & 0x7F);
            value >>>= 7;
            if (value != 0) b |= 0x80;
            buf.put(b);
            if (value == 0) break;
        }
    }

    static int varintSize(int value) {
        int size = 0;
        do {
            size++;
            value >>>= 7;
        } while (value != 0);
        return size;
    }

    private byte[] encodeValue(Object value, byte typeTag) {
        return switch (typeTag) {
            case ColumnTypeTag.BYTE -> {
                if (value instanceof Boolean b) {
                    yield new byte[] {(byte) (b ? 1 : 0)};
                }
                yield new byte[] {((Number) value).byteValue()};
            }
            case ColumnTypeTag.INT -> {
                ByteBuffer b = ByteBuffer.allocate(4).order(BYTE_ORDER);
                yield b.putInt(((Number) value).intValue()).array();
            }
            case ColumnTypeTag.LONG -> {
                ByteBuffer b = ByteBuffer.allocate(8).order(BYTE_ORDER);
                yield b.putLong(((Number) value).longValue()).array();
            }
            case ColumnTypeTag.FLOAT -> {
                ByteBuffer b = ByteBuffer.allocate(4).order(BYTE_ORDER);
                yield b.putFloat(((Number) value).floatValue()).array();
            }
            case ColumnTypeTag.DOUBLE -> {
                ByteBuffer b = ByteBuffer.allocate(8).order(BYTE_ORDER);
                yield b.putDouble(((Number) value).doubleValue()).array();
            }
            case ColumnTypeTag.STRING -> ((String) value).getBytes(StandardCharsets.UTF_8);
            case ColumnTypeTag.BYTES -> (byte[]) value;
            default -> throw new IllegalArgumentException("Unsupported TLV type tag: " + typeTag);
        };
    }

    private Object decodeValue(byte[] data, byte typeTag) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(BYTE_ORDER);
        return switch (typeTag) {
            case ColumnTypeTag.BYTE -> buf.get();
            case ColumnTypeTag.INT -> buf.getInt();
            case ColumnTypeTag.LONG -> buf.getLong();
            case ColumnTypeTag.FLOAT -> buf.getFloat();
            case ColumnTypeTag.DOUBLE -> buf.getDouble();
            case ColumnTypeTag.STRING -> new String(data, StandardCharsets.UTF_8);
            case ColumnTypeTag.BYTES -> data.clone();
            default -> throw new IllegalArgumentException("Unsupported TLV type tag: " + typeTag);
        };
    }
}
