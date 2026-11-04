package io.github.rawwise.model.record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.CRC32;

public class TlvCodec implements ColumnGroupCodec {

    public static final TlvCodec INSTANCE = new TlvCodec();

    private static final int NULL_LENGTH = -1;
    private static final int SIZEOF_CRC32 = 4;
    private static final int MAX_VAR_FIELD_SIZE = 16 * 1024 * 1024;

    @Override
    public byte[] encode(ColumnGroup cg, Map<String, Object> values) {
        if (cg.encoding() != ColumnGroup.Encoding.TLV)
            throw new UnsupportedOperationException("TlvCodec only supports TLV encoding");

        int totalSize = 4;
        byte[][] encodedValues = new byte[cg.columnNames().size()][];
        for (int i = 0; i < cg.columnNames().size(); i++) {
            String col = cg.columnNames().get(i);
            Object val = values.get(col);
            totalSize += 2 + 1 + 4;
            if (val != null) {
                byte[] data = valueToBytes(val);
                if (data.length > MAX_VAR_FIELD_SIZE) {
                    throw new IllegalArgumentException(
                            "Variable field too large: "
                                    + data.length
                                    + " > "
                                    + MAX_VAR_FIELD_SIZE);
                }
                encodedValues[i] = data;
                totalSize += data.length;
            }
        }
        totalSize += SIZEOF_CRC32;

        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.putInt(cg.schemaVersion());
        for (int i = 0; i < cg.columnNames().size(); i++) {
            String col = cg.columnNames().get(i);
            Object val = values.get(col);
            buf.putShort((short) i);
            buf.put(ColumnTypeTag.of(val));
            if (val == null) {
                buf.putInt(NULL_LENGTH);
            } else {
                byte[] data = encodedValues[i];
                buf.putInt(data.length);
                buf.put(data);
            }
        }

        CRC32 crc32 = new CRC32();
        crc32.update(buf.array(), 0, buf.position());
        buf.putInt((int) crc32.getValue());

        return buf.array();
    }

    @Override
    public Map<String, Object> decode(ColumnGroup cg, byte[] data) {
        if (cg.encoding() != ColumnGroup.Encoding.TLV)
            throw new UnsupportedOperationException("TlvCodec only supports TLV encoding");

        validateCrc32(data);

        ByteBuffer buf = ByteBuffer.wrap(data, 0, data.length - SIZEOF_CRC32);

        int version = buf.getInt();
        validateVersion(version, cg.schemaVersion());

        Map<String, Object> result = new LinkedHashMap<>();

        while (buf.hasRemaining()) {
            int columnIndex = buf.getShort() & 0xFFFF;
            byte type = buf.get();
            int length = buf.getInt();
            if (columnIndex >= cg.columnNames().size()) {
                throw new IllegalStateException("Column index out of bounds: " + columnIndex);
            }
            String colName = cg.columnNames().get(columnIndex);
            if (length == NULL_LENGTH) {
                result.put(colName, null);
            } else {
                if (length > MAX_VAR_FIELD_SIZE) {
                    throw new IllegalStateException(
                            "Variable field length "
                                    + length
                                    + " exceeds maximum "
                                    + MAX_VAR_FIELD_SIZE);
                }
                byte[] valBytes = new byte[length];
                buf.get(valBytes);
                result.put(colName, bytesToValue(valBytes, type, cg.columnType(columnIndex)));
            }
        }

        return result;
    }

    @Override
    public Object decodeField(ColumnGroup cg, byte[] data, String columnName) {
        if (cg.encoding() != ColumnGroup.Encoding.TLV)
            throw new UnsupportedOperationException("TlvCodec only supports TLV encoding");

        int colIndex = cg.columnNames().indexOf(columnName);
        if (colIndex < 0)
            throw new IllegalArgumentException("Column not found in ColumnGroup: " + columnName);

        validateCrc32(data);

        ByteBuffer buf = ByteBuffer.wrap(data, 0, data.length - SIZEOF_CRC32);

        int version = buf.getInt();
        validateVersion(version, cg.schemaVersion());

        while (buf.hasRemaining()) {
            int entryColIndex = buf.getShort() & 0xFFFF;
            byte type = buf.get();
            int length = buf.getInt();
            if (entryColIndex == colIndex) {
                if (length == NULL_LENGTH) return null;
                if (length > MAX_VAR_FIELD_SIZE) {
                    throw new IllegalStateException(
                            "Variable field length "
                                    + length
                                    + " exceeds maximum "
                                    + MAX_VAR_FIELD_SIZE);
                }
                byte[] valBytes = new byte[length];
                buf.get(valBytes);
                return bytesToValue(valBytes, type, cg.columnType(colIndex));
            }
            if (length > 0 && length <= MAX_VAR_FIELD_SIZE) {
                buf.position(buf.position() + length);
            } else if (length > MAX_VAR_FIELD_SIZE) {
                throw new IllegalStateException(
                        "Variable field length "
                                + length
                                + " exceeds maximum "
                                + MAX_VAR_FIELD_SIZE);
            }
        }

        return null;
    }

    public int extractInt(byte[] data, int columnIndex) {
        validateCrc32(data);
        ByteBuffer buf = ByteBuffer.wrap(data, 0, data.length - SIZEOF_CRC32);
        buf.getInt();
        while (buf.hasRemaining()) {
            int idx = buf.getShort() & 0xFFFF;
            byte type = buf.get();
            int length = buf.getInt();
            if (idx == columnIndex) {
                if (length == NULL_LENGTH) {
                    throw new NullPointerException("Column " + columnIndex + " is null");
                }
                if (type != ColumnTypeTag.INT) {
                    throw new IllegalStateException(
                            "Column " + columnIndex + " is not an int (type=" + type + ")");
                }
                return buf.getInt();
            }
            if (length > 0 && length <= MAX_VAR_FIELD_SIZE) {
                buf.position(buf.position() + length);
            }
        }
        throw new IllegalArgumentException(
                "Column index " + columnIndex + " not found in TLV data");
    }

    private void validateCrc32(byte[] data) {
        if (data.length < SIZEOF_CRC32 + 4) {
            throw new IllegalStateException("Data too short for CRC32 validation");
        }
        CRC32 crc32 = new CRC32();
        crc32.update(data, 0, data.length - SIZEOF_CRC32);
        int expectedChecksum =
                ByteBuffer.wrap(data, data.length - SIZEOF_CRC32, SIZEOF_CRC32).getInt();
        if ((int) crc32.getValue() != expectedChecksum) {
            throw new IllegalStateException("CRC32 checksum mismatch: data may be corrupted");
        }
    }

    private void validateVersion(int dataVersion, int schemaVersion) {
        if (dataVersion > schemaVersion) {
            throw new IllegalStateException(
                    "Data version "
                            + dataVersion
                            + " is newer than schema version "
                            + schemaVersion
                            + "; cannot read forward-incompatible data");
        }
    }

    private int valueSize(Object val) {
        if (val instanceof Integer) return 4;
        if (val instanceof Long) return 8;
        if (val instanceof Double) return 8;
        if (val instanceof Float) return 4;
        if (val instanceof String s) return s.getBytes(StandardCharsets.UTF_8).length;
        if (val instanceof byte[] b) return b.length;
        if (val instanceof Byte) return 1;
        if (val instanceof Boolean) return 1;
        throw new IllegalArgumentException("Unsupported type: " + val.getClass().getName());
    }

    private byte[] valueToBytes(Object val) {
        ByteBuffer tmp;
        if (val instanceof Integer i) {
            tmp = ByteBuffer.allocate(4);
            tmp.putInt(i);
        } else if (val instanceof Long l) {
            tmp = ByteBuffer.allocate(8);
            tmp.putLong(l);
        } else if (val instanceof Double d) {
            tmp = ByteBuffer.allocate(8);
            tmp.putDouble(d);
        } else if (val instanceof Float f) {
            tmp = ByteBuffer.allocate(4);
            tmp.putFloat(f);
        } else if (val instanceof String s) {
            return s.getBytes(StandardCharsets.UTF_8);
        } else if (val instanceof byte[] b) {
            return b;
        } else if (val instanceof Byte b) {
            tmp = ByteBuffer.allocate(1);
            tmp.put(b);
        } else if (val instanceof Boolean b) {
            tmp = ByteBuffer.allocate(1);
            tmp.put((byte) (b ? 1 : 0));
        } else {
            throw new IllegalArgumentException("Unsupported type: " + val.getClass().getName());
        }
        return tmp.array();
    }

    private Object bytesToValue(byte[] data, byte type) {
        return bytesToValue(data, type, null);
    }

    private Object bytesToValue(byte[] data, byte type, RawWiseType expectedType) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        return switch (type) {
            case ColumnTypeTag.BYTE -> {
                if (expectedType instanceof BooleanType) {
                    yield buf.get() != 0;
                }
                yield buf.get();
            }
            case ColumnTypeTag.INT -> {
                int v = buf.getInt();
                if (expectedType instanceof Int8Type) {
                    yield (byte) v;
                }
                yield v;
            }
            case ColumnTypeTag.LONG -> buf.getLong();
            case ColumnTypeTag.DOUBLE -> buf.getDouble();
            case ColumnTypeTag.FLOAT -> buf.getFloat();
            case ColumnTypeTag.STRING -> new String(data, StandardCharsets.UTF_8);
            case ColumnTypeTag.BYTES -> data;
            default -> throw new IllegalArgumentException("Unknown type: " + type);
        };
    }
}
