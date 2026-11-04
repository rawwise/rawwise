package io.github.rawwise.model.record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

public class IndexedOffsetCodec implements ColumnGroupCodec {

    public static final IndexedOffsetCodec INSTANCE = new IndexedOffsetCodec();

    private static final int SIZEOF_INT = 4;
    private static final int SIZEOF_LONG = 8;
    private static final int SIZEOF_FLOAT = 4;
    private static final int SIZEOF_DOUBLE = 8;
    private static final int SIZEOF_BYTE = 1;
    private static final int SIZEOF_CRC32 = 4;
    private static final int MAX_VAR_FIELD_SIZE = 16 * 1024 * 1024;

    @Override
    public byte[] encode(ColumnGroup cg, Map<String, Object> values) {
        if (cg.encoding() != ColumnGroup.Encoding.FIXED_OFFSET)
            throw new UnsupportedOperationException(
                    "IndexedOffsetCodec only supports FIXED_OFFSET encoding");
        if (cg.columnTypes() == null || cg.columnTypes().isEmpty())
            throw new IllegalArgumentException(
                    "FIXED_OFFSET encoding requires columnTypes in ColumnGroup");

        ColumnGroupSchemaResolver resolver = ColumnGroupSchemaResolver.resolve(cg);
        int columnCount = cg.columnNames().size();
        int bitmapBytes = resolver.bitmapBytes();
        int fieldOffsetTableSize = columnCount * SIZEOF_INT;
        int headerSize = SIZEOF_INT + SIZEOF_INT + bitmapBytes + fieldOffsetTableSize;

        List<Integer> fixedIndices = new ArrayList<>();
        List<Integer> varIndices = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            ColumnGroupSchemaResolver.FieldLayout layout =
                    resolver.fieldLayouts().get(cg.columnNames().get(i));
            if (layout.isVariable()) {
                varIndices.add(i);
            } else {
                fixedIndices.add(i);
            }
        }

        int fixedDataSize = 0;
        for (int idx : fixedIndices) {
            ColumnGroupSchemaResolver.FieldLayout layout =
                    resolver.fieldLayouts().get(cg.columnNames().get(idx));
            fixedDataSize += layout.size();
        }

        int varDataSize = 0;
        for (int idx : varIndices) {
            Object val = values.get(cg.columnNames().get(idx));
            if (val instanceof String s) {
                byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                varDataSize += SIZEOF_INT + bytes.length;
            } else if (val instanceof byte[] b) {
                varDataSize += SIZEOF_INT + b.length;
            }
        }

        int totalSize = headerSize + fixedDataSize + varDataSize + SIZEOF_CRC32;
        ByteBuffer buf = ByteBuffer.allocate(totalSize);

        buf.putInt(cg.schemaVersion());
        buf.putInt(columnCount);

        byte[] bitmap = new byte[bitmapBytes];
        for (int i = 0; i < columnCount; i++) {
            if (values.get(cg.columnNames().get(i)) == null) {
                bitmap[i / 8] |= (1 << (i % 8));
            }
        }
        buf.put(bitmap);

        int fieldOffsetTablePos = buf.position();
        buf.position(buf.position() + fieldOffsetTableSize);

        int[] fieldOffsets = new int[columnCount];

        for (int idx : fixedIndices) {
            Object val = values.get(cg.columnNames().get(idx));
            if (val == null) {
                fieldOffsets[idx] = 0;
                buf.position(
                        buf.position()
                                + resolver.fieldLayouts().get(cg.columnNames().get(idx)).size());
            } else {
                fieldOffsets[idx] = buf.position();
                ColumnGroupSchemaResolver.FieldLayout layout =
                        resolver.fieldLayouts().get(cg.columnNames().get(idx));
                writeFixed(buf, layout.fieldType(), val);
            }
        }

        for (int idx : varIndices) {
            Object val = values.get(cg.columnNames().get(idx));
            if (val == null) {
                fieldOffsets[idx] = 0;
            } else {
                fieldOffsets[idx] = buf.position();
                if (val instanceof String s) {
                    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                    if (bytes.length > MAX_VAR_FIELD_SIZE) {
                        throw new IllegalArgumentException(
                                "Variable field too large: "
                                        + bytes.length
                                        + " > "
                                        + MAX_VAR_FIELD_SIZE);
                    }
                    buf.putInt(bytes.length);
                    buf.put(bytes);
                } else if (val instanceof byte[] b) {
                    if (b.length > MAX_VAR_FIELD_SIZE) {
                        throw new IllegalArgumentException(
                                "Variable field too large: "
                                        + b.length
                                        + " > "
                                        + MAX_VAR_FIELD_SIZE);
                    }
                    buf.putInt(b.length);
                    buf.put(b);
                }
            }
        }

        int savedPos = buf.position();
        buf.position(fieldOffsetTablePos);
        for (int offset : fieldOffsets) {
            buf.putInt(offset);
        }
        buf.position(savedPos);

        CRC32 crc32 = new CRC32();
        crc32.update(buf.array(), 0, buf.position());
        buf.putInt((int) crc32.getValue());

        return buf.array();
    }

    @Override
    public Map<String, Object> decode(ColumnGroup cg, byte[] data) {
        if (cg.encoding() != ColumnGroup.Encoding.FIXED_OFFSET)
            throw new UnsupportedOperationException(
                    "IndexedOffsetCodec only supports FIXED_OFFSET encoding");
        if (cg.columnTypes() == null || cg.columnTypes().isEmpty())
            throw new IllegalArgumentException(
                    "FIXED_OFFSET encoding requires columnTypes in ColumnGroup");

        validateCrc32(data);

        ColumnGroupSchemaResolver resolver = ColumnGroupSchemaResolver.resolve(cg);
        int schemaColumnCount = cg.columnNames().size();
        ByteBuffer buf = ByteBuffer.wrap(data, 0, data.length - SIZEOF_CRC32);

        int dataVersion = buf.getInt();
        validateVersion(dataVersion, cg.schemaVersion());

        int dataColumnCount = buf.getInt();
        int dataBitmapBytes = (dataColumnCount + 7) / 8;
        byte[] bitmap = new byte[dataBitmapBytes];
        buf.get(bitmap);

        int fieldOffsetTableSize = dataColumnCount * SIZEOF_INT;
        int[] fieldOffsets = new int[dataColumnCount];
        for (int i = 0; i < dataColumnCount; i++) {
            fieldOffsets[i] = buf.getInt();
        }

        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < schemaColumnCount; i++) {
            String colName = cg.columnNames().get(i);
            ColumnGroupSchemaResolver.FieldLayout layout = resolver.fieldLayouts().get(colName);

            if (i >= dataColumnCount) {
                result.put(colName, null);
                continue;
            }

            boolean isNull = (bitmap[i / 8] & (1 << (i % 8))) != 0;
            if (isNull || fieldOffsets[i] == 0) {
                result.put(colName, null);
                continue;
            }

            int savedPos = buf.position();
            buf.position(fieldOffsets[i]);
            if (layout.isVariable()) {
                result.put(colName, readVariable(buf, layout.fieldType()));
            } else {
                result.put(colName, readFixed(buf, layout.fieldType()));
            }
            buf.position(savedPos);
        }

        return result;
    }

    @Override
    public Object decodeField(ColumnGroup cg, byte[] data, String columnName) {
        if (cg.encoding() != ColumnGroup.Encoding.FIXED_OFFSET)
            throw new UnsupportedOperationException(
                    "IndexedOffsetCodec only supports FIXED_OFFSET encoding");
        if (cg.columnTypes() == null || cg.columnTypes().isEmpty())
            throw new IllegalArgumentException(
                    "FIXED_OFFSET encoding requires columnTypes in ColumnGroup");

        int colIndex = cg.columnNames().indexOf(columnName);
        if (colIndex < 0)
            throw new IllegalArgumentException("Column not found in ColumnGroup: " + columnName);

        ColumnGroupSchemaResolver resolver = ColumnGroupSchemaResolver.resolve(cg);
        ColumnGroupSchemaResolver.FieldLayout layout = resolver.fieldLayouts().get(columnName);

        ByteBuffer buf = ByteBuffer.wrap(data, 0, data.length - SIZEOF_CRC32);

        int dataVersion = buf.getInt();
        validateVersion(dataVersion, cg.schemaVersion());

        int dataColumnCount = buf.getInt();

        if (colIndex >= dataColumnCount) {
            return null;
        }

        int dataBitmapBytes = (dataColumnCount + 7) / 8;
        byte[] bitmap = new byte[dataBitmapBytes];
        buf.get(bitmap);

        boolean isNull = (bitmap[colIndex / 8] & (1 << (colIndex % 8))) != 0;
        if (isNull) return null;

        int fieldOffsetTablePos = buf.position();
        buf.position(fieldOffsetTablePos + colIndex * SIZEOF_INT);
        int fieldOffset = buf.getInt();

        if (fieldOffset == 0) return null;

        buf.position(fieldOffset);
        if (layout.isVariable()) {
            return readVariable(buf, layout.fieldType());
        } else {
            return readFixed(buf, layout.fieldType());
        }
    }

    private void validateCrc32(byte[] data) {
        if (data.length < SIZEOF_CRC32 + 2 * SIZEOF_INT) {
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

    private void writeFixed(ByteBuffer buf, ColumnGroupSchemaResolver.FieldType ft, Object val) {
        switch (ft) {
            case BYTE -> buf.put(val == null ? 0 : ((Number) val).byteValue());
            case INT -> buf.putInt(val == null ? 0 : ((Number) val).intValue());
            case LONG -> buf.putLong(val == null ? 0L : ((Number) val).longValue());
            case FLOAT -> buf.putFloat(val == null ? 0.0f : ((Number) val).floatValue());
            case DOUBLE -> buf.putDouble(val == null ? 0.0 : ((Number) val).doubleValue());
            default -> throw new IllegalArgumentException("Unsupported fixed type: " + ft);
        }
    }

    private Object readFixed(ByteBuffer buf, ColumnGroupSchemaResolver.FieldType ft) {
        return switch (ft) {
            case BYTE -> buf.get();
            case INT -> buf.getInt();
            case LONG -> buf.getLong();
            case FLOAT -> buf.getFloat();
            case DOUBLE -> buf.getDouble();
            default -> throw new IllegalArgumentException("Unsupported fixed type: " + ft);
        };
    }

    private Object readVariable(ByteBuffer buf, ColumnGroupSchemaResolver.FieldType ft) {
        int length = buf.getInt();
        byte[] data = new byte[length];
        buf.get(data);
        return switch (ft) {
            case STRING -> new String(data, StandardCharsets.UTF_8);
            case BYTES -> data;
            default -> throw new IllegalArgumentException("Unsupported variable type: " + ft);
        };
    }
}
