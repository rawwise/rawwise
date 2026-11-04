package io.github.rawwise.model.record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

public class FixedOffsetCodec implements ColumnGroupCodec {

    public static final FixedOffsetCodec INSTANCE = new FixedOffsetCodec();

    private static final byte MAGIC_V2 = (byte) 0xF2;
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
                    "FixedOffsetCodec only supports FIXED_OFFSET encoding");
        if (cg.columnTypes() == null || cg.columnTypes().isEmpty())
            throw new IllegalArgumentException(
                    "FIXED_OFFSET encoding requires columnTypes in ColumnGroup");

        boolean hasFieldIds = !cg.columnFieldIds().isEmpty();
        return hasFieldIds ? encodeV2(cg, values) : encodeV1(cg, values);
    }

    @Override
    public Map<String, Object> decode(ColumnGroup cg, byte[] data) {
        if (cg.encoding() != ColumnGroup.Encoding.FIXED_OFFSET)
            throw new UnsupportedOperationException(
                    "FixedOffsetCodec only supports FIXED_OFFSET encoding");
        if (cg.columnTypes() == null || cg.columnTypes().isEmpty())
            throw new IllegalArgumentException(
                    "FIXED_OFFSET encoding requires columnTypes in ColumnGroup");

        if (data.length > 0 && data[0] == MAGIC_V2) {
            return decodeV2(cg, data);
        }
        return decodeV1(cg, data);
    }

    @Override
    public Object decodeField(ColumnGroup cg, byte[] data, String columnName) {
        if (cg.encoding() != ColumnGroup.Encoding.FIXED_OFFSET)
            throw new UnsupportedOperationException(
                    "FixedOffsetCodec only supports FIXED_OFFSET encoding");
        if (cg.columnTypes() == null || cg.columnTypes().isEmpty())
            throw new IllegalArgumentException(
                    "FIXED_OFFSET encoding requires columnTypes in ColumnGroup");

        if (data.length > 0 && data[0] == MAGIC_V2) {
            return decodeFieldV2(cg, data, columnName);
        }
        return decodeFieldV1(cg, data, columnName);
    }

    byte[] encodeV2(ColumnGroup cg, Map<String, Object> values) {
        ColumnGroupSchemaResolver resolver = ColumnGroupSchemaResolver.resolve(cg);
        int columnCount = cg.columnNames().size();
        int bitmapBytes = resolver.bitmapBytes();
        int varFieldCount = resolver.varFieldCount();
        int varOffsetTableSize = varFieldCount * SIZEOF_INT;

        int fieldIdMapSize = varintSize(columnCount);
        for (int i = 0; i < columnCount; i++) {
            fieldIdMapSize += varintSize(cg.columnFieldId(i));
        }

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

        int varDataSize = 0;
        for (int idx : varIndices) {
            Object val = values.get(cg.columnNames().get(idx));
            if (val == null) {
            } else if (val instanceof String s) {
                byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                varDataSize += SIZEOF_INT + bytes.length;
            } else if (val instanceof byte[] b) {
                varDataSize += SIZEOF_INT + b.length;
            }
        }

        int headerSize =
                1 + SIZEOF_INT + SIZEOF_INT + fieldIdMapSize + bitmapBytes + varOffsetTableSize;
        int fixedDataSize = resolver.fixedDataSize();
        int totalSize = headerSize + fixedDataSize + varDataSize + SIZEOF_CRC32;
        ByteBuffer buf = ByteBuffer.allocate(totalSize);

        buf.put(MAGIC_V2);
        buf.putInt(cg.schemaVersion());
        buf.putInt(columnCount);

        writeVarint(buf, columnCount);
        for (int i = 0; i < columnCount; i++) {
            writeVarint(buf, cg.columnFieldId(i));
        }

        byte[] bitmap = new byte[bitmapBytes];
        for (int i = 0; i < columnCount; i++) {
            if (values.get(cg.columnNames().get(i)) == null) {
                bitmap[i / 8] |= (1 << (i % 8));
            }
        }
        buf.put(bitmap);

        int varOffsetTablePos = buf.position();
        if (varFieldCount > 0) {
            buf.position(buf.position() + varOffsetTableSize);
        }

        for (int idx : fixedIndices) {
            Object val = values.get(cg.columnNames().get(idx));
            ColumnGroupSchemaResolver.FieldLayout layout =
                    resolver.fieldLayouts().get(cg.columnNames().get(idx));
            writeFixed(buf, layout.fieldType(), val);
        }

        int[] varOffsets = new int[varFieldCount];
        int varIdx = 0;
        for (int idx : varIndices) {
            Object val = values.get(cg.columnNames().get(idx));
            if (val == null) {
                varOffsets[varIdx] = 0;
            } else {
                varOffsets[varIdx] = buf.position();
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
            varIdx++;
        }

        if (varFieldCount > 0) {
            int savedPos = buf.position();
            buf.position(varOffsetTablePos);
            for (int offset : varOffsets) {
                buf.putInt(offset);
            }
            buf.position(savedPos);
        }

        CRC32 crc32 = new CRC32();
        crc32.update(buf.array(), 0, buf.position());
        buf.putInt((int) crc32.getValue());

        return buf.array();
    }

    Map<String, Object> decodeV2(ColumnGroup cg, byte[] data) {
        validateCrc32(data);

        ColumnGroupSchemaResolver resolver = ColumnGroupSchemaResolver.resolve(cg);
        int schemaColumnCount = cg.columnNames().size();
        ByteBuffer buf = ByteBuffer.wrap(data, 0, data.length - SIZEOF_CRC32);

        buf.get();
        int dataVersion = buf.getInt();
        validateVersion(dataVersion, cg.schemaVersion());

        int dataColumnCount = buf.getInt();

        int fieldIdCount = readVarint(buf);
        Map<Integer, Integer> fieldIdToDataIndex = new HashMap<>();
        for (int i = 0; i < fieldIdCount; i++) {
            int fieldId = readVarint(buf);
            fieldIdToDataIndex.put(fieldId, i);
        }

        int dataBitmapBytes = (dataColumnCount + 7) / 8;
        byte[] bitmap = new byte[dataBitmapBytes];
        buf.get(bitmap);

        boolean[] isNull = new boolean[dataColumnCount];
        for (int i = 0; i < dataColumnCount; i++) {
            isNull[i] = (bitmap[i / 8] & (1 << (i % 8))) != 0;
        }

        int dataVarFieldCount = 0;
        for (int i = 0; i < dataColumnCount; i++) {
            if (i < schemaColumnCount) {
                ColumnGroupSchemaResolver.FieldLayout layout =
                        resolver.fieldLayouts().get(cg.columnNames().get(i));
                if (layout.isVariable()) dataVarFieldCount++;
            }
        }

        int[] varOffsets = new int[dataVarFieldCount];
        for (int i = 0; i < dataVarFieldCount; i++) {
            varOffsets[i] = buf.getInt();
        }

        Map<Integer, Object> dataIndexToValue = new HashMap<>();
        for (int i = 0; i < dataColumnCount; i++) {
            if (i >= schemaColumnCount) break;
            String colName = cg.columnNames().get(i);
            ColumnGroupSchemaResolver.FieldLayout layout = resolver.fieldLayouts().get(colName);

            if (!layout.isVariable()) {
                if (isNull[i]) {
                    buf.position(buf.position() + layout.size());
                    dataIndexToValue.put(i, null);
                } else {
                    dataIndexToValue.put(i, readFixed(buf, layout.fieldType(), cg.columnType(i)));
                }
            }
        }

        int varIdx = 0;
        for (int i = 0; i < dataColumnCount; i++) {
            if (i >= schemaColumnCount) break;
            String colName = cg.columnNames().get(i);
            ColumnGroupSchemaResolver.FieldLayout layout = resolver.fieldLayouts().get(colName);

            if (layout.isVariable()) {
                if (isNull[i]) {
                    dataIndexToValue.put(i, null);
                } else {
                    buf.position(varOffsets[varIdx]);
                    dataIndexToValue.put(i, readVariable(buf, layout.fieldType()));
                }
                varIdx++;
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < schemaColumnCount; i++) {
            String colName = cg.columnNames().get(i);
            int fieldId = cg.columnFieldId(i);

            if (fieldId > 0 && fieldIdToDataIndex.containsKey(fieldId)) {
                int dataIdx = fieldIdToDataIndex.get(fieldId);
                result.put(colName, dataIndexToValue.getOrDefault(dataIdx, null));
            } else if (i < dataColumnCount && dataIndexToValue.containsKey(i)) {
                result.put(colName, dataIndexToValue.get(i));
            } else {
                result.put(colName, null);
            }
        }
        return result;
    }

    Object decodeFieldV2(ColumnGroup cg, byte[] data, String columnName) {
        validateCrc32(data);

        int colIndex = cg.columnNames().indexOf(columnName);
        if (colIndex < 0)
            throw new IllegalArgumentException("Column not found in ColumnGroup: " + columnName);

        ColumnGroupSchemaResolver resolver = ColumnGroupSchemaResolver.resolve(cg);
        ColumnGroupSchemaResolver.FieldLayout layout = resolver.fieldLayouts().get(columnName);

        ByteBuffer buf = ByteBuffer.wrap(data, 0, data.length - SIZEOF_CRC32);

        buf.get();
        int dataVersion = buf.getInt();
        validateVersion(dataVersion, cg.schemaVersion());

        int dataColumnCount = buf.getInt();

        int fieldIdCount = readVarint(buf);
        int fieldIdMapBytes = varintSize(fieldIdCount);
        Map<Integer, Integer> fieldIdToDataIndex = new HashMap<>();
        for (int i = 0; i < fieldIdCount; i++) {
            int fieldId = readVarint(buf);
            fieldIdMapBytes += varintSize(fieldId);
            fieldIdToDataIndex.put(fieldId, i);
        }

        int targetFieldId = cg.columnFieldId(colIndex);
        int targetDataIndex;
        if (targetFieldId > 0 && fieldIdToDataIndex.containsKey(targetFieldId)) {
            targetDataIndex = fieldIdToDataIndex.get(targetFieldId);
        } else {
            targetDataIndex = colIndex;
        }

        if (targetDataIndex >= dataColumnCount) return null;

        int dataBitmapBytes = (dataColumnCount + 7) / 8;
        byte[] bitmap = new byte[dataBitmapBytes];
        buf.get(bitmap);

        boolean isNull = (bitmap[targetDataIndex / 8] & (1 << (targetDataIndex % 8))) != 0;
        if (isNull) return null;

        int dataVarFieldCount = 0;
        for (int i = 0; i < dataColumnCount; i++) {
            if (i < cg.columnNames().size()) {
                ColumnGroupSchemaResolver.FieldLayout fl =
                        resolver.fieldLayouts().get(cg.columnNames().get(i));
                if (fl.isVariable()) dataVarFieldCount++;
            }
        }
        int dataHeaderSize =
                1
                        + SIZEOF_INT
                        + SIZEOF_INT
                        + fieldIdMapBytes
                        + dataBitmapBytes
                        + dataVarFieldCount * SIZEOF_INT;

        if (!layout.isVariable()) {
            int dataOffset = dataHeaderSize;
            for (int i = 0; i < targetDataIndex; i++) {
                if (i < cg.columnNames().size()) {
                    ColumnGroupSchemaResolver.FieldLayout fl =
                            resolver.fieldLayouts().get(cg.columnNames().get(i));
                    if (!fl.isVariable()) dataOffset += fl.size();
                }
            }
            buf.position(dataOffset);
            return readFixed(buf, layout.fieldType(), cg.columnType(colIndex));
        } else {
            int dataVarFieldIndex = 0;
            for (int i = 0; i < targetDataIndex; i++) {
                if (i < cg.columnNames().size()) {
                    ColumnGroupSchemaResolver.FieldLayout fl =
                            resolver.fieldLayouts().get(cg.columnNames().get(i));
                    if (fl.isVariable()) dataVarFieldIndex++;
                }
            }
            int varOffsetTablePos = 1 + SIZEOF_INT + SIZEOF_INT + fieldIdMapBytes + dataBitmapBytes;
            buf.position(varOffsetTablePos + dataVarFieldIndex * SIZEOF_INT);
            int fieldOffset = buf.getInt();
            if (fieldOffset == 0) return null;
            buf.position(fieldOffset);
            return readVariable(buf, layout.fieldType());
        }
    }

    byte[] encodeV1(ColumnGroup cg, Map<String, Object> values) {
        ColumnGroupSchemaResolver resolver = ColumnGroupSchemaResolver.resolve(cg);
        int columnCount = cg.columnNames().size();
        int bitmapBytes = resolver.bitmapBytes();
        int varFieldCount = resolver.varFieldCount();
        int varOffsetTableSize = varFieldCount * SIZEOF_INT;
        int headerSize = resolver.headerSize();
        int fixedDataSize = resolver.fixedDataSize();

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

        int varDataSize = 0;
        for (int idx : varIndices) {
            Object val = values.get(cg.columnNames().get(idx));
            if (val == null) {
            } else if (val instanceof String s) {
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

        int varOffsetTablePos = buf.position();
        if (varFieldCount > 0) {
            buf.position(buf.position() + varOffsetTableSize);
        }

        for (int idx : fixedIndices) {
            Object val = values.get(cg.columnNames().get(idx));
            ColumnGroupSchemaResolver.FieldLayout layout =
                    resolver.fieldLayouts().get(cg.columnNames().get(idx));
            writeFixed(buf, layout.fieldType(), val);
        }

        int[] varOffsets = new int[varFieldCount];
        int varIdx = 0;
        for (int idx : varIndices) {
            Object val = values.get(cg.columnNames().get(idx));
            if (val == null) {
                varOffsets[varIdx] = 0;
            } else {
                varOffsets[varIdx] = buf.position();
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
            varIdx++;
        }

        if (varFieldCount > 0) {
            int savedPos = buf.position();
            buf.position(varOffsetTablePos);
            for (int offset : varOffsets) {
                buf.putInt(offset);
            }
            buf.position(savedPos);
        }

        CRC32 crc32 = new CRC32();
        crc32.update(buf.array(), 0, buf.position());
        buf.putInt((int) crc32.getValue());

        return buf.array();
    }

    Map<String, Object> decodeV1(ColumnGroup cg, byte[] data) {
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

        int dataVarFieldCount = 0;
        for (int i = 0; i < Math.min(dataColumnCount, schemaColumnCount); i++) {
            ColumnGroupSchemaResolver.FieldLayout layout =
                    resolver.fieldLayouts().get(cg.columnNames().get(i));
            if (layout.isVariable()) {
                dataVarFieldCount++;
            }
        }

        boolean[] isNull = new boolean[dataColumnCount];
        for (int i = 0; i < dataColumnCount; i++) {
            isNull[i] = (bitmap[i / 8] & (1 << (i % 8))) != 0;
        }

        int[] varOffsets = new int[dataVarFieldCount];
        for (int i = 0; i < dataVarFieldCount; i++) {
            varOffsets[i] = buf.getInt();
        }

        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < schemaColumnCount; i++) {
            String colName = cg.columnNames().get(i);
            ColumnGroupSchemaResolver.FieldLayout layout = resolver.fieldLayouts().get(colName);

            if (i >= dataColumnCount) {
                result.put(colName, null);
                continue;
            }

            if (!layout.isVariable()) {
                if (isNull[i]) {
                    buf.position(buf.position() + layout.size());
                    result.put(colName, null);
                } else {
                    result.put(colName, readFixed(buf, layout.fieldType(), cg.columnType(i)));
                }
            }
        }

        int varIdx = 0;
        for (int i = 0; i < schemaColumnCount; i++) {
            String colName = cg.columnNames().get(i);
            ColumnGroupSchemaResolver.FieldLayout layout = resolver.fieldLayouts().get(colName);

            if (i >= dataColumnCount) {
                continue;
            }

            if (layout.isVariable()) {
                if (isNull[i]) {
                    result.put(colName, null);
                } else {
                    buf.position(varOffsets[varIdx]);
                    result.put(colName, readVariable(buf, layout.fieldType()));
                }
                varIdx++;
            }
        }

        return result;
    }

    Object decodeFieldV1(ColumnGroup cg, byte[] data, String columnName) {
        validateCrc32(data);

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

        int dataVarFieldCount = 0;
        for (int i = 0; i < dataColumnCount; i++) {
            ColumnGroupSchemaResolver.FieldLayout fl =
                    resolver.fieldLayouts().get(cg.columnNames().get(i));
            if (fl.isVariable()) {
                dataVarFieldCount++;
            }
        }
        int dataHeaderSize =
                SIZEOF_INT + SIZEOF_INT + dataBitmapBytes + dataVarFieldCount * SIZEOF_INT;

        if (!layout.isVariable()) {
            int dataOffset = dataHeaderSize;
            for (int i = 0; i < colIndex; i++) {
                ColumnGroupSchemaResolver.FieldLayout fl =
                        resolver.fieldLayouts().get(cg.columnNames().get(i));
                if (!fl.isVariable()) {
                    dataOffset += fl.size();
                }
            }
            buf.position(dataOffset);
            return readFixed(buf, layout.fieldType(), cg.columnType(colIndex));
        } else {
            int dataVarFieldIndex = 0;
            for (int i = 0; i < colIndex; i++) {
                ColumnGroupSchemaResolver.FieldLayout fl =
                        resolver.fieldLayouts().get(cg.columnNames().get(i));
                if (fl.isVariable()) {
                    dataVarFieldIndex++;
                }
            }
            int varOffsetTablePos = SIZEOF_INT + SIZEOF_INT + dataBitmapBytes;
            buf.position(varOffsetTablePos + dataVarFieldIndex * SIZEOF_INT);
            int fieldOffset = buf.getInt();
            if (fieldOffset == 0) return null;
            buf.position(fieldOffset);
            return readVariable(buf, layout.fieldType());
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
            case BYTE ->
                    buf.put(
                            val != null
                                    ? (val instanceof Boolean b
                                            ? (byte) (b ? 1 : 0)
                                            : ((Number) val).byteValue())
                                    : (byte) 0);
            case INT -> buf.putInt(val != null ? ((Number) val).intValue() : 0);
            case LONG -> buf.putLong(val != null ? ((Number) val).longValue() : 0L);
            case DOUBLE -> buf.putDouble(val != null ? ((Number) val).doubleValue() : 0.0);
            case FLOAT -> buf.putFloat(val != null ? ((Number) val).floatValue() : 0.0f);
            default -> throw new IllegalArgumentException("Unknown fixed type: " + ft);
        }
    }

    private Object readFixed(ByteBuffer buf, ColumnGroupSchemaResolver.FieldType ft) {
        return readFixed(buf, ft, null);
    }

    private Object readFixed(
            ByteBuffer buf, ColumnGroupSchemaResolver.FieldType ft, RawWiseType expectedType) {
        return switch (ft) {
            case BYTE -> {
                byte val = buf.get();
                if (expectedType instanceof BooleanType) {
                    yield val != 0;
                }
                yield val;
            }
            case INT -> {
                int v = buf.getInt();
                if (expectedType instanceof Int8Type) {
                    yield (byte) v;
                }
                yield v;
            }
            case LONG -> buf.getLong();
            case DOUBLE -> buf.getDouble();
            case FLOAT -> buf.getFloat();
            default -> throw new IllegalArgumentException("Unknown fixed type: " + ft);
        };
    }

    private Object readVariable(ByteBuffer buf, ColumnGroupSchemaResolver.FieldType ft) {
        int len = buf.getInt();
        if (len < 0) {
            throw new IllegalStateException(
                    "Invalid variable field length: "
                            + len
                            + " (null fields are indicated by bitmap, not negative length)");
        }
        if (len > MAX_VAR_FIELD_SIZE) {
            throw new IllegalStateException(
                    "Variable field length " + len + " exceeds maximum " + MAX_VAR_FIELD_SIZE);
        }
        byte[] bytes = new byte[len];
        buf.get(bytes);
        if (ft == ColumnGroupSchemaResolver.FieldType.STRING) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return bytes;
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
}
