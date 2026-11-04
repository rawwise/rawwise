package io.github.rawwise.model.record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ColumnGroupSchemaResolver {

    private static final int SIZEOF_INT = 4;
    private static final int SIZEOF_COLUMN_COUNT = 4;

    /**
     * Per-instance resolver cache keyed by {@link System#identity Hash Code(Object)}.
     *
     * <p>Avoids recomputing field layouts, header sizes, and offset tables for the same Column
     * Group instance on every encode/decode/decode Field call. Using identity hash avoids depending
     * on structural equality of Column Group (whose Raw Wise Type components may not implement
     * consistent equals/hash Code).
     */
    private static final ConcurrentHashMap<Integer, ColumnGroupSchemaResolver> RESOLVER_CACHE =
            new ConcurrentHashMap<>();

    private final ColumnGroup columnGroup;
    private final Map<String, FieldLayout> fieldLayouts;
    private final int headerSize;
    private final int varFieldCount;
    private final int fixedDataSize;

    private ColumnGroupSchemaResolver(
            ColumnGroup columnGroup,
            Map<String, FieldLayout> fieldLayouts,
            int headerSize,
            int varFieldCount,
            int fixedDataSize) {
        this.columnGroup = columnGroup;
        this.fieldLayouts = Collections.unmodifiableMap(fieldLayouts);
        this.headerSize = headerSize;
        this.varFieldCount = varFieldCount;
        this.fixedDataSize = fixedDataSize;
    }

    public static ColumnGroupSchemaResolver resolve(ColumnGroup cg) {
        if (cg.encoding() != ColumnGroup.Encoding.FIXED_OFFSET) {
            throw new UnsupportedOperationException(
                    "Only FIXED_OFFSET encoding is supported, got: " + cg.encoding());
        }
        if (cg.columnTypes() == null || cg.columnTypes().isEmpty()) {
            throw new IllegalArgumentException(
                    "FIXED_OFFSET encoding requires columnTypes in ColumnGroup");
        }

        int idKey = System.identityHashCode(cg);
        ColumnGroupSchemaResolver cached = RESOLVER_CACHE.get(idKey);
        if (cached != null) return cached;

        int columnCount = cg.columnNames().size();
        int bitmapBytes = (columnCount + 7) / 8;

        List<String> fixedCols = new ArrayList<>();
        List<String> varCols = new ArrayList<>();
        for (String col : cg.columnNames()) {
            RawWiseType type = cg.columnType(col);
            FieldType fieldType = mapFieldType(type);
            if (fieldType.fixedSize() > 0) {
                fixedCols.add(col);
            } else {
                varCols.add(col);
            }
        }

        int varFieldCount = varCols.size();
        int varOffsetTableSize = varFieldCount * SIZEOF_INT;
        int headerSize = SIZEOF_INT + SIZEOF_COLUMN_COUNT + bitmapBytes + varOffsetTableSize;

        Map<String, FieldLayout> layouts = new LinkedHashMap<>();
        int fixedOff = headerSize;
        int fixedDataSize = 0;
        for (String col : fixedCols) {
            RawWiseType type = cg.columnType(col);
            FieldType fieldType = mapFieldType(type);
            layouts.put(
                    col, new FieldLayout(fixedOff, fieldType.fixedSize(), false, fieldType, -1));
            fixedOff += fieldType.fixedSize();
            fixedDataSize += fieldType.fixedSize();
        }

        int varDataStart = fixedOff;
        int varIdx = 0;
        for (String col : varCols) {
            RawWiseType type = cg.columnType(col);
            FieldType fieldType = mapFieldType(type);
            layouts.put(col, new FieldLayout(varDataStart, -1, true, fieldType, varIdx));
            varIdx++;
        }

        ColumnGroupSchemaResolver result =
                new ColumnGroupSchemaResolver(
                        cg, layouts, headerSize, varFieldCount, fixedDataSize);
        RESOLVER_CACHE.put(idKey, result);
        return result;
    }

    public static ColumnGroupSchemaResolver resolve(ColumnGroup cg, StructType structType) {
        if (cg.encoding() != ColumnGroup.Encoding.FIXED_OFFSET) {
            throw new UnsupportedOperationException(
                    "Only FIXED_OFFSET encoding is supported, got: " + cg.encoding());
        }

        int columnCount = cg.columnNames().size();
        int bitmapBytes = (columnCount + 7) / 8;

        List<String> fixedCols = new ArrayList<>();
        List<String> varCols = new ArrayList<>();
        for (String col : cg.columnNames()) {
            RawWiseType type = structType.fieldType(col);
            FieldType fieldType = mapFieldType(type);
            if (fieldType.fixedSize() > 0) {
                fixedCols.add(col);
            } else {
                varCols.add(col);
            }
        }

        int varFieldCount = varCols.size();
        int varOffsetTableSize = varFieldCount * SIZEOF_INT;
        int headerSize = SIZEOF_INT + SIZEOF_COLUMN_COUNT + bitmapBytes + varOffsetTableSize;

        Map<String, FieldLayout> layouts = new LinkedHashMap<>();
        int fixedOff = headerSize;
        int fixedDataSize = 0;
        for (String col : fixedCols) {
            RawWiseType type = structType.fieldType(col);
            FieldType fieldType = mapFieldType(type);
            layouts.put(
                    col, new FieldLayout(fixedOff, fieldType.fixedSize(), false, fieldType, -1));
            fixedOff += fieldType.fixedSize();
            fixedDataSize += fieldType.fixedSize();
        }

        int varDataStart = fixedOff;
        int varIdx = 0;
        for (String col : varCols) {
            RawWiseType type = structType.fieldType(col);
            FieldType fieldType = mapFieldType(type);
            layouts.put(col, new FieldLayout(varDataStart, -1, true, fieldType, varIdx));
            varIdx++;
        }

        return new ColumnGroupSchemaResolver(cg, layouts, headerSize, varFieldCount, fixedDataSize);
    }

    private static FieldType mapFieldType(RawWiseType type) {
        return type.accept(
                new TypeVisitor<>() {
                    public FieldType visit(Int8Type t) {
                        return FieldType.INT;
                    }

                    public FieldType visit(Int32Type t) {
                        return FieldType.INT;
                    }

                    public FieldType visit(Int64Type t) {
                        return FieldType.LONG;
                    }

                    public FieldType visit(Float32Type t) {
                        return FieldType.FLOAT;
                    }

                    public FieldType visit(Float64Type t) {
                        return FieldType.DOUBLE;
                    }

                    public FieldType visit(StringType t) {
                        return FieldType.STRING;
                    }

                    public FieldType visit(BinaryType t) {
                        return FieldType.BYTES;
                    }

                    public FieldType visit(BooleanType t) {
                        return FieldType.BYTE;
                    }

                    public FieldType visit(DecimalType t) {
                        return FieldType.LONG;
                    }

                    public FieldType visit(DateType t) {
                        return FieldType.INT;
                    }

                    public FieldType visit(TimestampType t) {
                        return FieldType.LONG;
                    }

                    public FieldType visit(ArrayType t) {
                        return FieldType.BYTES;
                    }

                    public FieldType visit(MapType t) {
                        return FieldType.BYTES;
                    }

                    public FieldType visit(StructType t) {
                        return FieldType.BYTES;
                    }
                });
    }

    public Map<String, FieldLayout> fieldLayouts() {
        return fieldLayouts;
    }

    public int headerSize() {
        return headerSize;
    }

    public int varFieldCount() {
        return varFieldCount;
    }

    public int fixedDataSize() {
        return fixedDataSize;
    }

    public int bitmapBytes() {
        return (columnGroup.columnNames().size() + 7) / 8;
    }

    public int varOffsetTableOffset() {
        return SIZEOF_INT + SIZEOF_COLUMN_COUNT + bitmapBytes();
    }

    public int fixedDataOffset() {
        return headerSize;
    }

    public int varDataOffset() {
        return headerSize + fixedDataSize;
    }

    public Optional<Integer> fixedRecordSize() {
        boolean hasVariable = fieldLayouts.values().stream().anyMatch(FieldLayout::isVariable);
        if (hasVariable) return Optional.empty();
        int lastEnd =
                fieldLayouts.values().stream().mapToInt(l -> l.offset() + l.size()).max().orElse(0);
        return Optional.of(lastEnd);
    }

    public record FieldLayout(
            int offset, int size, boolean isVariable, FieldType fieldType, int varFieldIndex) {}

    public enum FieldType {
        BYTE(1),
        INT(4),
        LONG(8),
        DOUBLE(8),
        FLOAT(4),
        STRING(-1),
        BYTES(-1);

        final int fixedSize;

        FieldType(int fixedSize) {
            this.fixedSize = fixedSize;
        }

        public int fixedSize() {
            return fixedSize;
        }
    }
}
