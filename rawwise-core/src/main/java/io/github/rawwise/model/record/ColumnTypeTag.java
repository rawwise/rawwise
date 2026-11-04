package io.github.rawwise.model.record;

public final class ColumnTypeTag {

    public static final byte NULL = 0x00;
    public static final byte INT = 0x01;
    public static final byte LONG = 0x02;
    public static final byte DOUBLE = 0x03;
    public static final byte FLOAT = 0x04;
    public static final byte STRING = 0x05;
    public static final byte BYTES = 0x06;
    public static final byte BYTE = 0x07;

    private ColumnTypeTag() {}

    public static byte of(Object val) {
        if (val == null) return NULL;
        if (val instanceof Integer) return INT;
        if (val instanceof Long) return LONG;
        if (val instanceof Double) return DOUBLE;
        if (val instanceof Float) return FLOAT;
        if (val instanceof String) return STRING;
        if (val instanceof byte[]) return BYTES;
        if (val instanceof Byte) return BYTE;
        if (val instanceof Boolean) return BYTE;
        throw new IllegalArgumentException("Unsupported type: " + val.getClass().getName());
    }

    public static byte fromRawWiseType(RawWiseType type) {
        if (type == null) return INT;
        return type.accept(
                new TypeVisitor<>() {
                    public Byte visit(BooleanType t) {
                        return BYTE;
                    }

                    public Byte visit(Int8Type t) {
                        return INT;
                    }

                    public Byte visit(Int32Type t) {
                        return INT;
                    }

                    public Byte visit(Int64Type t) {
                        return LONG;
                    }

                    public Byte visit(Float32Type t) {
                        return FLOAT;
                    }

                    public Byte visit(Float64Type t) {
                        return DOUBLE;
                    }

                    public Byte visit(DecimalType t) {
                        return LONG;
                    }

                    public Byte visit(StringType t) {
                        return STRING;
                    }

                    public Byte visit(BinaryType t) {
                        return BYTES;
                    }

                    public Byte visit(DateType t) {
                        return INT;
                    }

                    public Byte visit(TimestampType t) {
                        return LONG;
                    }

                    public Byte visit(ArrayType t) {
                        return BYTES;
                    }

                    public Byte visit(MapType t) {
                        return BYTES;
                    }

                    public Byte visit(StructType t) {
                        return BYTES;
                    }
                });
    }

    public static byte fromFieldType(ColumnGroupSchemaResolver.FieldType ft) {
        return switch (ft) {
            case BYTE -> BYTE;
            case INT -> INT;
            case LONG -> LONG;
            case DOUBLE -> DOUBLE;
            case FLOAT -> FLOAT;
            case STRING -> STRING;
            case BYTES -> BYTES;
        };
    }

    public static boolean isFixed(byte type) {
        return type == INT || type == LONG || type == DOUBLE || type == FLOAT || type == BYTE;
    }

    public static boolean isVariable(byte type) {
        return type == STRING || type == BYTES;
    }

    public static int fixedSize(byte type) {
        return switch (type) {
            case BYTE -> 1;
            case INT, FLOAT -> 4;
            case LONG, DOUBLE -> 8;
            default -> 0;
        };
    }
}
