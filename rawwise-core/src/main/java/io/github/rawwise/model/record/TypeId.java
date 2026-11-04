package io.github.rawwise.model.record;

public final class TypeId {
    private TypeId() {}

    public static final byte TYPE_NULL = 0;
    public static final byte TYPE_BINARY = 1;
    public static final byte TYPE_STRING = 2;
    public static final byte TYPE_INT = 3;
    public static final byte TYPE_LONG = 4;
    public static final byte TYPE_FLOAT = 5;
    public static final byte TYPE_DOUBLE = 6;
    public static final byte TYPE_BOOL = 7;
    public static final byte TYPE_DECIMAL = 8;
    public static final byte TYPE_DATE = 9;
    public static final byte TYPE_TIMESTAMP = 10;
    public static final byte TYPE_FALLBACK = 127;
}
