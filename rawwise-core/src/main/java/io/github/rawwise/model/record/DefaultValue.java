package io.github.rawwise.model.record;

public record DefaultValue(RawWiseType type, Object raw) implements Record.Value {
    private static final byte[] EMPTY_BYTES = new byte[0];

    public static final DefaultValue NULL = new DefaultValue(NullType.INSTANCE, null);

    @Override
    public boolean isNull() {
        return raw == null;
    }

    @Override
    public byte[] rawBytes() {
        if (raw instanceof byte[] b) return b.clone();
        return EMPTY_BYTES;
    }

    @Override
    public Comparable<?> comparable() {
        if (isNull()) throw new ClassCastException("NULL value is not Comparable");
        if (raw instanceof Comparable<?> c) return c;
        if (raw instanceof byte[] bytes) return ComparableByteArray.wrap(bytes);
        throw new ClassCastException(
                "Value of type " + type + " is not Comparable: " + raw.getClass());
    }
}
