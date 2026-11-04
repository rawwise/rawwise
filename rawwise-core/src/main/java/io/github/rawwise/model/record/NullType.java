package io.github.rawwise.model.record;

import java.io.Serializable;

public record NullType() implements PrimitiveType, Serializable {
    private static final long serialVersionUID = 1L;
    public static final NullType INSTANCE = new NullType();

    @Override
    public String name() {
        return "null";
    }

    @Override
    public int id() {
        return 0;
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public String description() {
        return "Null sentinel type";
    }

    @Override
    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visitNull(this);
    }

    @Override
    public RawWiseType copyWithNullable(boolean nullable) {
        return this;
    }

    @Override
    public TypeCategory category() {
        return TypeCategory.PRIMITIVE;
    }
}
