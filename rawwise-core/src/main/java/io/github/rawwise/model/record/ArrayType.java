package io.github.rawwise.model.record;

import java.io.Serializable;

public record ArrayType(
        RawWiseType elementType, int fixedSize, boolean isNullable, String description)
        implements ComplexType, Serializable {
    private static final long serialVersionUID = 2L;

    public ArrayType {
        if (elementType == null) throw new IllegalArgumentException("Element type cannot be null");
        if (fixedSize < 0) throw new IllegalArgumentException("fixedSize must be >= 0");
        description = description != null ? description : "Array of " + elementType.name();
    }

    public ArrayType(RawWiseType elementType) {
        this(elementType, 0, true, null);
    }

    public ArrayType(RawWiseType elementType, boolean isNullable) {
        this(elementType, 0, isNullable, null);
    }

    public ArrayType(RawWiseType elementType, boolean isNullable, String description) {
        this(elementType, 0, isNullable, description);
    }

    public ArrayType(RawWiseType elementType, int fixedSize, boolean isNullable) {
        this(elementType, fixedSize, isNullable, null);
    }

    public String name() {
        if (fixedSize > 0) {
            return "array<" + elementType.name() + ">[" + fixedSize + "]";
        }
        return "array<" + elementType.name() + ">";
    }

    public int id() {
        return 100;
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public RawWiseType copyWithNullable(boolean nullable) {
        return new ArrayType(elementType, fixedSize, nullable, description);
    }
}
