package io.github.rawwise.model.record;

import java.io.Serializable;

public record MapType(
        RawWiseType keyType, RawWiseType valueType, boolean isNullable, String description)
        implements ComplexType, Serializable {
    private static final long serialVersionUID = 1L;

    public MapType {
        if (keyType == null) throw new IllegalArgumentException("Key type cannot be null");
        if (valueType == null) throw new IllegalArgumentException("Value type cannot be null");
        description =
                description != null
                        ? description
                        : "Map from " + keyType.name() + " to " + valueType.name();
    }

    public MapType(RawWiseType keyType, RawWiseType valueType) {
        this(keyType, valueType, true, null);
    }

    public String name() {
        return "map<" + keyType.name() + "," + valueType.name() + ">";
    }

    public int id() {
        return 101;
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public RawWiseType copyWithNullable(boolean nullable) {
        return new MapType(keyType, valueType, nullable, description);
    }
}
