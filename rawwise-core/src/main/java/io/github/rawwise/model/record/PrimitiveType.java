package io.github.rawwise.model.record;

import java.io.Serializable;

public sealed interface PrimitiveType extends RawWiseType, Serializable
        permits BooleanType,
                Int8Type,
                Int32Type,
                Int64Type,
                Float32Type,
                Float64Type,
                DecimalType,
                StringType,
                BinaryType,
                DateType,
                TimestampType,
                NullType {

    int DEFAULT_TYPE_ID = 0;

    PrimitiveType LONG = new Int64Type();
    PrimitiveType INT = new Int32Type();
    PrimitiveType DOUBLE = new Float64Type();
    PrimitiveType FLOAT = new Float32Type();
    PrimitiveType STRING = new StringType();
    PrimitiveType BINARY = new BinaryType();
    PrimitiveType BOOLEAN = new BooleanType();

    default int id() {
        return DEFAULT_TYPE_ID;
    }

    default TypeCategory category() {
        return TypeCategory.PRIMITIVE;
    }
}

record BooleanType(boolean isNullable, String description) implements PrimitiveType, Serializable {
    private static final long serialVersionUID = 1L;

    public BooleanType {
        description = description != null ? description : "RawWise boolean type";
    }

    public BooleanType(boolean isNullable) {
        this(isNullable, "RawWise boolean type");
    }

    public BooleanType() {
        this(true, "RawWise boolean type");
    }

    public String name() {
        return "boolean";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public RawWiseType copyWithNullable(boolean nullable) {
        return new BooleanType(nullable, description);
    }
}

record Int8Type(boolean isNullable, String description) implements PrimitiveType, Serializable {
    private static final long serialVersionUID = 1L;

    public Int8Type {
        description = description != null ? description : "RawWise 8-bit integer";
    }

    public Int8Type(boolean isNullable) {
        this(isNullable, "RawWise 8-bit integer");
    }

    public Int8Type() {
        this(true, "RawWise 8-bit integer");
    }

    public String name() {
        return "int8";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public RawWiseType copyWithNullable(boolean nullable) {
        return new Int8Type(nullable, description);
    }
}

record Int32Type(boolean isNullable, String description) implements PrimitiveType, Serializable {
    private static final long serialVersionUID = 1L;

    public Int32Type {
        description = description != null ? description : "RawWise 32-bit integer";
    }

    public Int32Type(boolean isNullable) {
        this(isNullable, "RawWise 32-bit integer");
    }

    public Int32Type() {
        this(true, "RawWise 32-bit integer");
    }

    public String name() {
        return "int32";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public RawWiseType copyWithNullable(boolean nullable) {
        return new Int32Type(nullable, description);
    }
}

record Int64Type(boolean isNullable, String description) implements PrimitiveType, Serializable {
    private static final long serialVersionUID = 1L;

    public Int64Type {
        description = description != null ? description : "RawWise 64-bit integer";
    }

    public Int64Type(boolean isNullable) {
        this(isNullable, "RawWise 64-bit integer");
    }

    public Int64Type() {
        this(true, "RawWise 64-bit integer");
    }

    public String name() {
        return "int64";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public RawWiseType copyWithNullable(boolean nullable) {
        return new Int64Type(nullable, description);
    }
}

record Float32Type(boolean isNullable, String description) implements PrimitiveType, Serializable {
    private static final long serialVersionUID = 1L;

    public Float32Type {
        description = description != null ? description : "RawWise 32-bit float";
    }

    public Float32Type(boolean isNullable) {
        this(isNullable, "RawWise 32-bit float");
    }

    public Float32Type() {
        this(true, "RawWise 32-bit float");
    }

    public String name() {
        return "float32";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public RawWiseType copyWithNullable(boolean nullable) {
        return new Float32Type(nullable, description);
    }
}

record Float64Type(boolean isNullable, String description) implements PrimitiveType, Serializable {
    private static final long serialVersionUID = 1L;

    public Float64Type {
        description = description != null ? description : "RawWise 64-bit float";
    }

    public Float64Type(boolean isNullable) {
        this(isNullable, "RawWise 64-bit float");
    }

    public Float64Type() {
        this(true, "RawWise 64-bit float");
    }

    public String name() {
        return "float64";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public RawWiseType copyWithNullable(boolean nullable) {
        return new Float64Type(nullable, description);
    }
}

record DecimalType(int precision, int scale, boolean isNullable, String description)
        implements PrimitiveType, Serializable {
    private static final long serialVersionUID = 1L;

    public DecimalType {
        if (precision <= 0 || precision > 38)
            throw new IllegalArgumentException("Precision must be between 1 and 38");
        if (scale < 0 || scale > precision)
            throw new IllegalArgumentException("Scale must be between 0 and precision");
        description =
                description != null
                        ? description
                        : "RawWise decimal type with precision " + precision + ", scale " + scale;
    }

    public DecimalType(int precision, int scale) {
        this(precision, scale, true, null);
    }

    public String name() {
        return "decimal(" + precision + "," + scale + ")";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public RawWiseType copyWithNullable(boolean nullable) {
        return new DecimalType(precision, scale, nullable, description);
    }
}

record StringType(boolean isNullable, String description) implements PrimitiveType, Serializable {
    private static final long serialVersionUID = 1L;

    public StringType {
        description = description != null ? description : "RawWise string type";
    }

    public StringType(boolean isNullable) {
        this(isNullable, "RawWise string type");
    }

    public StringType() {
        this(true, "RawWise string type");
    }

    public String name() {
        return "string";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public RawWiseType copyWithNullable(boolean nullable) {
        return new StringType(nullable, description);
    }
}

record BinaryType(boolean isNullable, String description) implements PrimitiveType, Serializable {
    private static final long serialVersionUID = 1L;

    public BinaryType {
        description = description != null ? description : "RawWise binary data";
    }

    public BinaryType(boolean isNullable) {
        this(isNullable, "RawWise binary data");
    }

    public BinaryType() {
        this(true, "RawWise binary data");
    }

    public String name() {
        return "binary";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public RawWiseType copyWithNullable(boolean nullable) {
        return new BinaryType(nullable, description);
    }
}

record DateType(boolean isNullable, String description) implements PrimitiveType, Serializable {
    private static final long serialVersionUID = 1L;

    public DateType {
        description = description != null ? description : "RawWise date type";
    }

    public DateType(boolean isNullable) {
        this(isNullable, "RawWise date type");
    }

    public DateType() {
        this(true, "RawWise date type");
    }

    public String name() {
        return "date";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public RawWiseType copyWithNullable(boolean nullable) {
        return new DateType(nullable, description);
    }
}

record TimestampType(boolean isNullable, String description)
        implements PrimitiveType, Serializable {
    private static final long serialVersionUID = 1L;

    public TimestampType {
        description = description != null ? description : "RawWise timestamp type";
    }

    public TimestampType(boolean isNullable) {
        this(isNullable, "RawWise timestamp type");
    }

    public TimestampType() {
        this(true, "RawWise timestamp type");
    }

    public String name() {
        return "timestamp";
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public RawWiseType copyWithNullable(boolean nullable) {
        return new TimestampType(nullable, description);
    }
}
