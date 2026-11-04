package io.github.rawwise.model.record;

public sealed interface RawWiseType permits ComplexType, PrimitiveType {

    String name();

    int id();

    boolean isNullable();

    String description();

    <T> T accept(TypeVisitor<T> visitor);

    RawWiseType copyWithNullable(boolean nullable);

    enum TypeCategory {
        PRIMITIVE,
        COMPLEX,
        COLLECTION,
        STRUCT
    }

    default TypeCategory category() {
        return TypeCategory.PRIMITIVE;
    }
}

interface TypeVisitor<T> {
    default T visitNull(NullType type) {
        return null;
    }

    T visit(BooleanType type);

    T visit(Int8Type type);

    T visit(Int32Type type);

    T visit(Int64Type type);

    T visit(Float32Type type);

    T visit(Float64Type type);

    T visit(DecimalType type);

    T visit(StringType type);

    T visit(BinaryType type);

    T visit(DateType type);

    T visit(TimestampType type);

    T visit(ArrayType type);

    T visit(MapType type);

    T visit(StructType type);
}
