package io.github.rawwise.model.record;

import java.util.Map;

public class JavaTypeConverter implements TypeConverter<Class<?>> {
    private static final Map<Class<?>, RawWiseType> JAVA_TO_DATA_TYPE =
            Map.ofEntries(
                    Map.entry(Boolean.class, new BooleanType(true)),
                    Map.entry(byte.class, new Int8Type(false)),
                    Map.entry(Byte.class, new Int8Type(true)),
                    Map.entry(int.class, new Int32Type(false)),
                    Map.entry(Integer.class, new Int32Type(true)),
                    Map.entry(long.class, new Int64Type(false)),
                    Map.entry(Long.class, new Int64Type(true)),
                    Map.entry(float.class, new Float32Type(false)),
                    Map.entry(Float.class, new Float32Type(true)),
                    Map.entry(double.class, new Float64Type(false)),
                    Map.entry(Double.class, new Float64Type(true)),
                    Map.entry(String.class, new StringType(true)),
                    Map.entry(byte[].class, new BinaryType(true)),
                    Map.entry(java.sql.Date.class, new DateType(true)),
                    Map.entry(java.util.Date.class, new TimestampType(true)),
                    Map.entry(java.time.LocalDate.class, new DateType(true)),
                    Map.entry(java.time.LocalDateTime.class, new TimestampType(true)));

    @Override
    public RawWiseType toRawWiseType(Class<?> javaType) {
        RawWiseType result = JAVA_TO_DATA_TYPE.get(javaType);
        if (result == null) {
            throw new IllegalArgumentException("Unsupported Java type: " + javaType.getName());
        }
        return result;
    }

    @Override
    public Class<?> fromRawWiseType(RawWiseType dataType) {
        return dataType.accept(new JavaTypeVisitor());
    }

    private static class JavaTypeVisitor implements TypeVisitor<Class<?>> {
        public Class<?> visitNull(NullType type) {
            return Void.class;
        }

        public Class<?> visit(BooleanType type) {
            return Boolean.class;
        }

        public Class<?> visit(Int8Type type) {
            return Byte.class;
        }

        public Class<?> visit(Int32Type type) {
            return Integer.class;
        }

        public Class<?> visit(Int64Type type) {
            return Long.class;
        }

        public Class<?> visit(Float32Type type) {
            return Float.class;
        }

        public Class<?> visit(Float64Type type) {
            return Double.class;
        }

        public Class<?> visit(DecimalType type) {
            return java.math.BigDecimal.class;
        }

        public Class<?> visit(StringType type) {
            return String.class;
        }

        public Class<?> visit(BinaryType type) {
            return byte[].class;
        }

        public Class<?> visit(DateType type) {
            return java.time.LocalDate.class;
        }

        public Class<?> visit(TimestampType type) {
            return java.time.LocalDateTime.class;
        }

        public Class<?> visit(ArrayType type) {
            Class<?> elementClass = type.elementType().accept(this);
            return java.util.List.class; // simplified handling
        }

        public Class<?> visit(MapType type) {
            return Map.class;
        }

        public Class<?> visit(StructType type) {
            return Map.class;
        }
    }
}
