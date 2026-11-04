package io.github.rawwise.model.record;

public final class Types {

    private Types() {}

    public static Int64Type int64() {
        return new Int64Type();
    }

    public static Int64Type int64(boolean nullable) {
        return new Int64Type(nullable);
    }

    public static Int32Type int32() {
        return new Int32Type();
    }

    public static Int32Type int32(boolean nullable) {
        return new Int32Type(nullable);
    }

    public static Int8Type int8() {
        return new Int8Type();
    }

    public static Int8Type int8(boolean nullable) {
        return new Int8Type(nullable);
    }

    public static StringType string() {
        return new StringType();
    }

    public static StringType string(boolean nullable) {
        return new StringType(nullable);
    }

    public static TimestampType timestamp() {
        return new TimestampType();
    }

    public static TimestampType timestamp(boolean nullable) {
        return new TimestampType(nullable);
    }

    public static DateType date() {
        return new DateType();
    }

    public static DateType date(boolean nullable) {
        return new DateType(nullable);
    }

    public static BooleanType bool() {
        return new BooleanType();
    }

    public static BooleanType bool(boolean nullable) {
        return new BooleanType(nullable);
    }

    public static Float32Type float32() {
        return new Float32Type();
    }

    public static Float32Type float32(boolean nullable) {
        return new Float32Type(nullable);
    }

    public static Float64Type float64() {
        return new Float64Type();
    }

    public static Float64Type float64(boolean nullable) {
        return new Float64Type(nullable);
    }

    public static DecimalType decimal(int precision, int scale) {
        return new DecimalType(precision, scale);
    }

    public static DecimalType decimal(int precision, int scale, boolean nullable) {
        return new DecimalType(precision, scale, nullable, null);
    }

    public static BinaryType binary() {
        return new BinaryType();
    }

    public static BinaryType binary(boolean nullable) {
        return new BinaryType(nullable);
    }
}
