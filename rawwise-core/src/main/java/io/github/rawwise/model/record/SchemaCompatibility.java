package io.github.rawwise.model.record;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaCompatibility {

    private SchemaCompatibility() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }

    private enum TypeFamily {
        INT,
        FLOAT,
        STRING,
        BINARY,
        BOOLEAN,
        DECIMAL,
        TEMPORAL,
        UNKNOWN
    }

    private static TypeFamily familyOf(RawWiseType type) {
        return switch (type) {
            case Int8Type t -> TypeFamily.INT;
            case Int32Type t -> TypeFamily.INT;
            case Int64Type t -> TypeFamily.INT;
            case Float32Type t -> TypeFamily.FLOAT;
            case Float64Type t -> TypeFamily.FLOAT;
            case StringType t -> TypeFamily.STRING;
            case BinaryType t -> TypeFamily.BINARY;
            case BooleanType t -> TypeFamily.BOOLEAN;
            case DecimalType t -> TypeFamily.DECIMAL;
            case DateType t -> TypeFamily.TEMPORAL;
            case TimestampType t -> TypeFamily.TEMPORAL;
            default -> TypeFamily.UNKNOWN;
        };
    }

    private static int typeOrdinal(RawWiseType type) {
        if (type instanceof Int8Type) return 0;
        if (type instanceof Int32Type) return 1;
        if (type instanceof Int64Type) return 2;
        if (type instanceof Float32Type) return 10;
        if (type instanceof Float64Type) return 11;
        if (type instanceof StringType) return 20;
        if (type instanceof BinaryType) return 25;
        if (type instanceof BooleanType) return 30;
        if (type instanceof DecimalType) return 40;
        if (type instanceof DateType) return 50;
        if (type instanceof TimestampType) return 51;
        return -1;
    }

    public static List<Incompatibility> check(Schema fileSchema, Schema readSchema) {
        if (fileSchema == null || readSchema == null) {
            throw new IllegalArgumentException("Schemas cannot be null");
        }
        List<Incompatibility> issues = new ArrayList<>();
        Map<Integer, StructField> fileById = new HashMap<>();
        Map<String, StructField> fileByName = new HashMap<>();
        for (StructField f : fileSchema.structType().fields()) {
            if (f.hasAssignedFieldId()) {
                fileById.put(f.fieldId(), f);
            }
            fileByName.put(f.name(), f);
        }
        for (StructField readField : readSchema.structType().fields()) {
            StructField fileField = null;
            if (readField.hasAssignedFieldId()) {
                fileField = fileById.get(readField.fieldId());
                if (fileField == null) {
                    continue;
                }
            } else {
                fileField = fileByName.get(readField.name());
                if (fileField == null) {
                    continue;
                }
            }
            if (!isTypeCompatible(fileField.dataType(), readField.dataType())) {
                issues.add(
                        new Incompatibility(
                                readField.name(),
                                fileField.dataType(),
                                readField.dataType(),
                                "Type mismatch: cannot read "
                                        + fileField.dataType().name()
                                        + " as "
                                        + readField.dataType().name()));
            }
            if (!isNullableCompatible(fileField.dataType(), readField.dataType())) {
                issues.add(
                        new Incompatibility(
                                readField.name(),
                                fileField.dataType(),
                                readField.dataType(),
                                "Nullable mismatch: file field nullable state is incompatible with read field"));
            }
        }
        return issues;
    }

    public static boolean isTypeCompatible(RawWiseType fileType, RawWiseType readType) {
        if (fileType == null || readType == null) {
            throw new IllegalArgumentException("Types cannot be null");
        }

        if (fileType.getClass() == readType.getClass()) {
            return checkTypeParamsCompatible(fileType, readType);
        }

        TypeFamily fileFamily = familyOf(fileType);
        TypeFamily readFamily = familyOf(readType);

        if (fileFamily == TypeFamily.UNKNOWN || readFamily == TypeFamily.UNKNOWN) {
            return false;
        }

        if (fileFamily == readFamily) {
            return typeOrdinal(fileType) <= typeOrdinal(readType);
        }

        if (fileFamily == TypeFamily.INT && readFamily == TypeFamily.FLOAT) {
            if (fileType instanceof Int64Type) {
                return readType instanceof Float64Type;
            }
            if (fileType instanceof Int32Type) {
                return readType instanceof Float64Type;
            }
            return true;
        }
        if (fileFamily == TypeFamily.INT && readFamily == TypeFamily.STRING) return true;
        if (fileFamily == TypeFamily.FLOAT && readFamily == TypeFamily.STRING) return true;
        if (fileFamily == TypeFamily.INT && readFamily == TypeFamily.DECIMAL) return true;
        if (fileFamily == TypeFamily.FLOAT && readFamily == TypeFamily.DECIMAL) return true;
        if (fileFamily == TypeFamily.TEMPORAL && readFamily == TypeFamily.STRING) return true;
        if (fileFamily == TypeFamily.BOOLEAN && readFamily == TypeFamily.STRING) return true;

        return false;
    }

    private static boolean checkTypeParamsCompatible(RawWiseType fileType, RawWiseType readType) {
        if (fileType instanceof DecimalType ft && readType instanceof DecimalType rt) {
            return rt.precision() >= ft.precision()
                    && rt.scale() >= ft.scale()
                    && (rt.precision() - rt.scale()) >= (ft.precision() - ft.scale());
        }
        if (fileType instanceof ArrayType fa && readType instanceof ArrayType ra) {
            if (fa.fixedSize() > 0 && ra.fixedSize() > 0 && fa.fixedSize() != ra.fixedSize()) {
                return false;
            }
            if (fa.fixedSize() > 0 && ra.fixedSize() == 0) {
                return false;
            }
            return isTypeCompatible(fa.elementType(), ra.elementType());
        }
        if (fileType instanceof MapType fm && readType instanceof MapType rm) {
            return isTypeCompatible(fm.keyType(), rm.keyType())
                    && isTypeCompatible(fm.valueType(), rm.valueType());
        }
        if (fileType instanceof StructType fs && readType instanceof StructType rs) {
            return checkStructCompatible(fs, rs);
        }
        return true;
    }

    private static boolean checkStructCompatible(StructType fileStruct, StructType readStruct) {
        Map<Integer, StructField> readById = new HashMap<>();
        Map<String, StructField> readByName = new HashMap<>();
        for (StructField f : readStruct.fields()) {
            if (f.hasAssignedFieldId()) {
                readById.put(f.fieldId(), f);
            }
            readByName.put(f.name(), f);
        }
        for (StructField fileField : fileStruct.fields()) {
            StructField readField = null;
            if (fileField.hasAssignedFieldId()) {
                readField = readById.get(fileField.fieldId());
                if (readField == null) continue;
            } else {
                readField = readByName.get(fileField.name());
                if (readField == null) continue;
            }
            if (!isTypeCompatible(fileField.dataType(), readField.dataType())) {
                return false;
            }
        }
        return true;
    }

    static boolean isNullableCompatible(RawWiseType fileType, RawWiseType readType) {
        if (fileType.isNullable() && !readType.isNullable()) return false;
        if (fileType instanceof ArrayType fa && readType instanceof ArrayType ra)
            return isNullableCompatible(fa.elementType(), ra.elementType());
        if (fileType instanceof MapType fm && readType instanceof MapType rm)
            return isNullableCompatible(fm.keyType(), rm.keyType())
                    && isNullableCompatible(fm.valueType(), rm.valueType());
        if (fileType instanceof StructType fs && readType instanceof StructType rs)
            return checkStructNullableCompatible(fs, rs);
        return true;
    }

    private static boolean checkStructNullableCompatible(
            StructType fileStruct, StructType readStruct) {
        Map<Integer, StructField> readById = new HashMap<>();
        Map<String, StructField> readByName = new HashMap<>();
        for (StructField f : readStruct.fields()) {
            if (f.hasAssignedFieldId()) readById.put(f.fieldId(), f);
            readByName.put(f.name(), f);
        }
        for (StructField fileField : fileStruct.fields()) {
            StructField readField = null;
            if (fileField.hasAssignedFieldId()) {
                readField = readById.get(fileField.fieldId());
                if (readField == null) continue;
            } else {
                readField = readByName.get(fileField.name());
                if (readField == null) continue;
            }
            if (!isNullableCompatible(fileField.dataType(), readField.dataType())) return false;
        }
        return true;
    }

    public static Object coerce(Object value, RawWiseType sourceType, RawWiseType targetType) {
        if (value == null) return null;
        if (sourceType.getClass() == targetType.getClass()) return value;
        if (!isTypeCompatible(sourceType, targetType)) {
            throw new IllegalArgumentException(
                    "Cannot coerce incompatible types: "
                            + sourceType.name()
                            + " -> "
                            + targetType.name());
        }

        TypeFamily srcFamily = familyOf(sourceType);
        TypeFamily tgtFamily = familyOf(targetType);

        if (srcFamily == TypeFamily.INT && tgtFamily == TypeFamily.INT) {
            return ((Number) value).longValue();
        }
        if (srcFamily == TypeFamily.INT && tgtFamily == TypeFamily.FLOAT) {
            return ((Number) value).doubleValue();
        }
        if (srcFamily == TypeFamily.FLOAT && tgtFamily == TypeFamily.FLOAT) {
            return ((Number) value).doubleValue();
        }
        if (tgtFamily == TypeFamily.STRING) {
            return String.valueOf(value);
        }
        if (tgtFamily == TypeFamily.DECIMAL) {
            if (srcFamily == TypeFamily.INT) {
                return BigDecimal.valueOf(((Number) value).longValue());
            }
            if (srcFamily == TypeFamily.FLOAT) {
                return BigDecimal.valueOf(((Number) value).doubleValue());
            }
        }
        throw new UnsupportedOperationException(
                "Coercion from "
                        + sourceType.name()
                        + " to "
                        + targetType.name()
                        + " is not implemented");
    }

    public record Incompatibility(
            String fieldName, RawWiseType fileType, RawWiseType readType, String message) {
        public Incompatibility {
            if (fieldName == null || fieldName.isBlank())
                throw new IllegalArgumentException("fieldName cannot be null or blank");
            if (message == null || message.isBlank())
                throw new IllegalArgumentException("message cannot be null or blank");
        }
    }

    public static Object coerceDefault(String defaultStr, RawWiseType dataType) {
        if (defaultStr == null) return null;
        try {
            return dataType.accept(
                    new TypeVisitor<>() {
                        public Object visit(BooleanType t) {
                            return Boolean.parseBoolean(defaultStr);
                        }

                        public Object visit(Int8Type t) {
                            return Byte.parseByte(defaultStr);
                        }

                        public Object visit(Int32Type t) {
                            return Integer.parseInt(defaultStr);
                        }

                        public Object visit(Int64Type t) {
                            return Long.parseLong(defaultStr);
                        }

                        public Object visit(Float32Type t) {
                            return Float.parseFloat(defaultStr);
                        }

                        public Object visit(Float64Type t) {
                            return Double.parseDouble(defaultStr);
                        }

                        public Object visit(DecimalType t) {
                            BigDecimal bd = new BigDecimal(defaultStr);
                            if (bd.precision() > t.precision()) {
                                throw new IllegalArgumentException(
                                        "Default value precision "
                                                + bd.precision()
                                                + " exceeds field precision "
                                                + t.precision());
                            }
                            if (bd.scale() > t.scale()) {
                                throw new IllegalArgumentException(
                                        "Default value scale "
                                                + bd.scale()
                                                + " exceeds field scale "
                                                + t.scale());
                            }
                            if ((bd.precision() - bd.scale()) > (t.precision() - t.scale())) {
                                throw new IllegalArgumentException(
                                        "Default value integer part width "
                                                + (bd.precision() - bd.scale())
                                                + " exceeds field integer part width "
                                                + (t.precision() - t.scale()));
                            }
                            return bd;
                        }

                        public Object visit(StringType t) {
                            return defaultStr;
                        }

                        public Object visit(BinaryType t) {
                            return defaultStr.getBytes(StandardCharsets.UTF_8);
                        }

                        public Object visit(DateType t) {
                            return LocalDate.parse(defaultStr).toEpochDay();
                        }

                        public Object visit(TimestampType t) {
                            return Instant.parse(defaultStr).toEpochMilli();
                        }

                        public Object visit(ArrayType t) {
                            throw new UnsupportedOperationException("Array default not supported");
                        }

                        public Object visit(MapType t) {
                            throw new UnsupportedOperationException("Map default not supported");
                        }

                        public Object visit(StructType t) {
                            throw new UnsupportedOperationException("Struct default not supported");
                        }
                    });
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid default value '" + defaultStr + "' for type " + dataType.name(), e);
        } catch (java.time.format.DateTimeParseException e) {
            throw new IllegalArgumentException(
                    "Invalid default value '" + defaultStr + "' for type " + dataType.name(), e);
        }
    }
}
