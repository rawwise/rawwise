package io.github.rawwise.model.record;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;

public class SparkTypeConverter implements TypeConverter<DataType> {
    @Override
    public RawWiseType toRawWiseType(DataType sparkType) {
        switch (sparkType) {
            case BooleanType t -> {
                return new io.github.rawwise.model.record.BooleanType(true);
            }
            case ByteType t -> {
                return new Int8Type(true, "Spark byte type");
            }
            case IntegerType t -> {
                return new Int32Type(true, "Spark integer type");
            }
            case LongType t -> {
                return new Int64Type(true, "Spark long type");
            }
            case FloatType t -> {
                return new Float32Type(true, "Spark float type");
            }
            case DoubleType t -> {
                return new Float64Type(true, "Spark double type");
            }
            case DecimalType decimal -> {
                return new io.github.rawwise.model.record.DecimalType(
                        decimal.precision(), decimal.scale(), true, "Spark decimal type");
            }
            case StringType t -> {
                return new io.github.rawwise.model.record.StringType(true, "Spark string type");
            }
            case BinaryType t -> {
                return new io.github.rawwise.model.record.BinaryType(true, "Spark binary type");
            }
            case DateType t -> {
                return new io.github.rawwise.model.record.DateType(true, "Spark date type");
            }
            case TimestampType t -> {
                return new io.github.rawwise.model.record.TimestampType(
                        true, "Spark timestamp type");
            }
            case ArrayType array -> {
                RawWiseType elementType = toRawWiseType(array.elementType());
                return new io.github.rawwise.model.record.ArrayType(
                        elementType, true, "Spark array type");
            }
            case MapType map -> {
                RawWiseType keyType = toRawWiseType(map.keyType());
                RawWiseType valueType = toRawWiseType(map.valueType());
                return new io.github.rawwise.model.record.MapType(
                        keyType, valueType, true, "Spark map type");
            }
            case StructType struct -> {
                List<io.github.rawwise.model.record.StructField> fields =
                        Arrays.stream(struct.fields())
                                .map(this::convertSparkField)
                                .collect(Collectors.toList());
                return new io.github.rawwise.model.record.StructType(
                        fields, true, "Spark struct type");
            }
            case null, default ->
                    throw new IllegalArgumentException("Unsupported Spark type: " + sparkType);
        }
    }

    @Override
    public DataType fromRawWiseType(RawWiseType dataType) {
        return dataType.accept(new RawWiseTypeToSparkTypeVisitor());
    }

    private io.github.rawwise.model.record.StructField convertSparkField(StructField sparkField) {
        RawWiseType dataType = toRawWiseType(sparkField.dataType());
        String comment = sparkField.getComment().isDefined() ? sparkField.getComment().get() : "";

        // extract metadata
        java.util.Map<String, String> metadata = new java.util.HashMap<>();
        metadata.put("nullable", String.valueOf(sparkField.nullable()));
        if (!comment.isEmpty()) {
            metadata.put("comment", comment);
        }

        return new io.github.rawwise.model.record.StructField(
                sparkField.name(), dataType, comment, metadata);
    }

    private static class RawWiseTypeToSparkTypeVisitor
            implements TypeVisitor<org.apache.spark.sql.types.DataType> {

        @Override
        public DataType visitNull(NullType type) {
            return DataTypes.NullType;
        }

        @Override
        public DataType visit(io.github.rawwise.model.record.BooleanType type) {
            return DataTypes.BooleanType;
        }

        @Override
        public DataType visit(Int8Type type) {
            return DataTypes.ByteType;
        }

        @Override
        public DataType visit(Int32Type type) {
            return DataTypes.IntegerType;
        }

        @Override
        public DataType visit(Int64Type type) {
            return DataTypes.LongType;
        }

        @Override
        public DataType visit(Float32Type type) {
            return DataTypes.FloatType;
        }

        @Override
        public DataType visit(Float64Type type) {
            return DataTypes.DoubleType;
        }

        @Override
        public DataType visit(io.github.rawwise.model.record.DecimalType type) {
            return DataTypes.createDecimalType(type.precision(), type.scale());
        }

        @Override
        public DataType visit(io.github.rawwise.model.record.StringType type) {
            return DataTypes.StringType;
        }

        @Override
        public DataType visit(io.github.rawwise.model.record.BinaryType type) {
            return DataTypes.BinaryType;
        }

        @Override
        public DataType visit(io.github.rawwise.model.record.DateType type) {
            return DataTypes.DateType;
        }

        @Override
        public DataType visit(io.github.rawwise.model.record.TimestampType type) {
            return DataTypes.TimestampType;
        }

        @Override
        public DataType visit(io.github.rawwise.model.record.ArrayType type) {
            DataType elementType = type.elementType().accept(this);
            return DataTypes.createArrayType(elementType, type.isNullable());
        }

        @Override
        public DataType visit(io.github.rawwise.model.record.MapType type) {
            DataType keyType = type.keyType().accept(this);
            DataType valueType = type.valueType().accept(this);
            return DataTypes.createMapType(keyType, valueType, type.isNullable());
        }

        @Override
        public DataType visit(io.github.rawwise.model.record.StructType type) {
            StructField[] sparkFields =
                    type.fields().stream()
                            .map(this::convertRawWiseField)
                            .toArray(StructField[]::new);
            return DataTypes.createStructType(sparkFields);
        }

        private StructField convertRawWiseField(io.github.rawwise.model.record.StructField field) {
            DataType sparkType = field.dataType().accept(this);
            MetadataBuilder metadataBuilder = new org.apache.spark.sql.types.MetadataBuilder();

            if (!field.comment().isEmpty()) {
                metadataBuilder.putString("comment", field.comment());
            }

            field.metadata()
                    .forEach(
                            (key, value) -> {
                                if (!key.equals("comment")) {
                                    metadataBuilder.putString(key, value);
                                }
                            });

            return new org.apache.spark.sql.types.StructField(
                    field.name(),
                    sparkType,
                    field.dataType().isNullable(),
                    metadataBuilder.build());
        }
    }
}
