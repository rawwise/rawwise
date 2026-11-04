package io.github.rawwise.model.record;

import java.util.List;
import java.util.Map;

public class SparkSchemaConverter {
    private static final SparkTypeConverter TYPE_CONVERTER = new SparkTypeConverter();

    public static Schema toRawWiseSchema(org.apache.spark.sql.types.StructType sparkSchema) {
        StructType structType = (StructType) TYPE_CONVERTER.toRawWiseType(sparkSchema);

        return new Schema(
                structType, extractSchemaProperties(sparkSchema), 1L, List.of(), List.of());
    }

    public static org.apache.spark.sql.types.StructType toSparkSchema(Schema dataLakeSchema) {
        return (org.apache.spark.sql.types.StructType)
                TYPE_CONVERTER.fromRawWiseType(dataLakeSchema.structType());
    }

    private static Map<String, String> extractSchemaProperties(
            org.apache.spark.sql.types.StructType sparkSchema) {
        Map<String, String> properties = new java.util.HashMap<>();

        properties.put("field_count", String.valueOf(sparkSchema.fields().length));
        properties.put("source", "spark");

        return properties;
    }

    public static org.apache.spark.sql.types.StructField createSparkField(
            String name, RawWiseType dataType, boolean nullable, String comment) {
        org.apache.spark.sql.types.DataType sparkType = TYPE_CONVERTER.fromRawWiseType(dataType);

        org.apache.spark.sql.types.MetadataBuilder metadataBuilder =
                new org.apache.spark.sql.types.MetadataBuilder();
        if (comment != null && !comment.isEmpty()) {
            metadataBuilder.putString("comment", comment);
        }

        return new org.apache.spark.sql.types.StructField(
                name, sparkType, nullable, metadataBuilder.build());
    }

    public static org.apache.spark.sql.types.StructField[] toSparkFields(
            List<StructField> dataLakeFields) {
        return dataLakeFields.stream()
                .map(
                        field ->
                                createSparkField(
                                        field.name(),
                                        field.dataType(),
                                        field.dataType().isNullable(),
                                        field.comment()))
                .toArray(org.apache.spark.sql.types.StructField[]::new);
    }
}
