package io.github.rawwise.model.record;

import com.google.protobuf.Timestamp;
import io.github.rawwise.meta.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ProtoTableMetaConverter {

    private ProtoTableMetaConverter() {}

    public static PbTableMetadata toPb(TableMeta meta) {
        PbTableMetadata.Builder builder =
                PbTableMetadata.newBuilder()
                        .setIdentifier(
                                PbTableIdentifier.newBuilder()
                                        .setCatalog(meta.catalogName())
                                        .setDatabase(meta.databaseName())
                                        .setTable(meta.tableName())
                                        .build())
                        .setTableId(meta.tableId())
                        .setTableType(toPbType(meta.tableType()))
                        .setSchema(toPbSchema(meta.schema()));

        if (meta.primaryKey() != null) {
            builder.setPrimaryKey(
                    PbPrimaryKeyConstraint.newBuilder()
                            .addAllFieldNames(meta.primaryKey().fields()));
        }

        if (meta.partitionSpec() != null) {
            builder.setPartitionSpec(toPbPartitionSpec(meta.partitionSpec()));
        }

        if (meta.hiddenColumnConfig() != null) {
            builder.setHiddenColumns(toPbHiddenColumnConfig(meta.hiddenColumnConfig()));
        }

        builder.putAllProperties(meta.properties());

        if (meta.location() != null && !meta.location().isEmpty()) {
            builder.setLocation(meta.location());
        }

        if (meta.createdAt() != null) {
            builder.setCreatedAt(toTimestamp(meta.createdAt()));
        }

        if (meta.updatedAt() != null) {
            builder.setUpdatedAt(toTimestamp(meta.updatedAt()));
        }

        return builder.build();
    }

    public static TableMeta fromPb(PbTableMetadata pb) {
        TableMeta.Builder builder =
                TableMeta.builder()
                        .withTableId(pb.getTableId())
                        .withCatalogName(pb.getIdentifier().getCatalog())
                        .withDatabaseName(pb.getIdentifier().getDatabase())
                        .withTableName(pb.getIdentifier().getTable())
                        .withTableType(fromPbType(pb.getTableType()))
                        .withSchema(fromPbSchema(pb.getSchema()));

        if (pb.hasPrimaryKey()) {
            builder.withPrimaryKey(
                    new PrimaryKey(new ArrayList<>(pb.getPrimaryKey().getFieldNamesList())));
        }

        if (pb.hasPartitionSpec()) {
            builder.withPartitionSpec(fromPbPartitionSpec(pb.getPartitionSpec()));
        }

        if (pb.hasHiddenColumns()) {
            builder.withHiddenColumnConfig(fromPbHiddenColumnConfig(pb.getHiddenColumns()));
        }

        if (!pb.getPropertiesMap().isEmpty()) {
            builder.withProperties(new HashMap<>(pb.getPropertiesMap()));
        }

        String location = pb.getLocation();
        if (location != null && !location.isEmpty()) {
            builder.withLocation(location);
        }

        if (pb.hasCreatedAt()) {
            builder.withCreatedAt(fromTimestamp(pb.getCreatedAt()));
        }

        if (pb.hasUpdatedAt()) {
            builder.withUpdatedAt(fromTimestamp(pb.getUpdatedAt()));
        }

        return builder.build();
    }

    public static PbSchema toPbSchema(io.github.rawwise.model.record.Schema schema) {
        PbSchema.Builder builder =
                PbSchema.newBuilder()
                        .setStructType(toPbStructType(schema.structType()))
                        .setVersion(schema.version());

        if (!schema.properties().isEmpty()) {
            builder.putAllProperties(schema.properties());
        }

        for (io.github.rawwise.model.record.Constraint constraint : schema.constraints()) {
            if (constraint instanceof io.github.rawwise.model.record.PrimaryKeyConstraint pk) {
                builder.addConstraints(
                        PbPrimaryKeyConstraint.newBuilder().addAllFieldNames(pk.fieldNames()));
            }
        }

        Map<String, StructField> fieldByName = new HashMap<>();
        for (StructField f : schema.structType().fields()) {
            if (f.hasAssignedFieldId()) {
                fieldByName.put(f.name(), f);
            }
        }
        for (ColumnGroup cg : schema.columnGroups()) {
            PbColumnGroupDef.Builder cgDefBuilder =
                    PbColumnGroupDef.newBuilder()
                            .setName(cg.name())
                            .addAllColumnNames(cg.columnNames())
                            .setEncoding(cg.encoding().name())
                            .setSchemaVersion(cg.schemaVersion());
            for (RawWiseType type : cg.columnTypes()) {
                cgDefBuilder.addColumnTypes(toPbDataType(type));
            }
            if (!cg.columnFieldIds().isEmpty()) {
                cgDefBuilder.addAllColumnFieldIds(cg.columnFieldIds());
            } else {
                for (String colName : cg.columnNames()) {
                    StructField f = fieldByName.get(colName);
                    if (f != null) {
                        cgDefBuilder.addColumnFieldIds(f.fieldId());
                    }
                }
            }
            builder.addColumnGroups(cgDefBuilder.build());
        }

        return builder.build();
    }

    public static io.github.rawwise.model.record.Schema fromPbSchema(PbSchema pb) {
        List<io.github.rawwise.model.record.Constraint> constraints = new ArrayList<>();
        for (PbPrimaryKeyConstraint pkc : pb.getConstraintsList()) {
            constraints.add(
                    new io.github.rawwise.model.record.PrimaryKeyConstraint(
                            new ArrayList<>(pkc.getFieldNamesList())));
        }

        List<ColumnGroup> columnGroups =
                pb.getColumnGroupsList().stream()
                        .map(
                                cgDef -> {
                                    List<RawWiseType> columnTypes =
                                            cgDef.getColumnTypesList().stream()
                                                    .map(ProtoTableMetaConverter::fromPbDataType)
                                                    .toList();
                                    List<Integer> columnFieldIds =
                                            cgDef.getColumnFieldIdsList().isEmpty()
                                                    ? List.of()
                                                    : cgDef.getColumnFieldIdsList();
                                    return new ColumnGroup(
                                            cgDef.getName(),
                                            cgDef.getColumnNamesList(),
                                            columnTypes.isEmpty() ? null : columnTypes,
                                            ColumnGroup.Encoding.valueOf(cgDef.getEncoding()),
                                            cgDef.getSchemaVersion(),
                                            columnFieldIds);
                                })
                        .toList();

        return new io.github.rawwise.model.record.Schema(
                fromPbStructType(pb.getStructType()),
                new HashMap<>(pb.getPropertiesMap()),
                pb.getVersion(),
                constraints,
                columnGroups);
    }

    public static PbStructType toPbStructType(
            io.github.rawwise.model.record.StructType structType) {
        PbStructType.Builder builder = PbStructType.newBuilder();
        for (io.github.rawwise.model.record.StructField field : structType.fields()) {
            var fieldBuilder =
                    PbStructField.newBuilder()
                            .setFieldName(field.name())
                            .setDataType(toPbDataType(field.dataType()));
            if (field.comment() != null && !field.comment().isEmpty()) {
                fieldBuilder.setComment(field.comment());
            }
            if (field.hasAssignedFieldId()) {
                fieldBuilder.setFieldId(field.fieldId());
            }
            if (field.metadata() != null && !field.metadata().isEmpty()) {
                fieldBuilder.putAllMetadata(field.metadata());
            }
            builder.addFields(fieldBuilder);
        }
        return builder.build();
    }

    public static io.github.rawwise.model.record.StructType fromPbStructType(PbStructType pb) {
        List<io.github.rawwise.model.record.StructField> fields = new ArrayList<>();
        for (PbStructField f : pb.getFieldsList()) {
            String comment = f.hasComment() ? f.getComment() : "";
            int fieldId =
                    f.hasFieldId()
                            ? f.getFieldId()
                            : io.github.rawwise.model.record.StructField.UNASSIGNED_FIELD_ID;
            Map<String, String> metadata =
                    f.getMetadataMap().isEmpty() ? Map.of() : Map.copyOf(f.getMetadataMap());
            fields.add(
                    new io.github.rawwise.model.record.StructField(
                            fieldId,
                            f.getFieldName(),
                            fromPbDataType(f.getDataType()),
                            comment,
                            metadata));
        }
        return new io.github.rawwise.model.record.StructType(fields);
    }

    private static PbDataType toPbDataType(io.github.rawwise.model.record.RawWiseType type) {
        PbDataType.Builder builder = PbDataType.newBuilder().setIsNullable(type.isNullable());

        if (type instanceof ArrayType at) {
            builder.setArrayType(
                    PbMetaArrayType.newBuilder()
                            .setElementType(toPbDataType(at.elementType()))
                            .setFixedSize(at.fixedSize()));
        } else if (type instanceof MapType mt) {
            builder.setMapType(
                    PbMetaMapType.newBuilder()
                            .setKeyType(toPbDataType(mt.keyType()))
                            .setValueType(toPbDataType(mt.valueType())));
        } else if (type instanceof StructType st) {
            PbMetaStructType.Builder structBuilder = PbMetaStructType.newBuilder();
            for (io.github.rawwise.model.record.StructField f : st.fields()) {
                var fb =
                        PbStructField.newBuilder()
                                .setFieldName(f.name())
                                .setDataType(toPbDataType(f.dataType()));
                if (f.hasAssignedFieldId()) fb.setFieldId(f.fieldId());
                if (f.comment() != null && !f.comment().isEmpty()) fb.setComment(f.comment());
                if (f.metadata() != null && !f.metadata().isEmpty())
                    fb.putAllMetadata(f.metadata());
                structBuilder.addFields(fb);
            }
            builder.setStructType(structBuilder);
        } else {
            String typeName = type.name();
            if (typeName.startsWith("decimal")) {
                builder.setPrimitiveType(io.github.rawwise.meta.PrimitiveType.DECIMAL);
                String inner = typeName.substring("decimal(".length(), typeName.length() - 1);
                String[] parts = inner.split(",");
                builder.setPrecision(Integer.parseInt(parts[0].trim()));
                builder.setScale(Integer.parseInt(parts[1].trim()));
            } else {
                builder.setPrimitiveType(toPbPrimitiveType(typeName));
            }
        }

        return builder.build();
    }

    private static io.github.rawwise.model.record.RawWiseType fromPbDataType(PbDataType pb) {
        boolean nullable = pb.getIsNullable();

        switch (pb.getKindCase()) {
            case ARRAY_TYPE:
                return new ArrayType(
                        fromPbDataType(pb.getArrayType().getElementType()),
                        pb.getArrayType().getFixedSize(),
                        nullable);
            case MAP_TYPE:
                return new MapType(
                        fromPbDataType(pb.getMapType().getKeyType()),
                        fromPbDataType(pb.getMapType().getValueType()),
                        nullable,
                        null);
            case STRUCT_TYPE:
                java.util.List<io.github.rawwise.model.record.StructField> fields =
                        new java.util.ArrayList<>();
                for (PbStructField f : pb.getStructType().getFieldsList()) {
                    int fid =
                            f.hasFieldId()
                                    ? f.getFieldId()
                                    : io.github.rawwise.model.record.StructField
                                            .UNASSIGNED_FIELD_ID;
                    String cmt = f.hasComment() ? f.getComment() : "";
                    java.util.Map<String, String> meta =
                            f.getMetadataMap().isEmpty()
                                    ? java.util.Map.of()
                                    : java.util.Map.copyOf(f.getMetadataMap());
                    fields.add(
                            new io.github.rawwise.model.record.StructField(
                                    fid,
                                    f.getFieldName(),
                                    fromPbDataType(f.getDataType()),
                                    cmt,
                                    meta));
                }
                return new StructType(fields, nullable, null);
            case PRIMITIVE_TYPE:
                return switch (pb.getPrimitiveType()) {
                    case BOOLEAN -> io.github.rawwise.model.record.Types.bool(nullable);
                    case INT8 -> io.github.rawwise.model.record.Types.int8(nullable);
                    case INT32 -> io.github.rawwise.model.record.Types.int32(nullable);
                    case INT64 -> io.github.rawwise.model.record.Types.int64(nullable);
                    case FLOAT32 -> io.github.rawwise.model.record.Types.float32(nullable);
                    case FLOAT64 -> io.github.rawwise.model.record.Types.float64(nullable);
                    case DECIMAL -> {
                        if (!pb.hasPrecision()) {
                            throw new IllegalArgumentException(
                                    "DECIMAL type missing precision field in PbDataType");
                        }
                        int scale = pb.hasScale() ? pb.getScale() : 0;
                        yield io.github.rawwise.model.record.Types.decimal(
                                pb.getPrecision(), scale, nullable);
                    }
                    case STRING -> io.github.rawwise.model.record.Types.string(nullable);
                    case BINARY -> io.github.rawwise.model.record.Types.binary(nullable);
                    case DATE -> io.github.rawwise.model.record.Types.date(nullable);
                    case TIMESTAMP -> io.github.rawwise.model.record.Types.timestamp(nullable);
                    default ->
                            throw new IllegalArgumentException(
                                    "Unknown primitive type: " + pb.getPrimitiveType());
                };
            default:
                throw new IllegalArgumentException("PbDataType kind not set");
        }
    }

    private static io.github.rawwise.meta.PrimitiveType toPbPrimitiveType(String typeName) {
        return switch (typeName) {
            case "boolean" -> io.github.rawwise.meta.PrimitiveType.BOOLEAN;
            case "int8" -> io.github.rawwise.meta.PrimitiveType.INT8;
            case "int32" -> io.github.rawwise.meta.PrimitiveType.INT32;
            case "int64" -> io.github.rawwise.meta.PrimitiveType.INT64;
            case "float32" -> io.github.rawwise.meta.PrimitiveType.FLOAT32;
            case "float64" -> io.github.rawwise.meta.PrimitiveType.FLOAT64;
            case "string" -> io.github.rawwise.meta.PrimitiveType.STRING;
            case "binary" -> io.github.rawwise.meta.PrimitiveType.BINARY;
            case "date" -> io.github.rawwise.meta.PrimitiveType.DATE;
            case "timestamp" -> io.github.rawwise.meta.PrimitiveType.TIMESTAMP;
            default -> throw new IllegalArgumentException("Unknown type: " + typeName);
        };
    }

    private static TableType toPbType(TableMeta.TableType type) {
        return switch (type) {
            case PRIMARY_KEY -> TableType.PRIMARY_KEY;
            case APPEND_ONLY -> TableType.APPEND_ONLY;
        };
    }

    private static TableMeta.TableType fromPbType(TableType type) {
        return switch (type) {
            case PRIMARY_KEY -> TableMeta.TableType.PRIMARY_KEY;
            case APPEND_ONLY -> TableMeta.TableType.APPEND_ONLY;
            default -> throw new IllegalArgumentException("Unknown table type: " + type);
        };
    }

    private static PbPartitionSpec toPbPartitionSpec(PartitionSpec spec) {
        PbPartitionSpec.Builder builder =
                PbPartitionSpec.newBuilder().setPartitionFieldsCount(spec.numLevels());
        for (PartitionSpec.BucketSpec bucketSpec : spec.bucketSpecs()) {
            builder.addBucketSpecs(
                    PbBucketSpec.newBuilder()
                            .setSourceColumn(
                                    bucketSpec.sourceColumn() != null
                                            ? bucketSpec.sourceColumn()
                                            : "")
                            .setTransform(toPbTransform(bucketSpec.transform()))
                            .setNumBuckets(bucketSpec.numBuckets())
                            .setHidden(bucketSpec.hidden()));
        }
        return builder.build();
    }

    private static PartitionSpec fromPbPartitionSpec(PbPartitionSpec pb) {
        PartitionSpec.Builder builder = PartitionSpec.builder();
        for (PbBucketSpec bs : pb.getBucketSpecsList()) {
            builder.withBucketSpec(
                    new PartitionSpec.BucketSpec(
                            bs.getSourceColumn().isEmpty() ? null : bs.getSourceColumn(),
                            fromPbTransform(bs.getTransform()),
                            bs.getNumBuckets(),
                            bs.getHidden()));
        }
        return builder.build();
    }

    private static BucketTransform toPbTransform(PartitionSpec.Transform transform) {
        return switch (transform) {
            case IDENTITY -> BucketTransform.IDENTITY;
            case HASH -> BucketTransform.HASH;
            case RANGE -> BucketTransform.RANGE;
            case VALUE -> BucketTransform.VALUE;
        };
    }

    private static PartitionSpec.Transform fromPbTransform(BucketTransform transform) {
        return switch (transform) {
            case IDENTITY -> PartitionSpec.Transform.IDENTITY;
            case HASH -> PartitionSpec.Transform.HASH;
            case RANGE -> PartitionSpec.Transform.RANGE;
            case VALUE -> PartitionSpec.Transform.VALUE;
            default -> throw new IllegalArgumentException("Unknown transform: " + transform);
        };
    }

    private static PbHiddenColumnConfig toPbHiddenColumnConfig(HiddenColumnConfig config) {
        PbHiddenColumnConfig.Builder builder = PbHiddenColumnConfig.newBuilder();
        for (HiddenColumnConfig.HiddenColumnDef def : config.columns()) {
            builder.addColumns(
                    PbHiddenColumnDef.newBuilder()
                            .setType(toPbColumnType(def.type()))
                            .setColumnName(def.columnName())
                            .setVisible(def.visible()));
        }
        return builder.build();
    }

    private static HiddenColumnConfig fromPbHiddenColumnConfig(PbHiddenColumnConfig pb) {
        HiddenColumnConfig.Builder builder = HiddenColumnConfig.builder();
        for (PbHiddenColumnDef def : pb.getColumnsList()) {
            builder.withColumn(
                    new HiddenColumnConfig.HiddenColumnDef(
                            fromPbColumnType(def.getType()),
                            def.getColumnName(),
                            null,
                            def.getVisible()));
        }
        return builder.build();
    }

    private static HiddenColumnType toPbColumnType(HiddenColumnConfig.ColumnType type) {
        return switch (type) {
            case ROW_NUMBER -> HiddenColumnType.ROW_NUMBER;
            case HIDDEN_PRIMARY_KEY -> HiddenColumnType.HIDDEN_PRIMARY_KEY;
            case COMMIT_TIMESTAMP -> HiddenColumnType.COMMIT_TIMESTAMP;
            case OPERATION_TYPE -> HiddenColumnType.OPERATION_TYPE;
        };
    }

    private static HiddenColumnConfig.ColumnType fromPbColumnType(HiddenColumnType type) {
        return switch (type) {
            case ROW_NUMBER -> HiddenColumnConfig.ColumnType.ROW_NUMBER;
            case HIDDEN_PRIMARY_KEY -> HiddenColumnConfig.ColumnType.HIDDEN_PRIMARY_KEY;
            case COMMIT_TIMESTAMP -> HiddenColumnConfig.ColumnType.COMMIT_TIMESTAMP;
            case OPERATION_TYPE -> HiddenColumnConfig.ColumnType.OPERATION_TYPE;
            default -> throw new IllegalArgumentException("Unknown hidden column type: " + type);
        };
    }

    public static PbSnapshot toPbSnapshot(Snapshot snapshot) {
        PbSnapshot.Builder builder =
                PbSnapshot.newBuilder()
                        .setSnapshotId(snapshot.snapshotId())
                        .setOperation(snapshot.operation())
                        .setTimestamp(snapshot.timestampMillis());
        if (snapshot.manifestList() != null && !snapshot.manifestList().isEmpty()) {
            builder.addAllManifestIds(snapshot.manifestList());
        }
        if (snapshot.summaryOptional().isPresent()) {
            builder.setSummary(snapshot.summaryOptional().get());
        }
        if (snapshot.schema() != null) {
            builder.setSchema(toPbSchema(snapshot.schema()));
        }
        return builder.build();
    }

    public static Snapshot fromPbSnapshot(PbSnapshot pb) {
        String summary = pb.getSummary().isEmpty() ? null : pb.getSummary();
        var schema = pb.hasSchema() ? fromPbSchema(pb.getSchema()) : null;
        return new Snapshot(
                pb.getSnapshotId(),
                pb.getManifestIdsList(),
                pb.getTimestamp(),
                summary,
                pb.getOperation(),
                schema,
                0,
                0);
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private static Instant fromTimestamp(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}
