package io.github.rawwise.model.record;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.rawwise.meta.PbColumnGroupDef;
import io.github.rawwise.meta.PbDataType;
import io.github.rawwise.meta.PbMetaArrayType;
import io.github.rawwise.meta.PbMetaMapType;
import io.github.rawwise.meta.PbMetaStructType;
import io.github.rawwise.schema.proto.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ProtoTypeConverter implements TypeConverter<PbType> {

    private ProtoTypeConverter() {}

    private static final ProtoTypeConverter INSTANCE = new ProtoTypeConverter();

    public static ProtoTypeConverter getInstance() {
        return INSTANCE;
    }

    // ==================== Type Converter<Pb Type> ====================

    @Override
    public RawWiseType toRawWiseType(PbType externalType) {
        return fromProto(externalType);
    }

    @Override
    public PbType fromRawWiseType(RawWiseType dataType) {
        return toProto(dataType);
    }

    // ==================== Schema convert ====================

    public static PbTypeSchema toProto(Schema schema) {
        var builder =
                PbTypeSchema.newBuilder()
                        .setStructType(toProtoStruct(schema.structType()))
                        .setVersion(schema.version());
        if (!schema.properties().isEmpty()) {
            builder.putAllProperties(schema.properties());
        }
        for (var constraint : schema.constraints()) {
            builder.addConstraints(toProto(constraint));
        }
        Map<String, StructField> fieldByName = new HashMap<>();
        for (StructField f : schema.structType().fields()) {
            if (f.hasAssignedFieldId()) {
                fieldByName.put(f.name(), f);
            }
        }
        for (var cg : schema.columnGroups()) {
            var cgBuilder =
                    PbColumnGroupDef.newBuilder()
                            .setName(cg.name())
                            .addAllColumnNames(cg.columnNames())
                            .setEncoding(cg.encoding().name())
                            .setSchemaVersion(cg.schemaVersion());
            for (RawWiseType type : cg.columnTypes()) {
                cgBuilder.addColumnTypes(toPbDataType(type));
            }
            if (!cg.columnFieldIds().isEmpty()) {
                cgBuilder.addAllColumnFieldIds(cg.columnFieldIds());
            } else {
                for (String colName : cg.columnNames()) {
                    StructField f = fieldByName.get(colName);
                    if (f != null) {
                        cgBuilder.addColumnFieldIds(f.fieldId());
                    }
                }
            }
            builder.addColumnGroups(cgBuilder.build());
        }
        return builder.build();
    }

    public static Schema fromProto(PbTypeSchema proto) {
        List<Constraint> constraints =
                proto.getConstraintsList().stream()
                        .map(ProtoTypeConverter::fromProtoConstraint)
                        .collect(Collectors.toList());
        List<ColumnGroup> columnGroups =
                proto.getColumnGroupsList().stream()
                        .map(
                                cgDef -> {
                                    List<RawWiseType> columnTypes =
                                            cgDef.getColumnTypesList().stream()
                                                    .map(ProtoTypeConverter::fromPbDataType)
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
        return new Schema(
                fromProto(proto.getStructType()),
                Map.copyOf(proto.getPropertiesMap()),
                proto.getVersion(),
                constraints,
                columnGroups);
    }

    public static byte[] toBytes(Schema schema) {
        return toProto(schema).toByteArray();
    }

    public static Schema fromBytes(byte[] data) {
        try {
            return fromProto(PbTypeSchema.parseFrom(data));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to deserialize PbSchema", e);
        }
    }

    // ==================== constraintconvert ====================

    public static PbConstraint toProto(Constraint constraint) {
        var builder = PbConstraint.newBuilder();
        if (constraint instanceof PrimaryKeyConstraint pk) {
            builder.setPrimaryKey(toProto(pk));
        }
        return builder.build();
    }

    private static PbPrimaryKey toProto(PrimaryKeyConstraint pk) {
        var builder =
                PbPrimaryKey.newBuilder()
                        .addAllFieldNames(pk.fieldNames())
                        .setEnforced(pk.enforced());
        if (pk.name() != null && !pk.name().isBlank()) {
            builder.setName(pk.name());
        }
        if (pk.description() != null && !pk.description().isBlank()) {
            builder.setDescription(pk.description());
        }
        return builder.build();
    }

    private static Constraint fromProtoConstraint(PbConstraint proto) {
        switch (proto.getKindCase()) {
            case PRIMARY_KEY:
                return fromProto(proto.getPrimaryKey());
            default:
                throw new IllegalArgumentException(
                        "Unknown constraint kind: " + proto.getKindCase());
        }
    }

    private static PrimaryKeyConstraint fromProto(PbPrimaryKey proto) {
        String name = proto.hasName() ? proto.getName() : null;
        String description = proto.hasDescription() ? proto.getDescription() : null;
        return new PrimaryKeyConstraint(
                name, proto.getFieldNamesList(), proto.getEnforced(), description);
    }

    // ==================== typeconvert(Raw Wise Type ↔ Pb Type)====================

    public static PbType toProto(RawWiseType type) {
        return type.accept(new ToProtoVisitor());
    }

    public static RawWiseType fromProto(PbType proto) {
        switch (proto.getKindCase()) {
            case PRIMITIVE:
                return fromPrimitive(proto.getPrimitive());
            case ARRAY:
                return fromArray(proto.getArray());
            case MAP:
                return fromMap(proto.getMap());
            case STRUCT:
                return fromStruct(proto.getStruct());
            default:
                throw new IllegalArgumentException("Unknown PbType kind: " + proto.getKindCase());
        }
    }

    private static class ToProtoVisitor implements TypeVisitor<PbType> {

        @Override
        public PbType visitNull(NullType t) {
            var pb = PbPrimitive.newBuilder().setKind(PbPrimitiveKind.NULL).setIsNullable(true);
            return PbType.newBuilder().setPrimitive(pb).build();
        }

        @Override
        public PbType visit(BooleanType t) {
            return wrapPrimitive(PbPrimitiveKind.BOOLEAN, t);
        }

        @Override
        public PbType visit(Int8Type t) {
            return wrapPrimitive(PbPrimitiveKind.INT8, t);
        }

        @Override
        public PbType visit(Int32Type t) {
            return wrapPrimitive(PbPrimitiveKind.INT32, t);
        }

        @Override
        public PbType visit(Int64Type t) {
            return wrapPrimitive(PbPrimitiveKind.INT64, t);
        }

        @Override
        public PbType visit(Float32Type t) {
            return wrapPrimitive(PbPrimitiveKind.FLOAT32, t);
        }

        @Override
        public PbType visit(Float64Type t) {
            return wrapPrimitive(PbPrimitiveKind.FLOAT64, t);
        }

        @Override
        public PbType visit(DecimalType t) {
            var pb =
                    PbPrimitive.newBuilder()
                            .setKind(PbPrimitiveKind.DECIMAL)
                            .setIsNullable(t.isNullable())
                            .setPrecision(t.precision())
                            .setScale(t.scale());
            if (t.description() != null) pb.setDescription(t.description());
            return PbType.newBuilder().setPrimitive(pb).build();
        }

        @Override
        public PbType visit(StringType t) {
            return wrapPrimitive(PbPrimitiveKind.STRING, t);
        }

        @Override
        public PbType visit(BinaryType t) {
            return wrapPrimitive(PbPrimitiveKind.BINARY, t);
        }

        @Override
        public PbType visit(DateType t) {
            return wrapPrimitive(PbPrimitiveKind.DATE, t);
        }

        @Override
        public PbType visit(TimestampType t) {
            return wrapPrimitive(PbPrimitiveKind.TIMESTAMP, t);
        }

        @Override
        public PbType visit(ArrayType t) {
            var pb =
                    PbArray.newBuilder()
                            .setElementType(toProto(t.elementType()))
                            .setIsNullable(t.isNullable())
                            .setFixedSize(t.fixedSize());
            if (t.description() != null) pb.setDescription(t.description());
            return PbType.newBuilder().setArray(pb).build();
        }

        @Override
        public PbType visit(MapType t) {
            var pb =
                    PbMap.newBuilder()
                            .setKeyType(toProto(t.keyType()))
                            .setValueType(toProto(t.valueType()))
                            .setIsNullable(t.isNullable());
            if (t.description() != null) pb.setDescription(t.description());
            return PbType.newBuilder().setMap(pb).build();
        }

        @Override
        public PbType visit(StructType t) {
            var fields =
                    t.fields().stream()
                            .map(ProtoTypeConverter::toProto)
                            .collect(Collectors.toList());
            var pb = PbStruct.newBuilder().addAllFields(fields).setIsNullable(t.isNullable());
            if (t.description() != null) pb.setDescription(t.description());
            return PbType.newBuilder().setStruct(pb).build();
        }

        private PbType wrapPrimitive(PbPrimitiveKind kind, PrimitiveType t) {
            var pb = PbPrimitive.newBuilder().setKind(kind).setIsNullable(t.isNullable());
            if (t.description() != null) pb.setDescription(t.description());
            return PbType.newBuilder().setPrimitive(pb).build();
        }
    }

    // ==================== Struct Field convert ====================

    public static PbStructField toProto(StructField field) {
        var builder =
                PbStructField.newBuilder()
                        .setName(field.name())
                        .setDataType(toProto(field.dataType()));
        if (field.hasAssignedFieldId()) {
            builder.setFieldId(field.fieldId());
        }
        if (field.comment() != null && !field.comment().isEmpty()) {
            builder.setComment(field.comment());
        }
        if (field.metadata() != null && !field.metadata().isEmpty()) {
            builder.putAllMetadata(field.metadata());
        }
        return builder.build();
    }

    public static PbStruct toProtoStruct(StructType structType) {
        var fields =
                structType.fields().stream()
                        .map(ProtoTypeConverter::toProto)
                        .collect(Collectors.toList());
        var builder =
                PbStruct.newBuilder().addAllFields(fields).setIsNullable(structType.isNullable());
        if (structType.description() != null) builder.setDescription(structType.description());
        return builder.build();
    }

    public static StructField fromProto(PbStructField proto) {
        String comment = proto.hasComment() ? proto.getComment() : "";
        Map<String, String> metadata =
                proto.getMetadataMap().isEmpty() ? Map.of() : Map.copyOf(proto.getMetadataMap());
        int fieldId = proto.hasFieldId() ? proto.getFieldId() : StructField.UNASSIGNED_FIELD_ID;
        return new StructField(
                fieldId, proto.getName(), fromProto(proto.getDataType()), comment, metadata);
    }

    // ==================== typeconvert ====================

    private static RawWiseType fromPrimitive(PbPrimitive proto) {
        boolean nullable = proto.getIsNullable();
        String desc = proto.hasDescription() ? proto.getDescription() : null;
        return switch (proto.getKind()) {
            case NULL -> NullType.INSTANCE;
            case BOOLEAN -> new BooleanType(nullable, desc);
            case INT8 -> new Int8Type(nullable, desc);
            case INT32 -> new Int32Type(nullable, desc);
            case INT64 -> new Int64Type(nullable, desc);
            case FLOAT32 -> new Float32Type(nullable, desc);
            case FLOAT64 -> new Float64Type(nullable, desc);
            case DECIMAL -> {
                if (!proto.hasPrecision()) {
                    throw new IllegalArgumentException(
                            "DECIMAL type missing precision field in PbPrimitive");
                }
                int scale = proto.hasScale() ? proto.getScale() : 0;
                yield new DecimalType(proto.getPrecision(), scale, nullable, desc);
            }
            case STRING -> new StringType(nullable, desc);
            case BINARY -> new BinaryType(nullable, desc);
            case DATE -> new DateType(nullable, desc);
            case TIMESTAMP -> new TimestampType(nullable, desc);
            default ->
                    throw new IllegalArgumentException(
                            "Unknown primitive kind: " + proto.getKind());
        };
    }

    private static ArrayType fromArray(PbArray proto) {
        String desc = proto.hasDescription() ? proto.getDescription() : null;
        return new ArrayType(
                fromProto(proto.getElementType()),
                proto.getFixedSize(),
                proto.getIsNullable(),
                desc);
    }

    private static MapType fromMap(PbMap proto) {
        String desc = proto.hasDescription() ? proto.getDescription() : null;
        return new MapType(
                fromProto(proto.getKeyType()),
                fromProto(proto.getValueType()),
                proto.getIsNullable(),
                desc);
    }

    private static StructType fromStruct(PbStruct proto) {
        List<StructField> fields =
                proto.getFieldsList().stream()
                        .map(ProtoTypeConverter::fromProto)
                        .collect(Collectors.toList());
        String desc = proto.hasDescription() ? proto.getDescription() : null;
        return new StructType(fields, proto.getIsNullable(), desc);
    }

    private static StructType fromProto(PbStruct proto) {
        return fromStruct(proto);
    }

    static PbDataType toPbDataType(RawWiseType type) {
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
            for (StructField f : st.fields()) {
                var fb =
                        io.github.rawwise.meta.PbStructField.newBuilder()
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
                builder.setPrimitiveType(toMetaPrimitiveType(typeName));
            }
        }
        return builder.build();
    }

    static RawWiseType fromPbDataType(PbDataType pb) {
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
                List<StructField> fields = new java.util.ArrayList<>();
                for (io.github.rawwise.meta.PbStructField f : pb.getStructType().getFieldsList()) {
                    int fid = f.hasFieldId() ? f.getFieldId() : StructField.UNASSIGNED_FIELD_ID;
                    String cmt = f.hasComment() ? f.getComment() : "";
                    Map<String, String> meta =
                            f.getMetadataMap().isEmpty()
                                    ? Map.of()
                                    : Map.copyOf(f.getMetadataMap());
                    fields.add(
                            new StructField(
                                    fid,
                                    f.getFieldName(),
                                    fromPbDataType(f.getDataType()),
                                    cmt,
                                    meta));
                }
                return new StructType(fields, nullable, null);
            case PRIMITIVE_TYPE:
                return switch (pb.getPrimitiveType()) {
                    case BOOLEAN -> Types.bool(nullable);
                    case INT8 -> Types.int8(nullable);
                    case INT32 -> Types.int32(nullable);
                    case INT64 -> Types.int64(nullable);
                    case FLOAT32 -> Types.float32(nullable);
                    case FLOAT64 -> Types.float64(nullable);
                    case DECIMAL -> {
                        if (!pb.hasPrecision()) {
                            throw new IllegalArgumentException(
                                    "DECIMAL type missing precision field in PbDataType");
                        }
                        int scale = pb.hasScale() ? pb.getScale() : 0;
                        yield Types.decimal(pb.getPrecision(), scale, nullable);
                    }
                    case STRING -> Types.string(nullable);
                    case BINARY -> Types.binary(nullable);
                    case DATE -> Types.date(nullable);
                    case TIMESTAMP -> Types.timestamp(nullable);
                    default ->
                            throw new IllegalArgumentException(
                                    "Unknown primitive type: " + pb.getPrimitiveType());
                };
            default:
                throw new IllegalArgumentException("PbDataType kind not set");
        }
    }

    private static io.github.rawwise.meta.PrimitiveType toMetaPrimitiveType(String typeName) {
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
}
