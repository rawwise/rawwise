package io.github.rawwise.frontend.ir.serializer;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.rawwise.frontend.ir.*;
import io.github.rawwise.frontend.ir.DataVersionToken;
import io.github.rawwise.frontend.ir.proto.FrontendIrProto;
import io.github.rawwise.meta.PbLiteralValue;
import io.github.rawwise.meta.TableType;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.expression.serializer.ProtobufExpressionSerializer;
import io.github.rawwise.model.record.ProtoTypeConverter;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.StructType;
import io.github.rawwise.schema.proto.PbTypeSchema;
import java.util.*;

/**
 * Converts between QueryIR Java objects and Protobuf bytes.
 *
 * <p>Uses the existing {@link ProtobufExpressionSerializer} for Expression fields and {@link
 * ProtoTypeConverter} for Schema/StructType fields.
 */
public final class IRProtobufSerializer {

    private IRProtobufSerializer() {}

    // ==================== Public API ====================

    public static byte[] toProtoBytes(QueryIR ir) {
        return toProto(ir).toByteArray();
    }

    public static QueryIR fromProtoBytes(byte[] bytes) {
        try {
            return fromProto(FrontendIrProto.PbQueryIR.parseFrom(bytes));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to deserialize QueryIR", e);
        }
    }

    public static FrontendIrProto.PbQueryIR toProto(QueryIR ir) {
        var builder = FrontendIrProto.PbQueryIR.newBuilder().setType(toProtoIRType(ir.type()));

        // Set table_identifier from the first table identifier
        Set<String> tableIds = ir.tableIdentifiers();
        if (!tableIds.isEmpty()) {
            builder.setTableIdentifier(tableIds.iterator().next());
        }

        // Set table_meta and snapshot_id based on IR type
        switch (ir) {
            case PointQueryIR point -> {
                builder.setTableMeta(toProto(point.tableMeta()));
                builder.setSnapshotId(point.snapshot().baseSnapshotId());
                builder.setPoint(toProto(point));
            }
            case BatchPointQueryIR batch -> {
                builder.setTableMeta(toProto(batch.tableMeta()));
                builder.setSnapshotId(batch.snapshot().baseSnapshotId());
                builder.setBatchPoint(toProto(batch));
            }
            case MetadataQueryIR meta -> {
                builder.setTableMeta(toProto(meta.tableMeta()));
                builder.setSnapshotId(meta.snapshotId());
                builder.setMetadata(toProto(meta));
            }
            case ScanQueryIR scan -> {
                builder.setTableMeta(toProto(scan.tableMeta()));
                builder.setSnapshotId(scan.snapshotId());
                builder.setScan(toProto(scan));
            }
            case AggregateQueryIR agg -> {
                if (!agg.tableMetas().isEmpty()) {
                    builder.setTableMeta(toProto(agg.tableMetas().getFirst()));
                }
                builder.setSnapshotId(agg.snapshotId());
                builder.setAggregate(toProto(agg));
            }
            case JoinQueryIR join -> {
                if (!join.tableMetas().isEmpty()) {
                    builder.setTableMeta(toProto(join.tableMetas().getFirst()));
                }
                builder.setSnapshotId(join.snapshotId());
                builder.setJoin(toProto(join));
            }
            case WriteQueryIR write -> {
                builder.setTableMeta(toProto(write.tableMeta()));
                builder.setSnapshotId(0);
                builder.setWrite(toProto(write));
            }
            case UpdateRangeIR update -> {
                builder.setTableMeta(toProto(update.tableMeta()));
                builder.setSnapshotId(0);
                builder.setUpdateRange(toProto(update));
            }
            case DeleteRangeIR delete -> {
                builder.setTableMeta(toProto(delete.tableMeta()));
                builder.setSnapshotId(0);
                builder.setDeleteRange(toProto(delete));
            }
            case UpdateQueryIR update -> {
                builder.setTableMeta(toProto(update.tableMeta()));
                builder.setSnapshotId(update.snapshotId());
                builder.setPointUpdate(toProto(update));
            }
            case DeleteQueryIR delete -> {
                builder.setTableMeta(toProto(delete.tableMeta()));
                builder.setSnapshotId(delete.snapshotId());
                builder.setPointDelete(toProto(delete));
            }
            case DDLIR ddl -> {
                builder.setTableMeta(toProto(ddl.tableMeta()));
                builder.setSnapshotId(0);
                builder.setDdl(toProto(ddl));
            }
        }

        return builder.build();
    }

    public static QueryIR fromProto(FrontendIrProto.PbQueryIR proto) {
        FrontendIrProto.PbQueryIR.BodyCase bodyCase = proto.getBodyCase();
        return switch (bodyCase) {
            case POINT -> fromProtoPoint(proto);
            case BATCH_POINT -> fromProtoBatchPoint(proto);
            case METADATA -> fromProtoMetadata(proto);
            case SCAN -> fromProtoScan(proto);
            case AGGREGATE -> fromProtoAggregate(proto);
            case JOIN -> fromProtoJoin(proto);
            case WRITE -> fromProtoWrite(proto);
            case UPDATE_RANGE -> fromProtoUpdateRange(proto);
            case DELETE_RANGE -> fromProtoDeleteRange(proto);
            case POINT_UPDATE -> fromProtoPointUpdate(proto);
            case POINT_DELETE -> fromProtoPointDelete(proto);
            case DDL -> fromProtoDDL(proto);
            default -> throw new IllegalArgumentException("Unknown QueryIR body case: " + bodyCase);
        };
    }

    // ==================== IRType conversion ====================

    private static FrontendIrProto.IRType toProtoIRType(IRType type) {
        return switch (type) {
            case POINT_QUERY -> FrontendIrProto.IRType.POINT_QUERY;
            case BATCH_POINT_QUERY -> FrontendIrProto.IRType.BATCH_POINT_QUERY;
            case METADATA_COUNT_ALL -> FrontendIrProto.IRType.METADATA_COUNT_ALL;
            case METADATA_COUNT_COLUMN -> FrontendIrProto.IRType.METADATA_COUNT_COLUMN;
            case METADATA_MIN -> FrontendIrProto.IRType.METADATA_MIN;
            case METADATA_MAX -> FrontendIrProto.IRType.METADATA_MAX;
            case METADATA_TABLE_INFO -> FrontendIrProto.IRType.METADATA_TABLE_INFO;
            case SCAN -> FrontendIrProto.IRType.SCAN;
            case AGGREGATE -> FrontendIrProto.IRType.AGGREGATE;
            case JOIN -> FrontendIrProto.IRType.JOIN;
            case INSERT -> FrontendIrProto.IRType.INSERT;
            case BATCH_INSERT -> FrontendIrProto.IRType.BATCH_INSERT;
            case UPDATE_RANGE -> FrontendIrProto.IRType.UPDATE_RANGE;
            case DELETE_RANGE -> FrontendIrProto.IRType.DELETE_RANGE;
            case POINT_UPDATE -> FrontendIrProto.IRType.POINT_UPDATE;
            case POINT_DELETE -> FrontendIrProto.IRType.POINT_DELETE;
            case CREATE_TABLE -> FrontendIrProto.IRType.CREATE_TABLE;
            case DROP_TABLE -> FrontendIrProto.IRType.DROP_TABLE;
            case ALTER_TABLE -> FrontendIrProto.IRType.ALTER_TABLE;
            case TRUNCATE -> FrontendIrProto.IRType.TRUNCATE;
            case REFRESH -> FrontendIrProto.IRType.REFRESH;
            case CREATE_INDEX, DROP_INDEX -> FrontendIrProto.IRType.ALTER_TABLE;
        };
    }

    private static IRType fromProtoIRType(FrontendIrProto.IRType type) {
        return switch (type) {
            case POINT_QUERY -> IRType.POINT_QUERY;
            case BATCH_POINT_QUERY -> IRType.BATCH_POINT_QUERY;
            case METADATA_COUNT_ALL -> IRType.METADATA_COUNT_ALL;
            case METADATA_COUNT_COLUMN -> IRType.METADATA_COUNT_COLUMN;
            case METADATA_MIN -> IRType.METADATA_MIN;
            case METADATA_MAX -> IRType.METADATA_MAX;
            case METADATA_TABLE_INFO -> IRType.METADATA_TABLE_INFO;
            case SCAN -> IRType.SCAN;
            case AGGREGATE -> IRType.AGGREGATE;
            case JOIN -> IRType.JOIN;
            case INSERT -> IRType.INSERT;
            case BATCH_INSERT -> IRType.BATCH_INSERT;
            case UPDATE_RANGE -> IRType.UPDATE_RANGE;
            case DELETE_RANGE -> IRType.DELETE_RANGE;
            case POINT_UPDATE -> IRType.POINT_UPDATE;
            case POINT_DELETE -> IRType.POINT_DELETE;
            case CREATE_TABLE -> IRType.CREATE_TABLE;
            case DROP_TABLE -> IRType.DROP_TABLE;
            case ALTER_TABLE -> IRType.ALTER_TABLE;
            case TRUNCATE -> IRType.TRUNCATE;
            case REFRESH -> IRType.REFRESH;
            default -> throw new IllegalArgumentException("Unknown IRType: " + type);
        };
    }

    // ==================== TableType conversion ====================

    private static TableType toProtoTableType(io.github.rawwise.frontend.ir.TableType type) {
        return switch (type) {
            case PRIMARY_KEY -> TableType.PRIMARY_KEY;
            case APPEND_ONLY -> TableType.APPEND_ONLY;
        };
    }

    private static io.github.rawwise.frontend.ir.TableType fromProtoTableType(TableType type) {
        return switch (type) {
            case PRIMARY_KEY -> io.github.rawwise.frontend.ir.TableType.PRIMARY_KEY;
            case APPEND_ONLY -> io.github.rawwise.frontend.ir.TableType.APPEND_ONLY;
            default -> throw new IllegalArgumentException("Unknown TableType: " + type);
        };
    }

    // ==================== TableMetaIR conversion ====================

    private static FrontendIrProto.PbTableMetaIR toProto(TableMetaIR meta) {
        var builder =
                FrontendIrProto.PbTableMetaIR.newBuilder()
                        .setTableIdentifier(meta.tableIdentifier())
                        .setTableId(meta.tableId())
                        .setTableType(toProtoTableType(meta.tableType()))
                        .setSchema(structTypeToPbSchema(meta.schema()))
                        .addAllPrimaryKeyCols(meta.primaryKeyColumns())
                        .putAllProperties(meta.properties())
                        .setSchemaVersion(meta.schemaVersion());
        return builder.build();
    }

    private static TableMetaIR fromProto(FrontendIrProto.PbTableMetaIR proto) {
        return new TableMetaIR(
                proto.getTableIdentifier(),
                proto.getTableId(),
                fromProtoTableType(proto.getTableType()),
                pbSchemaToStructType(proto.getSchema()),
                proto.getPrimaryKeyColsList(),
                proto.getPropertiesMap(),
                proto.getSchemaVersion());
    }

    // ==================== StructType <-> PbSchema ====================

    /** Converts a StructType to a PbSchema by wrapping it in a Schema first. */
    private static PbTypeSchema structTypeToPbSchema(StructType structType) {
        Schema schema = new Schema(structType);
        return ProtoTypeConverter.toProto(schema);
    }

    /** Extracts a StructType from a PbSchema. */
    private static StructType pbSchemaToStructType(PbTypeSchema pbSchema) {
        Schema schema = ProtoTypeConverter.fromProto(pbSchema);
        return schema.structType();
    }

    // ==================== PbLiteralValue conversion ====================

    private static PbLiteralValue toProtoLiteralValue(Comparable<?> value) {
        var builder = PbLiteralValue.newBuilder();
        if (value == null) {
            return builder.build();
        }
        if (value instanceof Boolean b) {
            return builder.setBoolVal(b).build();
        } else if (value instanceof Integer i) {
            return builder.setInt32Val(i).build();
        } else if (value instanceof Long l) {
            return builder.setInt64Val(l).build();
        } else if (value instanceof Float f) {
            return builder.setFloatVal(f).build();
        } else if (value instanceof Double d) {
            return builder.setDoubleVal(d).build();
        } else if (value instanceof String s) {
            return builder.setStringVal(s).build();
        } else if (value instanceof Object obj
                && obj.getClass().isArray()
                && obj.getClass().getComponentType() == byte.class) {
            return builder.setBytesVal(ByteString.copyFrom((byte[]) obj)).build();
        } else {
            // Fallback: convert to string representation
            return builder.setStringVal(String.valueOf(value)).build();
        }
    }

    @SuppressWarnings("unchecked")
    private static Comparable<?> fromProtoLiteralValue(PbLiteralValue literal) {
        return switch (literal.getValueCase()) {
            case BOOL_VAL -> Boolean.valueOf(literal.getBoolVal());
            case INT32_VAL -> Integer.valueOf(literal.getInt32Val());
            case INT64_VAL -> Long.valueOf(literal.getInt64Val());
            case FLOAT_VAL -> Float.valueOf(literal.getFloatVal());
            case DOUBLE_VAL -> Double.valueOf(literal.getDoubleVal());
            case STRING_VAL -> literal.getStringVal();
            case BYTES_VAL ->
                    literal.getBytesVal().toStringUtf8(); // bytes as string for Comparable
            default -> null;
        };
    }

    // ==================== PointQueryIR conversion ====================

    private static FrontendIrProto.PbPointQueryIR toProto(PointQueryIR ir) {
        var builder =
                FrontendIrProto.PbPointQueryIR.newBuilder()
                        .setSnapshotTs(ir.snapshotTs())
                        .setShardId(ir.shardId())
                        .setShardEpoch(ir.shardEpoch())
                        .setTargetExecutorId(ir.targetExecutorId());

        // version_token
        if (ir.versionToken() != null) {
            builder.setVersionToken(
                    FrontendIrProto.PbDataVersionToken.newBuilder()
                            .setTableId(ir.versionToken().tableId())
                            .setCommitTs(ir.versionToken().commitTs())
                            .build());
        }

        // pk_values map
        for (var entry : ir.pkValues().entrySet()) {
            builder.putPkValues(entry.getKey(), toProtoLiteralValue(entry.getValue()));
        }

        // encoded_key
        if (ir.encodedKey() != null && ir.encodedKey().length > 0) {
            builder.setEncodedKey(ByteString.copyFrom(ir.encodedKey()));
        }

        // projection
        builder.addAllProjection(ir.projection());

        return builder.build();
    }

    private static PointQueryIR fromProtoPoint(FrontendIrProto.PbQueryIR proto) {
        FrontendIrProto.PbPointQueryIR point = proto.getPoint();
        TableMetaIR tableMeta = fromProto(proto.getTableMeta());

        Map<String, Comparable<?>> pkValues = new LinkedHashMap<>();
        for (var entry : point.getPkValuesMap().entrySet()) {
            pkValues.put(entry.getKey(), fromProtoLiteralValue(entry.getValue()));
        }

        byte[] encodedKey =
                !point.getEncodedKey().isEmpty()
                        ? point.getEncodedKey().toByteArray()
                        : new byte[0];

        DataVersionToken versionToken =
                point.hasVersionToken()
                        ? DataVersionToken.of(
                                point.getVersionToken().getTableId(),
                                point.getVersionToken().getCommitTs())
                        : null;

        return new PointQueryIR(
                tableMeta,
                versionToken,
                point.getSnapshotTs(),
                new io.github.rawwise.engine.txn.ReadSnapshot(
                        point.getSnapshotTs(), proto.getSnapshotId()),
                pkValues,
                encodedKey,
                point.getProjectionList(),
                point.getTargetExecutorId(),
                point.getShardId(),
                point.getShardEpoch());
    }

    // ==================== BatchPointQueryIR conversion ====================

    private static FrontendIrProto.PbBatchPointQueryIR toProto(BatchPointQueryIR ir) {
        var builder =
                FrontendIrProto.PbBatchPointQueryIR.newBuilder()
                        .setShardId(ir.shardId())
                        .setShardEpoch(ir.shardEpoch())
                        .setTargetExecutorId(ir.targetExecutorId())
                        .setSnapshotTs(ir.snapshotTs());

        // version_token
        if (ir.versionToken() != null) {
            builder.setVersionToken(
                    FrontendIrProto.PbDataVersionToken.newBuilder()
                            .setTableId(ir.versionToken().tableId())
                            .setCommitTs(ir.versionToken().commitTs())
                            .build());
        }

        for (byte[] key : ir.encodedKeys()) {
            builder.addEncodedKeys(ByteString.copyFrom(key));
        }
        builder.addAllProjection(ir.projection());

        return builder.build();
    }

    private static BatchPointQueryIR fromProtoBatchPoint(FrontendIrProto.PbQueryIR proto) {
        FrontendIrProto.PbBatchPointQueryIR batch = proto.getBatchPoint();
        TableMetaIR tableMeta = fromProto(proto.getTableMeta());

        List<byte[]> encodedKeys = new ArrayList<>();
        for (ByteString bs : batch.getEncodedKeysList()) {
            encodedKeys.add(bs.toByteArray());
        }

        DataVersionToken versionToken =
                batch.hasVersionToken()
                        ? DataVersionToken.of(
                                batch.getVersionToken().getTableId(),
                                batch.getVersionToken().getCommitTs())
                        : null;

        return new BatchPointQueryIR(
                tableMeta,
                versionToken,
                batch.getSnapshotTs(),
                new io.github.rawwise.engine.txn.ReadSnapshot(
                        batch.getSnapshotTs(), proto.getSnapshotId()),
                encodedKeys,
                batch.getProjectionList(),
                batch.getTargetExecutorId(),
                batch.getShardId(),
                batch.getShardEpoch());
    }

    // ==================== MetadataQueryIR conversion ====================

    private static FrontendIrProto.PbMetadataQueryIR toProto(MetadataQueryIR ir) {
        var builder = FrontendIrProto.PbMetadataQueryIR.newBuilder();
        switch (ir) {
            case MetadataQueryIR.CountAll countAll -> {
                builder.setQueryType(FrontendIrProto.MetadataQueryType.COUNT_ALL);
            }
            case MetadataQueryIR.CountColumn countCol -> {
                builder.setQueryType(FrontendIrProto.MetadataQueryType.COUNT_COLUMN)
                        .setColumn(countCol.column())
                        .setDistinct(countCol.distinct());
            }
            case MetadataQueryIR.MinValue minVal -> {
                builder.setQueryType(FrontendIrProto.MetadataQueryType.MIN_VALUE)
                        .setColumn(minVal.column())
                        .setIsMin(true);
            }
            case MetadataQueryIR.MaxValue maxVal -> {
                builder.setQueryType(FrontendIrProto.MetadataQueryType.MAX_VALUE)
                        .setColumn(maxVal.column());
            }
            case MetadataQueryIR.TableInfo tableInfo -> {
                builder.setQueryType(FrontendIrProto.MetadataQueryType.TABLE_INFO);
            }
        }
        return builder.build();
    }

    private static MetadataQueryIR fromProtoMetadata(FrontendIrProto.PbQueryIR proto) {
        FrontendIrProto.PbMetadataQueryIR meta = proto.getMetadata();
        TableMetaIR tableMeta = fromProto(proto.getTableMeta());
        long snapshotId = proto.getSnapshotId();

        return switch (meta.getQueryType()) {
            case COUNT_ALL -> new MetadataQueryIR.CountAll(tableMeta, snapshotId);
            case COUNT_COLUMN ->
                    new MetadataQueryIR.CountColumn(
                            tableMeta, snapshotId, meta.getColumn(), meta.getDistinct());
            case MIN_VALUE -> new MetadataQueryIR.MinValue(tableMeta, snapshotId, meta.getColumn());
            case MAX_VALUE -> new MetadataQueryIR.MaxValue(tableMeta, snapshotId, meta.getColumn());
            case TABLE_INFO -> new MetadataQueryIR.TableInfo(tableMeta, snapshotId);
            default ->
                    throw new IllegalArgumentException(
                            "Unknown MetadataQueryType: " + meta.getQueryType());
        };
    }

    // ==================== ScanQueryIR conversion ====================

    private static FrontendIrProto.PbScanQueryIR toProto(ScanQueryIR ir) {
        var builder =
                FrontendIrProto.PbScanQueryIR.newBuilder()
                        .setLimit(ir.limit())
                        .setOffset(ir.offset());

        // projection
        for (ProjectionColumn col : ir.projection()) {
            builder.addProjection(toProto(col));
        }

        // filter
        if (ir.filter() != null) {
            builder.setFilter(ProtobufExpressionSerializer.toProto(ir.filter()));
        }

        // sort_keys
        for (SortKey key : ir.sortKeys()) {
            builder.addSortKeys(toProto(key));
        }

        // pre_filter
        if (ir.preFilter() != null) {
            builder.setPreFilter(ProtobufExpressionSerializer.toProto(ir.preFilter()));
        }

        return builder.build();
    }

    private static ScanQueryIR fromProtoScan(FrontendIrProto.PbQueryIR proto) {
        FrontendIrProto.PbScanQueryIR scan = proto.getScan();
        TableMetaIR tableMeta = fromProto(proto.getTableMeta());

        List<ProjectionColumn> projection = new ArrayList<>();
        for (FrontendIrProto.PbProjectionColumn col : scan.getProjectionList()) {
            projection.add(fromProto(col));
        }

        Expression filter =
                scan.hasFilter() ? ProtobufExpressionSerializer.fromProto(scan.getFilter()) : null;

        List<SortKey> sortKeys = new ArrayList<>();
        for (FrontendIrProto.PbSortKey key : scan.getSortKeysList()) {
            sortKeys.add(fromProto(key));
        }

        Expression preFilter =
                scan.hasPreFilter()
                        ? ProtobufExpressionSerializer.fromProto(scan.getPreFilter())
                        : null;

        return new ScanQueryIR(
                tableMeta,
                proto.getSnapshotId(),
                projection,
                filter,
                sortKeys,
                scan.getLimit(),
                scan.getOffset(),
                preFilter);
    }

    // ==================== ProjectionColumn conversion ====================

    private static FrontendIrProto.PbProjectionColumn toProto(ProjectionColumn col) {
        var builder =
                FrontendIrProto.PbProjectionColumn.newBuilder()
                        .setName(col.name())
                        .setFieldId(col.fieldId());
        if (col.alias() != null) {
            builder.setAlias(col.alias());
        }
        return builder.build();
    }

    private static ProjectionColumn fromProto(FrontendIrProto.PbProjectionColumn proto) {
        return new ProjectionColumn(
                proto.getName(),
                proto.getFieldId(),
                null, // type is not stored in proto; would need schema lookup
                !proto.getAlias().isEmpty() ? proto.getAlias() : null);
    }

    // ==================== SortKey conversion ====================

    private static FrontendIrProto.PbSortKey toProto(SortKey key) {
        return FrontendIrProto.PbSortKey.newBuilder()
                .setColumn(key.column())
                .setAscending(key.ascending())
                .setNullsFirst(key.nullsFirst())
                .build();
    }

    private static SortKey fromProto(FrontendIrProto.PbSortKey proto) {
        return new SortKey(proto.getColumn(), proto.getAscending(), proto.getNullsFirst());
    }

    // ==================== AggregateQueryIR conversion ====================

    private static FrontendIrProto.PbAggregateQueryIR toProto(AggregateQueryIR ir) {
        var builder = FrontendIrProto.PbAggregateQueryIR.newBuilder();

        for (TableMetaIR meta : ir.tableMetas()) {
            builder.addTableMetas(toProto(meta));
        }
        for (AggregateColumn col : ir.aggregates()) {
            builder.addAggregates(toProto(col));
        }
        for (Expression expr : ir.groupByKeys()) {
            builder.addGroupByKeys(ProtobufExpressionSerializer.toProto(expr));
        }
        if (ir.havingFilter() != null) {
            builder.setHavingFilter(ProtobufExpressionSerializer.toProto(ir.havingFilter()));
        }
        if (ir.whereFilter() != null) {
            builder.setWhereFilter(ProtobufExpressionSerializer.toProto(ir.whereFilter()));
        }

        return builder.build();
    }

    private static AggregateQueryIR fromProtoAggregate(FrontendIrProto.PbQueryIR proto) {
        FrontendIrProto.PbAggregateQueryIR agg = proto.getAggregate();

        List<TableMetaIR> tableMetas = new ArrayList<>();
        for (FrontendIrProto.PbTableMetaIR meta : agg.getTableMetasList()) {
            tableMetas.add(fromProto(meta));
        }

        List<AggregateColumn> aggregates = new ArrayList<>();
        for (FrontendIrProto.PbAggregateColumn col : agg.getAggregatesList()) {
            aggregates.add(fromProto(col));
        }

        List<Expression> groupByKeys = new ArrayList<>();
        for (var expr : agg.getGroupByKeysList()) {
            groupByKeys.add(ProtobufExpressionSerializer.fromProto(expr));
        }

        Expression havingFilter =
                agg.hasHavingFilter()
                        ? ProtobufExpressionSerializer.fromProto(agg.getHavingFilter())
                        : null;
        Expression whereFilter =
                agg.hasWhereFilter()
                        ? ProtobufExpressionSerializer.fromProto(agg.getWhereFilter())
                        : null;

        return new AggregateQueryIR(
                tableMetas,
                proto.getSnapshotId(),
                aggregates,
                groupByKeys,
                havingFilter,
                whereFilter);
    }

    // ==================== AggregateColumn conversion ====================

    private static FrontendIrProto.PbAggregateColumn toProto(AggregateColumn col) {
        var builder =
                FrontendIrProto.PbAggregateColumn.newBuilder()
                        .setFunction(toProtoAggregateFunc(col.function()))
                        .setColumn(col.column());
        if (col.alias() != null) {
            builder.setAlias(col.alias());
        }
        return builder.build();
    }

    private static AggregateColumn fromProto(FrontendIrProto.PbAggregateColumn proto) {
        return new AggregateColumn(
                fromProtoAggregateFunc(proto.getFunction()),
                proto.getColumn(),
                proto.hasAlias() ? proto.getAlias() : null);
    }

    private static FrontendIrProto.AggregateFunc toProtoAggregateFunc(AggregateFunction func) {
        return switch (func) {
            case SUM -> FrontendIrProto.AggregateFunc.SUM;
            case COUNT -> FrontendIrProto.AggregateFunc.COUNT;
            case AVG -> FrontendIrProto.AggregateFunc.AVG;
            case MIN -> FrontendIrProto.AggregateFunc.MIN;
            case MAX -> FrontendIrProto.AggregateFunc.MAX;
            case COUNT_DISTINCT -> FrontendIrProto.AggregateFunc.COUNT_DISTINCT;
        };
    }

    private static AggregateFunction fromProtoAggregateFunc(FrontendIrProto.AggregateFunc func) {
        return switch (func) {
            case SUM -> AggregateFunction.SUM;
            case COUNT -> AggregateFunction.COUNT;
            case AVG -> AggregateFunction.AVG;
            case MIN -> AggregateFunction.MIN;
            case MAX -> AggregateFunction.MAX;
            case COUNT_DISTINCT -> AggregateFunction.COUNT_DISTINCT;
            default -> throw new IllegalArgumentException("Unknown AggregateFunc: " + func);
        };
    }

    // ==================== JoinQueryIR conversion ====================

    private static FrontendIrProto.PbJoinQueryIR toProto(JoinQueryIR ir) {
        var builder = FrontendIrProto.PbJoinQueryIR.newBuilder();

        for (TableMetaIR meta : ir.tableMetas()) {
            builder.addTableMetas(toProto(meta));
        }
        builder.setLeft(toProto(ir.left()));
        builder.setRight(toProto(ir.right()));
        builder.setJoinType(toProtoJoinKind(ir.joinType()));
        for (ProjectionColumn col : ir.projection()) {
            builder.addProjection(toProto(col));
        }
        if (ir.joinCondition() != null) {
            builder.setJoinCondition(ProtobufExpressionSerializer.toProto(ir.joinCondition()));
        }
        if (ir.postFilter() != null) {
            builder.setPostFilter(ProtobufExpressionSerializer.toProto(ir.postFilter()));
        }
        builder.setLimit(ir.limit());

        return builder.build();
    }

    private static JoinQueryIR fromProtoJoin(FrontendIrProto.PbQueryIR proto) {
        FrontendIrProto.PbJoinQueryIR join = proto.getJoin();

        List<TableMetaIR> tableMetas = new ArrayList<>();
        for (FrontendIrProto.PbTableMetaIR meta : join.getTableMetasList()) {
            tableMetas.add(fromProto(meta));
        }

        QueryIR left = fromProto(join.getLeft());
        QueryIR right = fromProto(join.getRight());
        JoinType joinType = fromProtoJoinKind(join.getJoinType());

        List<ProjectionColumn> projection = new ArrayList<>();
        for (FrontendIrProto.PbProjectionColumn col : join.getProjectionList()) {
            projection.add(fromProto(col));
        }

        Expression joinCondition =
                join.hasJoinCondition()
                        ? ProtobufExpressionSerializer.fromProto(join.getJoinCondition())
                        : null;
        Expression postFilter =
                join.hasPostFilter()
                        ? ProtobufExpressionSerializer.fromProto(join.getPostFilter())
                        : null;

        return new JoinQueryIR(
                tableMetas,
                proto.getSnapshotId(),
                left,
                right,
                joinType,
                joinCondition,
                projection,
                postFilter,
                join.getLimit());
    }

    private static FrontendIrProto.JoinKind toProtoJoinKind(JoinType type) {
        return switch (type) {
            case INNER -> FrontendIrProto.JoinKind.INNER;
            case LEFT_OUTER -> FrontendIrProto.JoinKind.LEFT_OUTER;
            case RIGHT_OUTER -> FrontendIrProto.JoinKind.RIGHT_OUTER;
            case FULL_OUTER -> FrontendIrProto.JoinKind.FULL_OUTER;
            case CROSS -> FrontendIrProto.JoinKind.CROSS;
            case LEFT_SEMI -> FrontendIrProto.JoinKind.LEFT_SEMI;
            case LEFT_ANTI -> FrontendIrProto.JoinKind.LEFT_ANTI;
        };
    }

    private static JoinType fromProtoJoinKind(FrontendIrProto.JoinKind kind) {
        return switch (kind) {
            case INNER -> JoinType.INNER;
            case LEFT_OUTER -> JoinType.LEFT_OUTER;
            case RIGHT_OUTER -> JoinType.RIGHT_OUTER;
            case FULL_OUTER -> JoinType.FULL_OUTER;
            case CROSS -> JoinType.CROSS;
            case LEFT_SEMI -> JoinType.LEFT_SEMI;
            case LEFT_ANTI -> JoinType.LEFT_ANTI;
            default -> throw new IllegalArgumentException("Unknown JoinKind: " + kind);
        };
    }

    // ==================== WriteQueryIR conversion ====================

    private static FrontendIrProto.PbWriteQueryIR toProto(WriteQueryIR ir) {
        var builder =
                FrontendIrProto.PbWriteQueryIR.newBuilder()
                        .setWriteType(toProtoWriteType(ir.writeType()));

        for (Map<String, Comparable<?>> row : ir.rows()) {
            var rowBuilder = FrontendIrProto.PbRow.newBuilder();
            for (var entry : row.entrySet()) {
                rowBuilder.putValues(entry.getKey(), toProtoLiteralValue(entry.getValue()));
            }
            builder.addRows(rowBuilder.build());
        }

        if (ir.insertFromQuery() != null) {
            builder.setInsertFromQuery(toProto(ir.insertFromQuery()));
        }

        return builder.build();
    }

    private static WriteQueryIR fromProtoWrite(FrontendIrProto.PbQueryIR proto) {
        FrontendIrProto.PbWriteQueryIR write = proto.getWrite();
        TableMetaIR tableMeta = fromProto(proto.getTableMeta());

        List<Map<String, Comparable<?>>> rows = new ArrayList<>();
        for (FrontendIrProto.PbRow row : write.getRowsList()) {
            Map<String, Comparable<?>> rowMap = new LinkedHashMap<>();
            for (var entry : row.getValuesMap().entrySet()) {
                rowMap.put(entry.getKey(), fromProtoLiteralValue(entry.getValue()));
            }
            rows.add(rowMap);
        }

        QueryIR insertFromQuery =
                write.hasInsertFromQuery() ? fromProto(write.getInsertFromQuery()) : null;

        return new WriteQueryIR(
                tableMeta, fromProtoWriteType(write.getWriteType()), rows, insertFromQuery);
    }

    private static FrontendIrProto.WriteType toProtoWriteType(WriteType type) {
        return switch (type) {
            case INSERT -> FrontendIrProto.WriteType.WRITE_INSERT;
            case BATCH_INSERT -> FrontendIrProto.WriteType.WRITE_BATCH_INSERT;
            case INSERT_SELECT -> FrontendIrProto.WriteType.WRITE_INSERT_SELECT;
        };
    }

    private static WriteType fromProtoWriteType(FrontendIrProto.WriteType type) {
        return switch (type) {
            case WRITE_INSERT -> WriteType.INSERT;
            case WRITE_BATCH_INSERT -> WriteType.BATCH_INSERT;
            case WRITE_INSERT_SELECT -> WriteType.INSERT_SELECT;
            default -> throw new IllegalArgumentException("Unknown WriteType: " + type);
        };
    }

    // ==================== UpdateRangeIR conversion ====================

    private static FrontendIrProto.PbUpdateRangeIR toProto(UpdateRangeIR ir) {
        var builder = FrontendIrProto.PbUpdateRangeIR.newBuilder();
        for (var entry : ir.setClauses().entrySet()) {
            builder.putSetClauses(entry.getKey(), toProtoLiteralValue(entry.getValue()));
        }
        if (ir.rangeFilter() != null) {
            builder.setRangeFilter(ProtobufExpressionSerializer.toProto(ir.rangeFilter()));
        }
        return builder.build();
    }

    private static UpdateRangeIR fromProtoUpdateRange(FrontendIrProto.PbQueryIR proto) {
        FrontendIrProto.PbUpdateRangeIR update = proto.getUpdateRange();
        TableMetaIR tableMeta = fromProto(proto.getTableMeta());

        Map<String, Comparable<?>> setClauses = new LinkedHashMap<>();
        for (var entry : update.getSetClausesMap().entrySet()) {
            setClauses.put(entry.getKey(), fromProtoLiteralValue(entry.getValue()));
        }

        Expression rangeFilter =
                update.hasRangeFilter()
                        ? ProtobufExpressionSerializer.fromProto(update.getRangeFilter())
                        : null;

        return new UpdateRangeIR(tableMeta, setClauses, rangeFilter);
    }

    // ==================== DeleteRangeIR conversion ====================

    private static FrontendIrProto.PbDeleteRangeIR toProto(DeleteRangeIR ir) {
        var builder = FrontendIrProto.PbDeleteRangeIR.newBuilder();
        if (ir.rangeFilter() != null) {
            builder.setRangeFilter(ProtobufExpressionSerializer.toProto(ir.rangeFilter()));
        }
        return builder.build();
    }

    private static DeleteRangeIR fromProtoDeleteRange(FrontendIrProto.PbQueryIR proto) {
        FrontendIrProto.PbDeleteRangeIR delete = proto.getDeleteRange();
        TableMetaIR tableMeta = fromProto(proto.getTableMeta());

        Expression rangeFilter =
                delete.hasRangeFilter()
                        ? ProtobufExpressionSerializer.fromProto(delete.getRangeFilter())
                        : null;

        return new DeleteRangeIR(tableMeta, rangeFilter);
    }

    // ==================== UpdateQueryIR conversion ====================

    private static FrontendIrProto.PbUpdateQueryIR toProto(UpdateQueryIR ir) {
        var builder =
                FrontendIrProto.PbUpdateQueryIR.newBuilder()
                        .setSnapshotId(ir.snapshotId())
                        .setMode(toProtoUpdateMode(ir.mode()))
                        .setShardId(ir.shardId())
                        .setShardEpoch(ir.shardEpoch())
                        .setTargetExecutorId(ir.targetExecutorId());

        for (var entry : ir.pkValues().entrySet()) {
            builder.putPkValues(entry.getKey(), toProtoLiteralValue(entry.getValue()));
        }
        if (ir.encodedKey() != null && ir.encodedKey().length > 0) {
            builder.setEncodedKey(ByteString.copyFrom(ir.encodedKey()));
        }
        for (var entry : ir.setClauses().entrySet()) {
            builder.putSetClauses(entry.getKey(), toProtoLiteralValue(entry.getValue()));
        }
        return builder.build();
    }

    private static UpdateQueryIR fromProtoPointUpdate(FrontendIrProto.PbQueryIR proto) {
        FrontendIrProto.PbUpdateQueryIR update = proto.getPointUpdate();
        TableMetaIR tableMeta = fromProto(proto.getTableMeta());

        Map<String, Comparable<?>> pkValues = new LinkedHashMap<>();
        for (var entry : update.getPkValuesMap().entrySet()) {
            pkValues.put(entry.getKey(), fromProtoLiteralValue(entry.getValue()));
        }
        Map<String, Comparable<?>> setClauses = new LinkedHashMap<>();
        for (var entry : update.getSetClausesMap().entrySet()) {
            setClauses.put(entry.getKey(), fromProtoLiteralValue(entry.getValue()));
        }
        byte[] encodedKey =
                !update.getEncodedKey().isEmpty()
                        ? update.getEncodedKey().toByteArray()
                        : new byte[0];

        return new UpdateQueryIR(
                tableMeta,
                pkValues,
                encodedKey,
                setClauses,
                update.getSnapshotId(),
                fromProtoUpdateMode(update.getMode()),
                update.getTargetExecutorId(),
                update.getShardId(),
                update.getShardEpoch());
    }

    // ==================== DeleteQueryIR conversion ====================

    private static FrontendIrProto.PbDeleteQueryIR toProto(DeleteQueryIR ir) {
        var builder =
                FrontendIrProto.PbDeleteQueryIR.newBuilder()
                        .setSnapshotId(ir.snapshotId())
                        .setMode(toProtoUpdateMode(ir.mode()))
                        .setShardId(ir.shardId())
                        .setShardEpoch(ir.shardEpoch())
                        .setTargetExecutorId(ir.targetExecutorId());

        for (var entry : ir.pkValues().entrySet()) {
            builder.putPkValues(entry.getKey(), toProtoLiteralValue(entry.getValue()));
        }
        if (ir.encodedKey() != null && ir.encodedKey().length > 0) {
            builder.setEncodedKey(ByteString.copyFrom(ir.encodedKey()));
        }
        return builder.build();
    }

    private static DeleteQueryIR fromProtoPointDelete(FrontendIrProto.PbQueryIR proto) {
        FrontendIrProto.PbDeleteQueryIR delete = proto.getPointDelete();
        TableMetaIR tableMeta = fromProto(proto.getTableMeta());

        Map<String, Comparable<?>> pkValues = new LinkedHashMap<>();
        for (var entry : delete.getPkValuesMap().entrySet()) {
            pkValues.put(entry.getKey(), fromProtoLiteralValue(entry.getValue()));
        }
        byte[] encodedKey =
                !delete.getEncodedKey().isEmpty()
                        ? delete.getEncodedKey().toByteArray()
                        : new byte[0];

        return new DeleteQueryIR(
                tableMeta,
                pkValues,
                encodedKey,
                delete.getSnapshotId(),
                fromProtoUpdateMode(delete.getMode()),
                delete.getTargetExecutorId(),
                delete.getShardId(),
                delete.getShardEpoch());
    }

    private static FrontendIrProto.UpdateMode toProtoUpdateMode(UpdateMode mode) {
        return switch (mode) {
            case POINT -> FrontendIrProto.UpdateMode.UPDATE_MODE_POINT;
            case LAZY -> FrontendIrProto.UpdateMode.UPDATE_MODE_LAZY;
        };
    }

    private static UpdateMode fromProtoUpdateMode(FrontendIrProto.UpdateMode mode) {
        return switch (mode) {
            case UPDATE_MODE_POINT -> UpdateMode.POINT;
            case UPDATE_MODE_LAZY -> UpdateMode.LAZY;
            default -> throw new IllegalArgumentException("Unknown UpdateMode: " + mode);
        };
    }

    // ==================== DDLIR conversion ====================

    private static FrontendIrProto.PbDDLIR toProto(DDLIR ir) {
        var builder = FrontendIrProto.PbDDLIR.newBuilder().setDdlType(toProtoDDLType(ir.ddlType()));
        for (var entry : ir.options().entrySet()) {
            builder.putOptions(entry.getKey(), String.valueOf(entry.getValue()));
        }
        return builder.build();
    }

    private static DDLIR fromProtoDDL(FrontendIrProto.PbQueryIR proto) {
        FrontendIrProto.PbDDLIR ddl = proto.getDdl();
        TableMetaIR tableMeta = fromProto(proto.getTableMeta());

        Map<String, Object> options = new LinkedHashMap<>();
        for (var entry : ddl.getOptionsMap().entrySet()) {
            options.put(entry.getKey(), entry.getValue());
        }

        return new DDLIR(tableMeta, fromProtoDDLType(ddl.getDdlType()), options);
    }

    private static FrontendIrProto.DDLType toProtoDDLType(DDLType type) {
        return switch (type) {
            case CREATE_TABLE -> FrontendIrProto.DDLType.DDL_CREATE_TABLE;
            case DROP_TABLE -> FrontendIrProto.DDLType.DDL_DROP_TABLE;
            case ALTER_TABLE -> FrontendIrProto.DDLType.DDL_ALTER_TABLE;
            case TRUNCATE -> FrontendIrProto.DDLType.DDL_TRUNCATE;
            case REFRESH -> FrontendIrProto.DDLType.DDL_REFRESH;
            case CREATE_INDEX, DROP_INDEX -> FrontendIrProto.DDLType.DDL_ALTER_TABLE;
        };
    }

    private static DDLType fromProtoDDLType(FrontendIrProto.DDLType type) {
        return switch (type) {
            case DDL_CREATE_TABLE -> DDLType.CREATE_TABLE;
            case DDL_DROP_TABLE -> DDLType.DROP_TABLE;
            case DDL_ALTER_TABLE -> DDLType.ALTER_TABLE;
            case DDL_TRUNCATE -> DDLType.TRUNCATE;
            case DDL_REFRESH -> DDLType.REFRESH;
            default -> throw new IllegalArgumentException("Unknown DDLType: " + type);
        };
    }
}
