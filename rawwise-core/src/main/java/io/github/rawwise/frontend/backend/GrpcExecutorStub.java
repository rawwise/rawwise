package io.github.rawwise.frontend.backend;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.github.rawwise.frontend.ir.routing.FastPathExecutor;
import io.github.rawwise.frontend.ir.routing.PointQueryResult;
import io.github.rawwise.meta.PbLiteralValue;
import io.github.rawwise.server.BatchPointQueryRequest;
import io.github.rawwise.server.BatchPointQueryResponse;
import io.github.rawwise.server.ExecutorPointQueryServiceGrpc;
import io.github.rawwise.server.GetShardEpochRequest;
import io.github.rawwise.server.GetShardEpochResponse;
import io.github.rawwise.server.PointDeleteRequest;
import io.github.rawwise.server.PointDeleteResponse;
import io.github.rawwise.server.PointInsertRequest;
import io.github.rawwise.server.PointInsertResponse;
import io.github.rawwise.server.PointQueryRequest;
import io.github.rawwise.server.PointQueryResponse;
import io.github.rawwise.server.PointUpdateRequest;
import io.github.rawwise.server.PointUpdateResponse;
import io.grpc.ManagedChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GrpcExecutorStub implements FastPathExecutor.ExecutorStub {

    private static final ConcurrentHashMap<Long, io.github.rawwise.meta.PbStructType> SCHEMA_CACHE =
            new ConcurrentHashMap<>();

    private final ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub stub;
    private final int timeoutSeconds;

    public GrpcExecutorStub(ManagedChannel channel, int timeoutSeconds) {
        this.stub = ExecutorPointQueryServiceGrpc.newBlockingStub(channel);
        this.timeoutSeconds = timeoutSeconds;
    }

    @Override
    public Optional<byte[]> pointQuery(
            String tableId, byte[] key, long snapshotTs, long shardEpoch) {
        return pointQuery(tableId, key, null, null, snapshotTs, shardEpoch);
    }

    @Override
    public Optional<byte[]> pointQuery(
            String tableId,
            byte[] key,
            Map<String, Comparable<?>> pkValues,
            List<String> primaryKeyCols,
            long snapshotTs,
            long shardEpoch) {
        try {
            pkValues = normalizeBySchema(pkValues);
            PointQueryRequest.Builder reqBuilder =
                    PointQueryRequest.newBuilder()
                            .setTableId(tableId)
                            .setSnapshotTs(snapshotTs)
                            .setShardEpoch(shardEpoch);
            if (key != null) {
                // Zero-copy: key is newly-allocated byte[] passed by FastPathExecutor, will not be
                // reused/modified
                reqBuilder.setKey(UnsafeByteOperations.unsafeWrap(key));
            }
            if (pkValues != null && !pkValues.isEmpty()) {
                for (var e : pkValues.entrySet()) {
                    reqBuilder.putPkValues(e.getKey(), toProtoLiteral(e.getValue()));
                }
            }
            if (primaryKeyCols != null && !primaryKeyCols.isEmpty()) {
                reqBuilder.addAllPrimaryKeyCols(primaryKeyCols);
            }
            // Approach A: read schema from ThreadLocal and write to request, enabling server side
            // RecordCodec decode.
            io.github.rawwise.frontend.ir.routing.FastPathExecutor.SchemaContext sc =
                    io.github.rawwise.frontend.ir.routing.FastPathExecutor.CURRENT_SCHEMA.get();
            if (sc != null && sc.schema() != null) {
                reqBuilder.setSchema(
                        SCHEMA_CACHE.computeIfAbsent(
                                sc.numericTableId(),
                                k ->
                                        io.github.rawwise.model.record.ProtoTableMetaConverter
                                                .toPbStructType(sc.schema())));
                reqBuilder.setNumericTableId(sc.numericTableId());
            }
            PointQueryResponse response =
                    stub.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
                            .pointQuery(reqBuilder.build());
            // Check if response.epoch matches request.shardEpoch
            // Throw StaleEpochException on mismatch, triggering FastPathExecutor retry
            long responseEpoch = response.getEpoch();
            if (shardEpoch > 0 && responseEpoch > 0 && responseEpoch != shardEpoch) {
                throw new FastPathExecutor.StaleEpochException(
                        "epoch mismatch: request="
                                + shardEpoch
                                + ", response="
                                + responseEpoch
                                + " for table="
                                + tableId);
            }
            if (response.getFound()) {
                return Optional.of(response.getData().toByteArray());
            }
            return Optional.empty();
        } catch (FastPathExecutor.StaleEpochException e) {
            // Re-throw without wrapping, so FastPathExecutor's catch (StaleEpochException) can
            // match
            throw e;
        } catch (Exception e) {
            log.debug(
                    "GrpcExecutorStub pointQuery failed for table={}: {}", tableId, e.getMessage());
            throw new RuntimeException("Point query failed: " + e.getMessage(), e);
        }
    }

    @Override
    public List<PointQueryResult> batchPointQuery(
            String tableId, List<byte[]> keys, long snapshotTs, long shardEpoch) {
        try {
            List<ByteString> keyList = new ArrayList<>();
            for (byte[] key : keys) {
                // Zero-copy: byte[] in keys list will not be reused
                keyList.add(UnsafeByteOperations.unsafeWrap(key));
            }
            BatchPointQueryRequest request =
                    BatchPointQueryRequest.newBuilder()
                            .setTableId(tableId)
                            .addAllKeys(keyList)
                            .setSnapshotTs(snapshotTs)
                            .setShardEpoch(shardEpoch)
                            .build();
            BatchPointQueryResponse response =
                    stub.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
                            .batchPointQuery(request);
            List<PointQueryResult> results = new ArrayList<>();
            for (var r : response.getResultsList()) {
                results.add(
                        new PointQueryResult(
                                r.getKey().toByteArray(), r.getData().toByteArray(), r.getFound()));
            }
            return results;
        } catch (Exception e) {
            log.debug(
                    "GrpcExecutorStub batchPointQuery failed for table={}: {}",
                    tableId,
                    e.getMessage());
            throw new RuntimeException("Batch point query failed: " + e.getMessage(), e);
        }
    }

    @Override
    public long getShardEpoch(String tableId, int shardId) {
        // Call real RPC to query Backend Node local epoch
        try {
            GetShardEpochRequest request =
                    GetShardEpochRequest.newBuilder()
                            .setTableId(tableId)
                            .setShardId(shardId)
                            .build();
            GetShardEpochResponse response =
                    stub.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS).getShardEpoch(request);
            return response.getEpoch();
        } catch (Exception e) {
            log.debug(
                    "GrpcExecutorStub getShardEpoch failed for table={} shardId={}: {}",
                    tableId,
                    shardId,
                    e.getMessage());
            // Return 0 on failure, letting FastPathExecutor treat as stale and trigger fallback
            return 0L;
        }
    }

    @Override
    public FastPathExecutor.PointWriteResult pointUpdate(
            String tableId,
            Map<String, Comparable<?>> pkValues,
            Map<String, Comparable<?>> setClauses,
            List<String> primaryKeyCols,
            long snapshotTs,
            long shardEpoch) {
        try {
            pkValues = normalizeBySchema(pkValues);
            setClauses = normalizeBySchema(setClauses);
            PointUpdateRequest.Builder reqBuilder =
                    PointUpdateRequest.newBuilder().setTableId(tableId).setSnapshotTs(snapshotTs);
            if (pkValues != null) {
                for (var e : pkValues.entrySet()) {
                    reqBuilder.putPkValues(e.getKey(), toProtoLiteral(e.getValue()));
                }
            }
            if (setClauses != null) {
                for (var e : setClauses.entrySet()) {
                    reqBuilder.putSetClauses(e.getKey(), toProtoLiteral(e.getValue()));
                }
            }
            if (primaryKeyCols != null) {
                reqBuilder.addAllPrimaryKeyCols(primaryKeyCols);
            }
            // Approach A: inject schema
            io.github.rawwise.frontend.ir.routing.FastPathExecutor.SchemaContext sc =
                    io.github.rawwise.frontend.ir.routing.FastPathExecutor.CURRENT_SCHEMA.get();
            if (sc != null && sc.schema() != null) {
                reqBuilder.setSchema(
                        SCHEMA_CACHE.computeIfAbsent(
                                sc.numericTableId(),
                                k ->
                                        io.github.rawwise.model.record.ProtoTableMetaConverter
                                                .toPbStructType(sc.schema())));
                reqBuilder.setNumericTableId(sc.numericTableId());
            }
            PointUpdateResponse response =
                    stub.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
                            .pointUpdate(reqBuilder.build());
            if (response.getSuccess()) {
                return FastPathExecutor.PointWriteResult.ok(1);
            }
            String err = response.getError();
            if (err != null && err.contains("not found")) {
                return FastPathExecutor.PointWriteResult.notFound();
            }
            return FastPathExecutor.PointWriteResult.conflict(err);
        } catch (Exception e) {
            log.debug(
                    "GrpcExecutorStub pointUpdate failed for table={}: {}",
                    tableId,
                    e.getMessage());
            throw new RuntimeException("Point update failed: " + e.getMessage(), e);
        }
    }

    @Override
    public FastPathExecutor.PointWriteResult pointDelete(
            String tableId,
            Map<String, Comparable<?>> pkValues,
            List<String> primaryKeyCols,
            long snapshotTs,
            long shardEpoch) {
        try {
            pkValues = normalizeBySchema(pkValues);
            PointDeleteRequest.Builder reqBuilder =
                    PointDeleteRequest.newBuilder().setTableId(tableId);
            if (pkValues != null) {
                for (var e : pkValues.entrySet()) {
                    reqBuilder.putPkValues(e.getKey(), toProtoLiteral(e.getValue()));
                }
            }
            if (primaryKeyCols != null) {
                reqBuilder.addAllPrimaryKeyCols(primaryKeyCols);
            }
            // Approach A: inject schema
            io.github.rawwise.frontend.ir.routing.FastPathExecutor.SchemaContext sc =
                    io.github.rawwise.frontend.ir.routing.FastPathExecutor.CURRENT_SCHEMA.get();
            if (sc != null && sc.schema() != null) {
                reqBuilder.setSchema(
                        SCHEMA_CACHE.computeIfAbsent(
                                sc.numericTableId(),
                                k ->
                                        io.github.rawwise.model.record.ProtoTableMetaConverter
                                                .toPbStructType(sc.schema())));
                reqBuilder.setNumericTableId(sc.numericTableId());
            }
            PointDeleteResponse response =
                    stub.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
                            .pointDelete(reqBuilder.build());
            if (response.getSuccess()) {
                return FastPathExecutor.PointWriteResult.ok(1);
            }
            String err = response.getError();
            if (err != null && err.contains("not found")) {
                return FastPathExecutor.PointWriteResult.notFound();
            }
            return FastPathExecutor.PointWriteResult.conflict(err);
        } catch (Exception e) {
            log.debug(
                    "GrpcExecutorStub pointDelete failed for table={}: {}",
                    tableId,
                    e.getMessage());
            throw new RuntimeException("Point delete failed: " + e.getMessage(), e);
        }
    }

    @Override
    public FastPathExecutor.PointWriteResult pointInsert(
            String tableId,
            byte[] encodedKey,
            byte[] rowData,
            Map<String, Comparable<?>> pkValues,
            List<String> primaryKeyCols,
            long snapshotTs,
            long shardEpoch) {
        return pointInsert(
                tableId,
                encodedKey,
                rowData,
                pkValues,
                null,
                primaryKeyCols,
                snapshotTs,
                shardEpoch);
    }

    @Override
    public FastPathExecutor.PointWriteResult pointInsert(
            String tableId,
            byte[] encodedKey,
            byte[] rowData,
            Map<String, Comparable<?>> pkValues,
            Map<String, Comparable<?>> rowValues,
            List<String> primaryKeyCols,
            long snapshotTs,
            long shardEpoch) {
        try {
            pkValues = normalizeBySchema(pkValues);
            rowValues = normalizeBySchema(rowValues);
            PointInsertRequest.Builder reqBuilder =
                    PointInsertRequest.newBuilder().setTableId(tableId).setSnapshotTs(snapshotTs);
            if (encodedKey != null) {
                // Zero-copy: encodedKey is newly allocated by OrderPreservingKeyEncoder.encode,
                // will not be reused
                reqBuilder.setKey(UnsafeByteOperations.unsafeWrap(encodedKey));
            }
            if (rowData != null && (rowValues == null || rowValues.isEmpty())) {
                // Zero-copy: rowData is newly allocated by RecordCodec.encode
                reqBuilder.setData(UnsafeByteOperations.unsafeWrap(rowData));
            }
            if (pkValues != null) {
                for (var e : pkValues.entrySet()) {
                    reqBuilder.putPkValues(e.getKey(), toProtoLiteral(e.getValue()));
                }
            }
            if (rowValues != null) {
                for (var e : rowValues.entrySet()) {
                    if (e.getValue() != null) {
                        reqBuilder.putRowValues(e.getKey(), toProtoLiteral(e.getValue()));
                    }
                }
            }
            if (primaryKeyCols != null) {
                reqBuilder.addAllPrimaryKeyCols(primaryKeyCols);
            }
            // Approach A: inject schema to enable server side RecordCodec.encode
            io.github.rawwise.frontend.ir.routing.FastPathExecutor.SchemaContext sc =
                    io.github.rawwise.frontend.ir.routing.FastPathExecutor.CURRENT_SCHEMA.get();
            if (sc != null && sc.schema() != null) {
                reqBuilder.setSchema(
                        SCHEMA_CACHE.computeIfAbsent(
                                sc.numericTableId(),
                                k ->
                                        io.github.rawwise.model.record.ProtoTableMetaConverter
                                                .toPbStructType(sc.schema())));
                reqBuilder.setNumericTableId(sc.numericTableId());
            }
            PointInsertResponse response =
                    stub.withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
                            .pointInsert(reqBuilder.build());
            if (response.getSuccess()) {
                return FastPathExecutor.PointWriteResult.ok(1);
            }
            return FastPathExecutor.PointWriteResult.conflict(response.getError());
        } catch (Exception e) {
            log.debug(
                    "GrpcExecutorStub pointInsert failed for table={}: {}",
                    tableId,
                    e.getMessage());
            throw new RuntimeException("Point insert failed: " + e.getMessage(), e);
        }
    }

    private static PbLiteralValue toProtoLiteral(Comparable<?> value) {
        PbLiteralValue.Builder b = PbLiteralValue.newBuilder();
        if (value == null) {
            return b.build();
        }
        if (value instanceof Boolean v) return b.setBoolVal(v).build();
        if (value instanceof Integer v) return b.setInt32Val(v).build();
        if (value instanceof Long v) return b.setInt64Val(v).build();
        if (value instanceof Float v) return b.setFloatVal(v).build();
        if (value instanceof Double v) return b.setDoubleVal(v).build();
        if (value instanceof String v) return b.setStringVal(v).build();
        return b.setStringVal(String.valueOf(value)).build();
    }

    /**
     * Normalize values in a map using the schema from CURRENT_SCHEMA ThreadLocal. Converts Long to
     * Integer for non-int64 columns so that the keyCodec on the server side produces the same
     * encoding as the Spark SQL write path (which uses Integer for INT columns). For int64 (BIGINT)
     * columns, Long is preserved to match Spark's Long encoding.
     */
    private static Map<String, Comparable<?>> normalizeBySchema(Map<String, Comparable<?>> values) {
        if (values == null || values.isEmpty()) return values;
        FastPathExecutor.SchemaContext sc = FastPathExecutor.CURRENT_SCHEMA.get();
        io.github.rawwise.model.record.StructType schema = (sc != null) ? sc.schema() : null;
        Map<String, Comparable<?>> out = new java.util.LinkedHashMap<>();
        for (var e : values.entrySet()) {
            Comparable<?> v = e.getValue();
            if (v instanceof Long l && l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
                boolean isInt64 = false;
                if (schema != null) {
                    io.github.rawwise.model.record.StructField f =
                            schema.field(e.getKey()).orElse(null);
                    if (f != null && "int64".equals(f.dataType().name())) {
                        isInt64 = true;
                    }
                }
                if (!isInt64) {
                    v = Integer.valueOf(l.intValue());
                }
            }
            out.put(e.getKey(), v);
        }
        return out;
    }
}
