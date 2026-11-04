package io.github.rawwise.backend.executor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.github.rawwise.axiom.pipeline.derivation.DerivationChain;
import io.github.rawwise.axiom.pipeline.derivation.DerivationEngine;
import io.github.rawwise.axiom.pipeline.derivation.KeywordIndex;
import io.github.rawwise.axiom.pipeline.derivation.Layer;
import io.github.rawwise.axiom.pipeline.derivation.RederiveService;
import io.github.rawwise.axiom.pipeline.feedback.FeedbackLoopCloser;
import io.github.rawwise.axiom.pipeline.feedback.LoopRecord;
import io.github.rawwise.axiom.pipeline.ingestion.IngestionEngine;
import io.github.rawwise.backend.replication.GrpcWalReplicator;
import io.github.rawwise.common.collection.iterator.CloseableIterator;
import io.github.rawwise.common.hash.MurmurHash3;
import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.PostWriteHook;
import io.github.rawwise.engine.storage.RoutingOwnerContext;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.codec.RecordDecoder;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import io.github.rawwise.engine.txn.SimpleTableRef;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnContextHolder;
import io.github.rawwise.engine.txn.wal.FastPathWalWriter;
import io.github.rawwise.engine.vector.JVectorVectorIndex;
import io.github.rawwise.engine.vector.ShardCentroids;
import io.github.rawwise.engine.vector.VectorIndex;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.model.op.Find;
import io.github.rawwise.model.op.KvWriteBuilder;
import io.github.rawwise.model.record.DefaultBody;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.RecordCodec;
import io.github.rawwise.model.record.Records;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.SemanticRecord;
import io.github.rawwise.model.record.Types;
import io.github.rawwise.server.AxiomIngestRequest;
import io.github.rawwise.server.AxiomIngestResponse;
import io.github.rawwise.server.BatchPointQueryRequest;
import io.github.rawwise.server.BatchPointQueryResponse;
import io.github.rawwise.server.BatchPointUpdateRequest;
import io.github.rawwise.server.BatchPointUpdateResponse;
import io.github.rawwise.server.CognizeRequest;
import io.github.rawwise.server.CognizeResponse;
import io.github.rawwise.server.EvaluateRequest;
import io.github.rawwise.server.EvaluateResponse;
import io.github.rawwise.server.ExecutorPointQueryServiceGrpc;
import io.github.rawwise.server.ExecutorRederiveRequest;
import io.github.rawwise.server.ExecutorRederiveResponse;
import io.github.rawwise.server.FeedbackRequest;
import io.github.rawwise.server.FeedbackResponse;
import io.github.rawwise.server.GetShardEpochRequest;
import io.github.rawwise.server.GetShardEpochResponse;
import io.github.rawwise.server.KeyBatch;
import io.github.rawwise.server.KeywordSearchHit;
import io.github.rawwise.server.KeywordSearchRequest;
import io.github.rawwise.server.KeywordSearchResponse;
import io.github.rawwise.server.MigrateBatchRequest;
import io.github.rawwise.server.MigrateBatchResponse;
import io.github.rawwise.server.PointDeleteRequest;
import io.github.rawwise.server.PointDeleteResponse;
import io.github.rawwise.server.PointInsertRequest;
import io.github.rawwise.server.PointInsertResponse;
import io.github.rawwise.server.PointQueryRequest;
import io.github.rawwise.server.PointQueryResponse;
import io.github.rawwise.server.PointQueryResult;
import io.github.rawwise.server.PointUpdateRequest;
import io.github.rawwise.server.PointUpdateResponse;
import io.github.rawwise.server.ReplicateAck;
import io.github.rawwise.server.ScanKeysRequest;
import io.github.rawwise.server.SnapshotChunk;
import io.github.rawwise.server.SnapshotRequest;
import io.github.rawwise.server.VectorSearchRequest;
import io.github.rawwise.server.VectorSearchResponse;
import io.github.rawwise.server.VectorSearchResult;
import io.github.rawwise.server.WalEntry;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class ExecutorPointQueryServiceImpl
        extends ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceImplBase {

    private static final String COL_ID = "id";
    private static final String EXECUTOR_OWNER = "point-query-exec";
    private static final String DEFAULT_STORE_ID = "spark";
    private static final int CENTROID_BUILD_THRESHOLD = 16;

    private final String warehousePath;
    private final RawWiseConfig config;
    private final ConcurrentHashMap<String, StoreEntry> storeCache = new ConcurrentHashMap<>();
    private volatile FastPathWalWriter fastPathWalWriter;
    // P2: 每个 tableIdentifier 的本地 routingTableEpoch，初始 1。
    // 实际生产由 Backend Server 通过推送/周期刷新更新；当前提供 RPC 接口可被外部设置。
    private final ConcurrentHashMap<String, Long> routingTableEpochs = new ConcurrentHashMap<>();

    private static class StoreEntry {
        final DynamicMmapStore store;
        final RoutingOwnerContext ownerContext;
        volatile TableStore tableStore;
        volatile KeyCodec keyCodec;

        StoreEntry(DynamicMmapStore store) {
            this.store = store;
            this.ownerContext = new RoutingOwnerContext(EXECUTOR_OWNER, 1);
        }

        TableStore ensureTableStore(String tableId) {
            TableStore ts = tableStore;
            if (ts == null) {
                synchronized (this) {
                    ts = tableStore;
                    if (ts == null) {
                        ts = store.tableStore(tableId, List.of(COL_ID));
                        tableStore = ts;
                        keyCodec = ts.getKeyCodec();
                        if (store.getSnapshotManager() != null) {
                            try {
                                store.recoverColdBlocksForTable(tableId);
                            } catch (Exception e) {
                                log.warn(
                                        "Cold block recovery failed for table {}: {}",
                                        tableId,
                                        e.getMessage());
                            }
                        }
                    }
                }
            }
            return ts;
        }
    }

    public ExecutorPointQueryServiceImpl(RawWiseConfig config) {
        this.warehousePath = io.github.rawwise.governance.config.WarehousePathResolver.resolve();
        this.config = config;
    }

    /** Inject FastPathWalWriter for group-commit WAL persistence on point-query write path. */
    private GrpcWalReplicator walReplicator;

    public void setWalReplicator(GrpcWalReplicator replicator) {
        this.walReplicator = replicator;
    }

    public void setFastPathWalWriter(FastPathWalWriter writer) {
        this.fastPathWalWriter = writer;
    }

    /**
     * begin(): 分配唯一 commitTs 并设置 ThreadLocal，使后续 encodeAsRecordCodec / readMergeEncode 能通过
     * TxnContextHolder.current().txnId() 拿到 commitTs。 调用者必须在编码完成后调用 clear() 脱离事务，避免 store.write()
     * 走事务缓冲路径。 返回 TxnContext，调用者通过 ctx.txnId() 获取 commitTs。
     */
    private TxnContext allocateCommitTs(String tableId, String[] errMsg) {
        TxnContextHolder.ensureClockSet();
        try {
            return TxnContextHolder.begin(
                    Set.of(tableId),
                    io.github.rawwise.engine.txn.IsolationLevel.READ_COMMITTED,
                    config,
                    "fastpath-" + tableId);
        } catch (Exception e) {
            errMsg[0] = "TxnContext allocation failed: " + e.getMessage();
            return null;
        }
    }

    private StoreEntry getOrCreateStore(String tableId) {
        return storeCache.computeIfAbsent(
                tableId,
                id -> {
                    DynamicMmapStore store =
                            DynamicMmapStore.getInstance(warehousePath, DEFAULT_STORE_ID, config);
                    if (!store.isStarted()) {
                        store.start();
                    }
                    return new StoreEntry(store);
                });
    }

    private static String normalizeTableId(String tableId) {
        String[] parts = tableId.split("\\.");
        if (parts.length >= 3) {
            return parts[1] + "." + parts[2];
        }
        return tableId;
    }

    /**
     * 把 request 携带的 schema 注册到 TableStore（如果 ts 还没有 SchemaResolver）。 让后续 doFindByKey /
     * readMergeEncode 经 RecordDecoder 都能解析 RecordCodec 字节。 等价于"FastPath 客户端把表 schema marshal 过来就地注入
     * server SchemaResolver"。
     */
    private void maybeRegisterSchema(
            TableStore ts, io.github.rawwise.meta.PbStructType pbSchema, long numericTableId) {
        if (pbSchema == null || pbSchema.getFieldsCount() == 0) return;
        try {
            io.github.rawwise.model.record.StructType structType =
                    io.github.rawwise.model.record.ProtoTableMetaConverter.fromPbStructType(
                            pbSchema);
            Schema schema = new Schema(structType);
            long resolveTableId = numericTableId > 0 ? numericTableId : ts.getNumericTableId();
            // 把 schema 装到一个 SchemaResolver 闭包里，调 ts.setSchemaResolverSupplier 注册。
            ts.setSchemaResolverSupplier(
                    () ->
                            (tid, ver) -> {
                                if (tid == resolveTableId) return schema;
                                return null;
                            });
        } catch (Exception e) {
            log.debug("maybeRegisterSchema failed: {}", e.getMessage());
        }
    }

    @Override
    public void pointQuery(
            PointQueryRequest request, StreamObserver<PointQueryResponse> responseObserver) {
        try {
            String normalizedId = normalizeTableId(request.getTableId());
            StoreEntry entry = getOrCreateStore(normalizedId);
            // 优先用 pk_values map（支持复合 PK）；否则 fallback 到 legacy 单 PK byte[] key 路径。
            Map<String, Comparable<?>> pkValues = literalMapToComparable(request.getPkValuesMap());
            List<String> pkCols = request.getPrimaryKeyColsList();
            TableStore ts;
            byte[] encodedKey;
            int legacyKey = 0;
            if (!pkValues.isEmpty()) {
                if (pkCols.isEmpty()) {
                    pkCols = new java.util.ArrayList<>(pkValues.keySet());
                }
                ts = entry.store.tableStore(normalizedId, pkCols);
                // 注册 schema 让后续 doFindByKey 能解析 RecordCodec 字节（方案 A 关键）
                if (request.hasSchema()) {
                    maybeRegisterSchema(ts, request.getSchema(), request.getNumericTableId());
                }
                Map<String, Comparable> typed = new java.util.LinkedHashMap<>();
                for (var e : pkValues.entrySet()) {
                    typed.put(e.getKey(), e.getValue());
                }
                encodedKey = ts.getKeyCodec().encodePrimaryKey(typed);
            } else if (request.getRawKey()) {
                ts = entry.ensureTableStore(normalizedId);
                encodedKey = request.getKey().toByteArray();
            } else {
                ts = entry.ensureTableStore(normalizedId);
                legacyKey = bytesToInt(request.getKey().toByteArray());
                encodedKey =
                        ts.getKeyCodec()
                                .encodePrimaryKey(Map.of(COL_ID, (Comparable) (Integer) legacyKey));
            }
            Find.Result result = doFindByKey(ts, encodedKey, legacyKey);

            PointQueryResponse.Builder resp = PointQueryResponse.newBuilder();
            // P2: 在 response 中返回本地 epoch，让 Frontend 端检测 staleness
            long currentEpoch = getCurrentEpoch(normalizedId);
            resp.setEpoch(currentEpoch);
            if (result.success() && result.foundRecord().isPresent()) {
                Record record = result.foundRecord().get();
                String json = recordToJson(record);
                resp.setData(ByteString.copyFrom(json.getBytes(StandardCharsets.UTF_8)));
                resp.setFound(true);
            } else if (result.success()
                    && result.rawNullableValue() != null
                    && result.rawNullableValue().length > 0) {
                resp.setData(ByteString.copyFrom(result.rawNullableValue()));
                resp.setFound(true);
            }
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("PointQuery failed for table={}", request.getTableId(), e);
            responseObserver.onError(e);
        }
    }

    /** P2: 获取本地 routingTableEpoch（初始 1）。 实际生产中 Backend Server 通过推送或周期刷新更新该值。 */
    private long getCurrentEpoch(String tableId) {
        return routingTableEpochs.computeIfAbsent(tableId, k -> 1L);
    }

    /** P2: 设置 tableId 的本地 routingTableEpoch（由外部推送调用）。 */
    public void setRoutingTableEpoch(String tableId, long epoch) {
        if (epoch < 0) return;
        routingTableEpochs.put(tableId, epoch);
    }

    @Override
    public void getShardEpoch(
            GetShardEpochRequest request, StreamObserver<GetShardEpochResponse> responseObserver) {
        try {
            String normalizedId = normalizeTableId(request.getTableId());
            long epoch = getCurrentEpoch(normalizedId);
            responseObserver.onNext(GetShardEpochResponse.newBuilder().setEpoch(epoch).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("GetShardEpoch failed for table={}", request.getTableId(), e);
            responseObserver.onError(e);
        }
    }

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private String recordToJson(Record record) {
        try {
            return JSON_MAPPER.writeValueAsString(record.body().toMap());
        } catch (Exception e) {
            log.debug("Failed to convert record to JSON: {}", e.getMessage());
            return "{}";
        }
    }

    @Override
    public void batchPointQuery(
            BatchPointQueryRequest request,
            StreamObserver<BatchPointQueryResponse> responseObserver) {
        try {
            String normalizedId = normalizeTableId(request.getTableId());
            StoreEntry entry = getOrCreateStore(normalizedId);
            TableStore ts = entry.ensureTableStore(normalizedId);

            List<PointQueryResult> results = new ArrayList<>();
            for (ByteString keyBytes : request.getKeysList()) {
                int key = bytesToInt(keyBytes.toByteArray());
                Find.Result result = doFind(ts, key);
                PointQueryResult.Builder r =
                        PointQueryResult.newBuilder().setKey(keyBytes).setFound(false);
                if (result.success() && result.foundRecord().isPresent()) {
                    byte[] raw = result.rawNullableValue();
                    if (raw != null) {
                        r.setData(ByteString.copyFrom(raw));
                        r.setFound(true);
                    }
                }
                results.add(r.build());
            }
            responseObserver.onNext(
                    BatchPointQueryResponse.newBuilder().addAllResults(results).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("BatchPointQuery failed for table={}", request.getTableId(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void pointUpdate(
            PointUpdateRequest request, StreamObserver<PointUpdateResponse> responseObserver) {
        String normalizedId = normalizeTableId(request.getTableId());
        StoreEntry entry = getOrCreateStore(normalizedId);
        DynamicMmapStore store = entry.store;
        List<String> pkCols = request.getPrimaryKeyColsList();
        if (pkCols.isEmpty()) {
            pkCols = List.of(COL_ID);
        }
        TableStore ts = store.tableStore(normalizedId, pkCols);
        if (request.hasSchema()) {
            maybeRegisterSchema(ts, request.getSchema(), request.getNumericTableId());
        }
        KeyCodec keyCodec = ts.getKeyCodec();

        Map<String, Comparable<?>> pkValues = literalMapToComparable(request.getPkValuesMap());
        byte[] encodedKey;
        try {
            if (!pkValues.isEmpty()) {
                Map<String, Comparable> typed = new java.util.LinkedHashMap<>();
                for (var e : pkValues.entrySet()) {
                    typed.put(e.getKey(), e.getValue());
                }
                encodedKey = keyCodec.encodePrimaryKey(typed);
            } else if (request.getKey().size() > 0) {
                int key = bytesToInt(request.getKey().toByteArray());
                encodedKey = keyCodec.encodePrimaryKey(Map.of(COL_ID, (Comparable) key));
            } else {
                responseObserver.onNext(
                        PointUpdateResponse.newBuilder()
                                .setSuccess(false)
                                .setError("missing pk values")
                                .build());
                responseObserver.onCompleted();
                return;
            }
        } catch (IOException e) {
            log.error("PointUpdate key encoding failed for table={}", normalizedId, e);
            responseObserver.onNext(
                    PointUpdateResponse.newBuilder()
                            .setSuccess(false)
                            .setError("key encoding failed: " + e.getMessage())
                            .build());
            responseObserver.onCompleted();
            return;
        }

        byte[] valueBytes;
        Map<String, Comparable<?>> setClauses = literalMapToComparable(request.getSetClausesMap());

        TxnContext ctx = allocateCommitTs(normalizedId, new String[1]);
        if (ctx == null) {
            responseObserver.onNext(
                    PointUpdateResponse.newBuilder()
                            .setSuccess(false)
                            .setError("failed to allocate commitTs")
                            .build());
            responseObserver.onCompleted();
            return;
        }
        try {
            if (!setClauses.isEmpty()) {
                byte[] mergedRecord =
                        readMergeEncode(ts, encodedKey, setClauses, pkValues, normalizedId);
                if (mergedRecord == null) {
                    responseObserver.onNext(
                            PointUpdateResponse.newBuilder()
                                    .setSuccess(false)
                                    .setError("not found")
                                    .build());
                    responseObserver.onCompleted();
                    return;
                }
                valueBytes = mergedRecord;
            } else {
                valueBytes = request.getData().toByteArray();
            }
            // WAL 写入由 WalWriteAdapter.executeWithWalGuard() 在 store.write() 路径中统一处理，
            // 不再在此处通过 fastPathWalWriter 重复写入。
        } finally {
            TxnContextHolder.clear();
        }

        KvWriteBuilder builder =
                KvWriteBuilder.builder()
                        .table(new SimpleTableRef(normalizedId))
                        .key(encodedKey)
                        .value(valueBytes)
                        .partitionHint(0);
        store.write(builder, entry.ownerContext);

        responseObserver.onNext(PointUpdateResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void pointDelete(
            PointDeleteRequest request, StreamObserver<PointDeleteResponse> responseObserver) {
        try {
            String normalizedId = normalizeTableId(request.getTableId());
            StoreEntry entry = getOrCreateStore(normalizedId);
            DynamicMmapStore store = entry.store;
            List<String> pkCols = request.getPrimaryKeyColsList();
            if (pkCols.isEmpty()) {
                pkCols = List.of(COL_ID);
            }
            TableStore ts = store.tableStore(normalizedId, pkCols);
            if (request.hasSchema()) {
                maybeRegisterSchema(ts, request.getSchema(), request.getNumericTableId());
            }
            KeyCodec keyCodec = ts.getKeyCodec();

            Map<String, Comparable<?>> pkValues = literalMapToComparable(request.getPkValuesMap());
            byte[] encodedKey;
            if (!pkValues.isEmpty()) {
                Map<String, Comparable> typed = new java.util.LinkedHashMap<>();
                for (var e : pkValues.entrySet()) {
                    typed.put(e.getKey(), e.getValue());
                }
                encodedKey = keyCodec.encodePrimaryKey(typed);
            } else if (request.getKey().size() > 0) {
                int key = bytesToInt(request.getKey().toByteArray());
                encodedKey = keyCodec.encodePrimaryKey(Map.of(COL_ID, (Comparable) key));
            } else {
                responseObserver.onNext(
                        PointDeleteResponse.newBuilder()
                                .setSuccess(false)
                                .setError("missing pk values")
                                .build());
                responseObserver.onCompleted();
                return;
            }

            // Verify existence (so caller gets NotFound semantics).
            byte[] existing = ts.getRawValue(encodedKey);
            if (existing == null || NullableValueCodec.isTombstone(existing)) {
                responseObserver.onNext(
                        PointDeleteResponse.newBuilder()
                                .setSuccess(false)
                                .setError("not found")
                                .build());
                responseObserver.onCompleted();
                return;
            }

            store.delete(normalizedId, encodedKey, entry.ownerContext);

            responseObserver.onNext(PointDeleteResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("PointDelete failed for table={}", request.getTableId(), e);
            responseObserver.onNext(
                    PointDeleteResponse.newBuilder()
                            .setSuccess(false)
                            .setError(
                                    e.getMessage() != null
                                            ? e.getMessage()
                                            : e.getClass().getSimpleName())
                            .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void pointInsert(
            PointInsertRequest request, StreamObserver<PointInsertResponse> responseObserver) {
        String normalizedId = normalizeTableId(request.getTableId());
        StoreEntry entry = getOrCreateStore(normalizedId);
        DynamicMmapStore store = entry.store;
        List<String> pkCols = request.getPrimaryKeyColsList();
        if (pkCols.isEmpty()) {
            pkCols = List.of(COL_ID);
        }
        TableStore ts = store.tableStore(normalizedId, pkCols);
        if (request.hasSchema()) {
            maybeRegisterSchema(ts, request.getSchema(), request.getNumericTableId());
        }
        KeyCodec keyCodec = ts.getKeyCodec();

        Map<String, Comparable<?>> pkValues = literalMapToComparable(request.getPkValuesMap());
        byte[] encodedKey;
        try {
            if (!pkValues.isEmpty()) {
                Map<String, Comparable> typed = new java.util.LinkedHashMap<>();
                for (var e : pkValues.entrySet()) {
                    typed.put(e.getKey(), e.getValue());
                }
                encodedKey = keyCodec.encodePrimaryKey(typed);
            } else if (request.getKey().size() > 0) {
                int key = bytesToInt(request.getKey().toByteArray());
                encodedKey = keyCodec.encodePrimaryKey(Map.of(COL_ID, (Comparable) key));
            } else {
                responseObserver.onNext(
                        PointInsertResponse.newBuilder()
                                .setSuccess(false)
                                .setError("missing pk values")
                                .build());
                responseObserver.onCompleted();
                return;
            }
        } catch (IOException e) {
            log.error("PointInsert key encoding failed for table={}", normalizedId, e);
            responseObserver.onNext(
                    PointInsertResponse.newBuilder()
                            .setSuccess(false)
                            .setError("key encoding failed: " + e.getMessage())
                            .build());
            responseObserver.onCompleted();
            return;
        }

        byte[] valueBytes;
        Map<String, Comparable<?>> rowValues = literalMapToComparable(request.getRowValuesMap());

        TxnContext ctx = allocateCommitTs(normalizedId, new String[1]);
        if (ctx == null) {
            responseObserver.onNext(
                    PointInsertResponse.newBuilder()
                            .setSuccess(false)
                            .setError("failed to allocate commitTs")
                            .build());
            responseObserver.onCompleted();
            return;
        }
        try {
            if (!rowValues.isEmpty()) {
                Schema requestSchema = null;
                long requestNumericTableId = request.getNumericTableId();
                if (request.hasSchema()) {
                    io.github.rawwise.model.record.StructType structType =
                            io.github.rawwise.model.record.ProtoTableMetaConverter.fromPbStructType(
                                    request.getSchema());
                    requestSchema = new Schema(structType);
                }
                valueBytes =
                        encodeAsRecordCodec(
                                ts,
                                encodedKey,
                                rowValues,
                                normalizedId,
                                Record.Kind.INSERT,
                                requestSchema,
                                requestNumericTableId);
                if (valueBytes == null) {
                    responseObserver.onNext(
                            PointInsertResponse.newBuilder()
                                    .setSuccess(false)
                                    .setError("failed to resolve schema or encode record")
                                    .build());
                    responseObserver.onCompleted();
                    return;
                }
            } else {
                valueBytes = request.getData().toByteArray();
            }
            // WAL 写入由 WalWriteAdapter.executeWithWalGuard() 在 store.write() 路径中统一处理，
            // 不再在此处通过 fastPathWalWriter 重复写入。
            // WalWriteAdapter 有更完整的错误处理（HDFS 不可用检查、failure policy、group commit）。
        } finally {
            TxnContextHolder.clear();
        }

        KvWriteBuilder builder =
                KvWriteBuilder.builder()
                        .table(new SimpleTableRef(normalizedId))
                        .key(encodedKey)
                        .value(valueBytes)
                        .partitionHint(0);
        store.write(builder, entry.ownerContext);

        if (walReplicator != null) {
            try {
                java.nio.ByteBuffer walBuf =
                        java.nio.ByteBuffer.allocate(4 + encodedKey.length + valueBytes.length);
                walBuf.putInt(encodedKey.length);
                walBuf.put(encodedKey);
                walBuf.put(valueBytes);
                io.github.rawwise.backend.wal.WALRecord walRecord =
                        new io.github.rawwise.backend.wal.DefaultWALRecord(
                                System.nanoTime(),
                                walBuf.array(),
                                System.currentTimeMillis(),
                                0L,
                                normalizedId,
                                "INSERT");
                walReplicator.replicateToAllReplicas(walRecord);
            } catch (Exception e) {
                log.debug("WAL replication failed for table={}: {}", normalizedId, e.getMessage());
            }
        }

        responseObserver.onNext(PointInsertResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    /**
     * 字节布局完全一致（含 RecordHeader + commitTs + sequenceNumber + Kind）。
     *
     * <p>对比：
     *
     * <ul>
     *   <li>JSON 路径（旧）：{@code {"id":1,"name":"alpha"}} —— 无 header / 不含 MVCC 元信息 / Compaction
     *       路径无法识别 / AutoDerivationHook 接 null record。
     *   <li>RecordCodec 路径（本方法）：完整 Record，可被 Compaction / Flush / AutoDerivationHook / MVCC
     *       可见性判定全链路识别。
     * </ul>
     *
     * @return 编码后的字节，或 null（schema 解析失败 / 编码失败）。返回的字节 <b>不</b>外包 NullableValueCodec ——
     *     LmdbWriter.write 路径会负责包装一次。
     */
    private byte[] encodeAsRecordCodec(
            TableStore ts,
            byte[] encodedKey,
            Map<String, Comparable<?>> rowValues,
            String tableId,
            Record.Kind kind,
            Schema requestSchema,
            long requestNumericTableId) {
        try {
            RecordCodec recordCodec = ts.getRecordCodec();
            // 优先用 request 中的 schema/numericTableId（FastPath frontend 已 marshal 完整表元信息）；
            // server 端没注册 SchemaResolver 时这是唯一可用 schema。
            Schema schema = requestSchema;
            long numericTableId =
                    requestNumericTableId > 0 ? requestNumericTableId : ts.getNumericTableId();
            if (schema == null) {
                // 回退：尝试用 ts.getSchemaResolver() 解析（仅在 server 已注册 SchemaResolver 时可行）。
                try {
                    RecordCodec.SchemaResolver schemaResolver = ts.getSchemaResolver();
                    schema = schemaResolver.resolve(numericTableId, 0L);
                } catch (Exception e) {
                    log.warn(
                            "encodeAsRecordCodec: schemaResolver unavailable for tableId={}: {}",
                            tableId,
                            e.getMessage());
                }
            }
            if (schema == null) {
                log.warn(
                        "encodeAsRecordCodec: no schema available for tableId={}; require request.schema or registered SchemaResolver",
                        tableId);
                return null;
            }

            Object[] values = new Object[schema.fields().size()];
            for (int i = 0; i < schema.fields().size(); i++) {
                values[i] = rowValues.get(schema.fields().get(i).name());
            }
            Record.Body body = new DefaultBody(schema, values);
            Record.Key key = Records.keyOf(encodedKey);

            // commitTs ≡ txnId (unified strategy)
            long commitTs =
                    TxnContextHolder.hasActive()
                            ? TxnContextHolder.current().txnId()
                            : Long.MIN_VALUE;
            Record record = Records.create(key, body, numericTableId, 0L, commitTs, kind, true);
            return recordCodec.encode(record);
        } catch (Exception e) {
            log.warn("encodeAsRecordCodec failed for tableId={}: {}", tableId, e.getMessage(), e);
            return null;
        }
    }

    /**
     * Read existing record, merge SET clauses, return encoded NullableValue bytes. Returns null if
     * record not found.
     */
    private byte[] readMergeEncode(
            TableStore ts,
            byte[] encodedKey,
            Map<String, Comparable<?>> setClauses,
            Map<String, Comparable<?>> pkValues,
            String tableId) {
        byte[] raw = ts.getRawValue(encodedKey);
        if (raw == null || NullableValueCodec.isTombstone(raw)) {
            return null;
        }

        // Legacy 兼容：方案 A 之前的 FastPath INSERT 写入 JSON 字节，UPDATE 时也用 JSON merge。
        // 方案 A 后所有 FastPath INSERT 走 RecordCodec，UPDATE 走 RecordCodec merge（下方分支）。
        // 保留 JSON 分支仅为兼容历史数据；下个 sprint 可彻底删除。
        byte[] unwrapped;
        try {
            unwrapped = NullableValueCodec.decode(raw);
        } catch (Exception e) {
            unwrapped = null;
        }
        if (unwrapped != null && unwrapped.length > 0 && unwrapped[0] == '{') {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> currentMap = JSON_MAPPER.readValue(unwrapped, Map.class);
                Map<String, Object> merged = new java.util.LinkedHashMap<>(currentMap);
                for (var e : setClauses.entrySet()) {
                    merged.put(e.getKey(), e.getValue());
                }
                byte[] mergedJson = JSON_MAPPER.writeValueAsBytes(merged);
                // 直接返回 raw JSON：LmdbWriter 会用 NullableValueCodec.encode 包装一次。
                return mergedJson;
            } catch (Exception e) {
                log.warn("readMergeEncode JSON merge failed: {}", e.getMessage());
                return null;
            }
        }
        log.debug(
                "readMergeEncode: NOT json path, unwrapped.firstByte={}",
                unwrapped == null || unwrapped.length == 0
                        ? "n/a"
                        : String.format("0x%02x", unwrapped[0]));

        // RecordCodec 二进制路径（兼容历史写入格式）
        RecordCodec recordCodec = ts.getRecordCodec();
        RecordCodec.SchemaResolver schemaResolver = ts.getSchemaResolver();
        RecordDecoder.DecodeResult decodeResult =
                RecordDecoder.decodeNullableWrapped(raw, recordCodec, schemaResolver);
        if (!(decodeResult instanceof RecordDecoder.DecodeResult.Found f)) {
            return null;
        }
        Record current = f.record();
        Map<String, Object> currentMap = current.body().toMap();
        Map<String, Object> merged = new java.util.LinkedHashMap<>(currentMap);
        for (var e : setClauses.entrySet()) {
            merged.put(e.getKey(), e.getValue());
        }
        Schema schema = current.body().schema();
        Object[] values = new Object[schema.fields().size()];
        for (int i = 0; i < schema.fields().size(); i++) {
            values[i] = merged.get(schema.fields().get(i).name());
        }
        Record.Body body = new DefaultBody(schema, values);
        // commitTs ≡ txnId (unified strategy), sourced from active TxnContext.
        // Falls back to Long.MIN_VALUE as unknown sentinel when no context is active —
        // pointUpdate may be invoked outside the Spark transaction pipeline.
        long commitTs =
                TxnContextHolder.hasActive() ? TxnContextHolder.current().txnId() : Long.MIN_VALUE;
        Record updated =
                Records.create(
                        current.key(),
                        body,
                        current.header().tableId(),
                        current.header().sequenceNumber() + 1,
                        commitTs,
                        Record.Kind.UPDATE,
                        true,
                        current.header().immutable(),
                        current.header().immutableSource());
        byte[] encoded = recordCodec.encode(updated);
        // 直接返回 RecordCodec 字节；LmdbWriter 会经 NullableValueCodec.encode 包装一次。
        // 不能在此处再包一次，否则 LMDB 中是双重 NullableValueCodec(NullableValueCodec(RecordCodec))，
        // 读路径 NullableValueCodec.decode 一次后还剩一层 tag，导致 RecordDecoder 检测 magic 失败。
        return encoded;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Comparable<?>> literalMapToComparable(
            Map<String, io.github.rawwise.meta.PbLiteralValue> proto) {
        Map<String, Comparable<?>> out = new java.util.LinkedHashMap<>();
        for (var e : proto.entrySet()) {
            out.put(e.getKey(), fromProtoLiteral(e.getValue()));
        }
        return out;
    }

    private static Comparable<?> fromProtoLiteral(io.github.rawwise.meta.PbLiteralValue lit) {
        return switch (lit.getValueCase()) {
            case BOOL_VAL -> Boolean.valueOf(lit.getBoolVal());
            case INT32_VAL -> Integer.valueOf(lit.getInt32Val());
                // 注意：保持 Long 不降级。早期版本曾尝试 INT64 fits Integer 时降级以兼容 SELECT
                // doFind 单 PK 4-byte 路径，但这会破坏 BIGINT PK 表写后读一致性
                // （当 PK > Integer.MAX_VALUE 时 INSERT 用 8-byte 编码，SELECT 仍用 4-byte 截断 → Miss）。
                // 现 SELECT 路径已统一走 pkValues + keyCodec.encodePrimaryKey，类型由 keyCodec 按 schema 决定。
            case INT64_VAL -> Long.valueOf(lit.getInt64Val());
            case FLOAT_VAL -> Float.valueOf(lit.getFloatVal());
            case DOUBLE_VAL -> Double.valueOf(lit.getDoubleVal());
            case STRING_VAL -> lit.getStringVal();
            case BYTES_VAL -> new String(lit.getBytesVal().toByteArray(), StandardCharsets.UTF_8);
            case VALUE_NOT_SET -> null;
        };
    }

    @Override
    public void batchPointUpdate(
            BatchPointUpdateRequest request,
            StreamObserver<BatchPointUpdateResponse> responseObserver) {
        try {
            String normalizedId = normalizeTableId(request.getTableId());
            StoreEntry entry = getOrCreateStore(normalizedId);
            TableStore ts = entry.ensureTableStore(normalizedId);
            KeyCodec keyCodec = ts.getKeyCodec();

            List<ByteString> keys = request.getKeysList();
            List<ByteString> values = request.getValuesList();
            int count = 0;
            for (int i = 0; i < keys.size(); i++) {
                int key = bytesToInt(keys.get(i).toByteArray());
                byte[] encodedKey = keyCodec.encodePrimaryKey(Map.of(COL_ID, (Comparable) key));
                KvWriteBuilder builder =
                        KvWriteBuilder.builder()
                                .table(new SimpleTableRef(normalizedId))
                                .key(encodedKey)
                                .value(values.get(i).toByteArray())
                                .partitionHint(0);
                entry.store.write(builder, entry.ownerContext);
                count++;
            }

            responseObserver.onNext(
                    BatchPointUpdateResponse.newBuilder().setSuccess(true).setCount(count).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("BatchPointUpdate failed for table={}", request.getTableId(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void vectorSearch(
            VectorSearchRequest request, StreamObserver<VectorSearchResponse> responseObserver) {
        try {
            String normalizedId = normalizeTableId(request.getTableId());
            StoreEntry entry = getOrCreateStore(normalizedId);
            DynamicMmapStore store = entry.store;

            VectorIndex vi = store.getVectorIndex();
            if (vi == null) {
                responseObserver.onError(
                        Status.UNAVAILABLE
                                .withDescription("VectorIndex not initialized")
                                .asRuntimeException());
                return;
            }

            float[] queryEmbedding = bytesToFloatArray(request.getQueryVector().toByteArray());
            List<io.github.rawwise.engine.vector.VectorSearchResult> results =
                    vi.search(request.getTableId(), queryEmbedding, request.getTopK());

            VectorSearchResponse.Builder resp = VectorSearchResponse.newBuilder();
            for (io.github.rawwise.engine.vector.VectorSearchResult r : results) {
                VectorSearchResult.Builder item =
                        VectorSearchResult.newBuilder()
                                .setKey(ByteString.copyFromUtf8(r.entryKey()))
                                .setSimilarity(r.score());
                if (r.l0Abstract() != null) {
                    item.putMetadata("summary", r.l0Abstract());
                }
                resp.addResults(item.build());
            }
            resp.setTotalMatches(results.size());
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT
                            .withDescription("Invalid search request: " + e.getMessage())
                            .asRuntimeException());
        } catch (Exception e) {
            log.error("Vector search failed for namespace={}", request.getTableId(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription("Vector search failed").asRuntimeException());
        }
    }

    @Override
    public void keywordSearch(
            KeywordSearchRequest request, StreamObserver<KeywordSearchResponse> responseObserver) {
        try {
            String normalizedId = normalizeTableId(request.getTableId());
            StoreEntry entry = getOrCreateStore(normalizedId);
            DynamicMmapStore store = entry.store;

            KeywordIndex ki = store.getKeywordIndex();
            if (ki == null) {
                KeywordSearchResponse resp = KeywordSearchResponse.newBuilder().build();
                responseObserver.onNext(resp);
                responseObserver.onCompleted();
                return;
            }

            List<KeywordIndex.KeywordHit> hits =
                    ki.search(request.getTableId(), request.getQuery(), request.getTopK());
            KeywordSearchResponse.Builder resp = KeywordSearchResponse.newBuilder();
            for (var hit : hits) {
                resp.addHits(
                        KeywordSearchHit.newBuilder()
                                .setDocId(hit.docId())
                                .setScore(hit.score())
                                .setSummary(ki.getSummary(request.getTableId(), hit.docId()))
                                .build());
            }
            responseObserver.onNext(resp.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Keyword search failed for table={}", request.getTableId(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription("Keyword search failed").asRuntimeException());
        }
    }

    @Override
    public void axiomIngest(
            AxiomIngestRequest request, StreamObserver<AxiomIngestResponse> responseObserver) {
        try {
            String normalizedId = normalizeTableId(request.getTableId());
            StoreEntry entry = getOrCreateStore(normalizedId);
            DynamicMmapStore store = entry.store;

            if (!store.isTableRegistered(normalizedId)) {
                responseObserver.onError(
                        Status.FAILED_PRECONDITION
                                .withDescription(
                                        "Table not registered: "
                                                + normalizedId
                                                + ". Run CREATE TABLE ... USING rawwise (with ai-native/derivation.strategy/primary-key TBLPROPERTIES) first to register table meta before /api/axiom/ingest.")
                                .asRuntimeException());
                return;
            }

            store.tableStore(normalizedId);
            if (!store.isTableAiNative(normalizedId)) {
                store.setTableAiNative(normalizedId, true);
            }
            store.applyTablePropertiesFromMetastore(normalizedId);
            store.registerAutoDeriveHook(normalizedId);

            byte[] keyBytes = request.getKey().toByteArray();
            if (!store.ensureWritable(normalizedId, keyBytes)) {
                responseObserver.onError(
                        new IllegalStateException("No writable instance available"));
                return;
            }
            if (store.isLmdbBacked()) {
                LmdbInstance instance = store.currentLmdbInstance(normalizedId, keyBytes);
                if (instance.currentLease() == null
                        || !instance.currentLease().isActive(System.currentTimeMillis())) {
                    instance.tryAcquireLease("axiom", 3600_000L);
                }
            }

            IngestionEngine engine = store.getIngestionEngine();
            if (engine == null) {
                responseObserver.onError(
                        new IllegalStateException("IngestionEngine not available"));
                return;
            }

            Record.Key key = Records.keyOf(keyBytes);
            Schema schema = Schema.builder().field("body", Types.binary(true)).build();
            Record.Body body =
                    new DefaultBody(schema, new Object[] {request.getBody().toByteArray()});
            Map<String, String> props = new HashMap<>(request.getPropertiesMap());

            IngestionEngine.IngestionRequest ingestionRequest =
                    new IngestionEngine.IngestionRequest(
                            key, body, 0L, request.getImmutable(), props, normalizedId);

            SemanticRecord record = engine.ingest(ingestionRequest);

            List<PostWriteHook> hooks = store.tableStore(normalizedId).getPostWriteHooks();
            log.info(
                    "axiomIngest dispatching to {} PostWriteHook(s) for table={} recordKey={}",
                    hooks.size(),
                    normalizedId,
                    request.getKey().toStringUtf8());
            for (PostWriteHook hook : hooks) {
                try {
                    hook.afterWrite(record, normalizedId);
                } catch (Exception e) {
                    log.error(
                            "PostWriteHook FAILED for table={} hook={}: {}",
                            normalizedId,
                            hook.getClass().getSimpleName(),
                            e.getMessage(),
                            e);
                }
            }

            int chunkCount = record.chunks() != null ? record.chunks().size() : 0;
            if (chunkCount == 0) {
                DerivationEngine de = store.getDerivationEngine();
                if (de != null) {
                    String rawId =
                            "raw:" + java.util.Base64.getEncoder().encodeToString(key.encoded());
                    List<DerivationChain> chains = de.getChain(rawId, Layer.RAW);
                    chunkCount =
                            (int)
                                    chains.stream()
                                            .filter(c -> c.derivedLayer() == Layer.CHUNK)
                                            .count();
                }
            }
            AxiomIngestResponse response =
                    AxiomIngestResponse.newBuilder()
                            .setRecordKey(new String(key.encoded()))
                            .setChunkCount(chunkCount)
                            .setStatus("OK")
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

            tryTriggerCentroidBuild(store, normalizedId);
        } catch (Exception e) {
            log.error("Axiom ingest failed on executor", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void axiomRederive(
            ExecutorRederiveRequest request,
            StreamObserver<ExecutorRederiveResponse> responseObserver) {
        try {
            String normalizedId = normalizeTableId(request.getTableId());
            StoreEntry entry = getOrCreateStore(normalizedId);
            DynamicMmapStore store = entry.store;

            RederiveService.Scope scope = RederiveService.Scope.values()[request.getScope()];
            byte[] startKey =
                    !request.getStartKey().isEmpty() ? request.getStartKey().toByteArray() : null;
            byte[] endKey =
                    !request.getEndKey().isEmpty() ? request.getEndKey().toByteArray() : null;
            byte[] singleKey =
                    !request.getSingleKey().isEmpty() ? request.getSingleKey().toByteArray() : null;

            RederiveService rederiveService = new RederiveService(store);
            RederiveService.RederiveResult result =
                    rederiveService.rederive(
                            normalizedId,
                            scope,
                            startKey,
                            endKey,
                            singleKey,
                            request.getForceReindex());

            ExecutorRederiveResponse response =
                    ExecutorRederiveResponse.newBuilder()
                            .setTotalKeys(result.totalKeys())
                            .setRederivedCount(result.rederivedCount())
                            .setFailedCount(result.failedCount())
                            .setStatus(result.status())
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Axiom rederive failed on executor", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void evaluate(
            EvaluateRequest request, StreamObserver<EvaluateResponse> responseObserver) {
        try {
            String viewId = request.getViewId();
            DynamicMmapStore store = getAnyStore();
            if (store == null) {
                responseObserver.onError(
                        Status.UNAVAILABLE
                                .withDescription("No DynamicMmapStore available")
                                .asRuntimeException());
                return;
            }

            FeedbackLoopCloser flc = store.getFeedbackLoopCloser();
            if (flc == null) {
                responseObserver.onError(
                        Status.UNAVAILABLE
                                .withDescription("FeedbackLoopCloser not initialized")
                                .asRuntimeException());
                return;
            }

            var view = flc.loadView(viewId);
            if (view == null) {
                responseObserver.onError(
                        Status.NOT_FOUND
                                .withDescription("View not found: " + viewId)
                                .asRuntimeException());
                return;
            }

            var evaluationEngine = store.getEvaluationEngine();
            if (evaluationEngine == null) {
                responseObserver.onError(
                        Status.UNAVAILABLE
                                .withDescription("EvaluationEngine not initialized")
                                .asRuntimeException());
                return;
            }
            var criteria = evaluationEngine.getDefaultCriteria(view.viewType());
            var result = evaluationEngine.evaluate(view, criteria);

            EvaluateResponse response =
                    EvaluateResponse.newBuilder()
                            .setQualityScore(result.qualityScore())
                            .setShouldCascade(
                                    flc.shouldCascade(
                                            Math.abs(
                                                    result.qualityScore()
                                                            - view.qualityScore().orElse(0.0))))
                            .setEvaluationId("eval-" + System.currentTimeMillis())
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Evaluate failed for viewId={}", request.getViewId(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void cognize(CognizeRequest request, StreamObserver<CognizeResponse> responseObserver) {
        try {
            DynamicMmapStore store = getAnyStore();
            if (store == null) {
                responseObserver.onError(
                        Status.UNAVAILABLE
                                .withDescription("No DynamicMmapStore available")
                                .asRuntimeException());
                return;
            }

            FeedbackLoopCloser flc = store.getFeedbackLoopCloser();
            if (flc == null) {
                responseObserver.onError(
                        Status.UNAVAILABLE
                                .withDescription(
                                        "FeedbackLoopCloser not initialized (CognitionEngine not available)")
                                .asRuntimeException());
                return;
            }

            String semanticId = request.getSemanticId();
            String actionType = request.getActionType();
            Map<String, String> params = request.getParamsMap();

            String resultStr = "cognize:" + semanticId + ":" + actionType;
            CognizeResponse response =
                    CognizeResponse.newBuilder()
                            .setActionId("action-" + System.currentTimeMillis())
                            .setResult(resultStr)
                            .setSuccess(true)
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Cognize failed for semanticId={}", request.getSemanticId(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void processFeedback(
            FeedbackRequest request, StreamObserver<FeedbackResponse> responseObserver) {
        try {
            String inputId = request.getViewId();
            DynamicMmapStore store = getAnyStore();
            if (store == null) {
                responseObserver.onError(
                        Status.UNAVAILABLE
                                .withDescription("No DynamicMmapStore available")
                                .asRuntimeException());
                return;
            }

            // 修复 wiring: 若 inputId 是 entryKey (rec:...)，从 metastore 反查真实 viewId
            // 映射由 AutoDerivationHook 在派生 view 后写入：key="entrykey2view:<entryKey>"
            String viewId = resolveViewIdFromEntryKey(store, inputId);
            if (!viewId.equals(inputId)) {
                log.info("Resolved entryKey={} → viewId={} for processFeedback", inputId, viewId);
            }

            FeedbackLoopCloser flc = store.getFeedbackLoopCloser();
            if (flc == null) {
                responseObserver.onError(
                        Status.UNAVAILABLE
                                .withDescription("FeedbackLoopCloser not initialized")
                                .asRuntimeException());
                return;
            }

            var annotation =
                    new io.github.rawwise.model.record.DefaultFeedbackAnnotation(
                            "ann-grpc-" + System.currentTimeMillis(),
                            viewId,
                            request.getSource(),
                            io.github.rawwise.model.record.FeedbackAnnotation.AnnotationType
                                    .valueOf(request.getAnnotationType()),
                            request.getAnnotationValue(),
                            1.0,
                            System.currentTimeMillis());

            LoopRecord record = flc.onAnnotation(viewId, annotation, request.getDepth());

            FeedbackResponse response =
                    FeedbackResponse.newBuilder()
                            .setLoopRecordId(viewId + ":" + record.timestamp())
                            .setCascadeTriggered(record.cascaded())
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("ProcessFeedback failed for viewId={}", request.getViewId(), e);
            responseObserver.onError(e);
        }
    }

    private DynamicMmapStore getAnyStore() {
        for (StoreEntry entry : storeCache.values()) {
            return entry.store;
        }
        return null;
    }

    /**
     * 修复 wiring: 若传入的 viewId 实际是 entryKey (以 "rec:" 开头)， 从 store 的 metastore 反查真实的 viewId（由
     * AutoDerivationHook 派生 view 时写入）。 若反查失败或本身就是真 viewId（"view:" 开头），返回原值。
     */
    private String resolveViewIdFromEntryKey(DynamicMmapStore store, String inputId) {
        if (inputId == null || inputId.isEmpty()) return inputId;
        if (inputId.startsWith("view:")) return inputId;
        if (!inputId.startsWith("rec:")) return inputId;
        if (store == null) return inputId;
        try {
            io.github.rawwise.metastore.Metastore ms = store.getMetastore();
            if (ms == null) return inputId;
            byte[] key =
                    (io.github.rawwise.axiom.pipeline.derivation.AutoDerivationHook
                                            .ENTRY_KEY_TO_VIEW_PREFIX
                                    + inputId)
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            java.util.Optional<byte[]> raw = ms.get(key);
            if (raw.isPresent()) {
                return new String(raw.get(), java.nio.charset.StandardCharsets.UTF_8);
            }
            return inputId;
        } catch (Exception e) {
            log.warn("entryKey→viewId lookup failed for {}: {}", inputId, e.getMessage());
            return inputId;
        }
    }

    public void flushSync(String tableId) {
        StoreEntry entry = storeCache.get(tableId);
        if (entry != null) {
            entry.store.triggerFlushSync();
        }
    }

    public void persistShardCentroids(String tableId) {
        StoreEntry entry = storeCache.get(tableId);
        if (entry == null) return;
        try {
            io.github.rawwise.metastore.Metastore metastore = entry.store.getMetastore();
            if (metastore == null) {
                log.debug("No metastore configured, skipping centroid persistence");
                return;
            }
            VectorIndex vi = entry.store.getVectorIndex();
            if (!(vi instanceof JVectorVectorIndex jvVi)) return;
            ShardCentroids sc = jvVi.getLatestCentroids(tableId);
            if (sc == null) return;
            byte[] key = ShardCentroids.metastoreKey(sc.shardId());
            metastore.put(key, sc.toBytes());
            log.info(
                    "Persisted shard centroids to metastore: shardId={}, nlist={}",
                    sc.shardId(),
                    sc.nlist());
        } catch (Exception e) {
            log.warn(
                    "Failed to persist shard centroids for tableId={}: {}",
                    tableId,
                    e.getMessage());
        }
    }

    private void tryTriggerCentroidBuild(DynamicMmapStore store, String tableId) {
        try {
            VectorIndex vi = store.getVectorIndex();
            if (!(vi instanceof JVectorVectorIndex jvVi)) return;
            if (jvVi.getLatestCentroids(tableId) != null) return;
            int count = jvVi.totalVectorCount(tableId);
            if (count < CENTROID_BUILD_THRESHOLD) return;

            ShardCentroids sc = jvVi.buildAndPersistCentroids(tableId, "shard-0");
            if (sc != null) {
                log.info(
                        "Auto-built centroids for tableId={}, vectorCount={}, shardId={}",
                        tableId,
                        count,
                        sc.shardId());
            }
        } catch (Exception e) {
            log.warn(
                    "Failed to trigger centroid build for tableId={}: {}", tableId, e.getMessage());
        }
    }

    private Find.Result doFind(TableStore ts, int key) throws IOException {
        KeyCodec keyCodec = ts.getKeyCodec();
        byte[] encodedKey = keyCodec.encodePrimaryKey(Map.of(COL_ID, (Comparable) (Integer) key));
        return doFindByKey(ts, encodedKey, key);
    }

    /**
     * 用预编码的 encodedKey 查找。legacyKey 仅用于 reader chain fallback 的单 PK 兼容路径 （复合 PK 时传 0，且 reader chain
     * 路径不会被命中因为 encodedKey 已能直接 hit instance）。
     */
    private Find.Result doFindByKey(TableStore ts, byte[] encodedKey, int legacyKey)
            throws IOException {
        byte[] raw = ts.getRawValue(encodedKey);
        if (raw != null && !NullableValueCodec.isTombstone(raw)) {
            // Records 由 LmdbWriter 写入时通过 NullableValueCodec.encode 包装；先解包再判断格式。
            byte[] unwrapped;
            try {
                unwrapped = NullableValueCodec.decode(raw);
            } catch (Exception e) {
                unwrapped = raw;
            }
            if (unwrapped == null) {
                return Find.Result.notFound();
            }
            // Legacy 兼容：方案 A 之前的 FastPath INSERT 写入 JSON 字节（{"col":val,...}），解包后第一字节是 '{'。
            // 方案 A 后所有 FastPath INSERT 都走 RecordCodec 路径，新数据这条嗅探永不命中。
            // 保留是为了兼容已有 LMDB 中可能的 legacy JSON 数据；下个 sprint 可彻底删除。
            if (unwrapped.length > 0 && unwrapped[0] == '{') {
                return Find.Result.found(null, unwrapped);
            }
            return decodeResult(ts, raw, legacyKey);
        }

        io.github.rawwise.engine.storage.table.read.TableReader reader = ts.reader();
        if (reader != null) {
            try {
                Find find =
                        Find.builder()
                                .withTable(new SimpleTableRef(ts.getTableIdentifier()))
                                .withPrimaryKey(Map.of(COL_ID, (Comparable) (Integer) legacyKey))
                                .withReader(reader)
                                .build();
                Find.Result chainResult = reader.find(find);
                if (chainResult.success() && chainResult.foundRecord().isPresent()) {
                    return chainResult;
                }
            } catch (UnsupportedOperationException e) {
                // reader chain not available, fall through
            }
        }

        return Find.Result.notFound();
    }

    private Find.Result decodeResult(TableStore ts, byte[] raw, int key) {
        io.github.rawwise.model.record.RecordCodec recordCodec = ts.getRecordCodec();
        io.github.rawwise.model.record.RecordCodec.SchemaResolver schemaResolver =
                ts.getSchemaResolver();
        RecordDecoder.DecodeResult decodeResult =
                RecordDecoder.decodeNullableWrapped(raw, recordCodec, schemaResolver);
        if (decodeResult instanceof RecordDecoder.DecodeResult.Found f) {
            Record record = f.record();
            log.debug("doFind: key={} found record body={}", key, record.body().toMap());
            return Find.Result.found(record, raw);
        }
        log.debug("doFind: key={} decode failed", key);
        return Find.Result.notFound();
    }

    private static int bytesToInt(byte[] bytes) {
        if (bytes.length >= 4) {
            return ByteBuffer.wrap(bytes).getInt();
        }
        return Integer.parseInt(new String(bytes, StandardCharsets.UTF_8));
    }

    private static float[] bytesToFloatArray(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        float[] arr = new float[bytes.length / 4];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = buf.getFloat();
        }
        return arr;
    }

    private static final int SHARD_HASH_SEED = 42;

    @Override
    public io.grpc.stub.StreamObserver<WalEntry> replicateWal(
            StreamObserver<ReplicateAck> responseObserver) {
        return new StreamObserver<>() {
            private long lastSeq = -1;

            @Override
            public void onNext(WalEntry entry) {
                lastSeq = entry.getSeq();
                try {
                    if (entry.getTableId() != null && !entry.getTableId().isEmpty()) {
                        String walNormalizedId = normalizeTableId(entry.getTableId());
                        StoreEntry walStoreEntry = getOrCreateStore(walNormalizedId);
                        byte[] payload = entry.getPayload().toByteArray();
                        java.nio.ByteBuffer walBuf = java.nio.ByteBuffer.wrap(payload);
                        int keyLen = walBuf.getInt();
                        byte[] walKey = new byte[keyLen];
                        walBuf.get(walKey);
                        byte[] walValue = new byte[walBuf.remaining()];
                        walBuf.get(walValue);
                        KvWriteBuilder walBuilder =
                                KvWriteBuilder.builder()
                                        .table(new SimpleTableRef(walNormalizedId))
                                        .key(walKey)
                                        .value(walValue)
                                        .partitionHint(0);
                        walStoreEntry.store.write(walBuilder, walStoreEntry.ownerContext);
                    }
                    responseObserver.onNext(
                            ReplicateAck.newBuilder()
                                    .setAckedSeq(entry.getSeq())
                                    .setSuccess(true)
                                    .build());
                } catch (Exception e) {
                    log.warn("replicateWal failed for seq={}: {}", entry.getSeq(), e.getMessage());
                    responseObserver.onNext(
                            ReplicateAck.newBuilder()
                                    .setAckedSeq(entry.getSeq())
                                    .setSuccess(false)
                                    .setError(e.getMessage())
                                    .build());
                }
            }

            @Override
            public void onError(Throwable t) {
                log.warn("replicateWal stream error: {}", t.getMessage());
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void streamSnapshot(
            SnapshotRequest request, StreamObserver<SnapshotChunk> responseObserver) {
        String tableId = request.getTableId();
        int shardId = request.getShardId();
        log.info("streamSnapshot: table={}, shard={}", tableId, shardId);
        try {
            String normalizedSnapshotId = normalizeTableId(tableId);
            StoreEntry snapshotEntry = getOrCreateStore(normalizedSnapshotId);
            TableStore snapshotTs = snapshotEntry.ensureTableStore(normalizedSnapshotId);
            int batchSize = 100;
            java.util.List<com.google.protobuf.ByteString> keys = new java.util.ArrayList<>();
            java.util.List<com.google.protobuf.ByteString> values = new java.util.ArrayList<>();
            int count = 0;
            for (LmdbInstance instance : snapshotTs.streamInstances().toList()) {
                try (CloseableIterator<Pair<byte[], byte[]>> iter = instance.closeableIterator()) {
                    while (iter.hasNext()) {
                        Pair<byte[], byte[]> kv = iter.next();
                        byte[] key = kv.getKey();
                        byte[] value = kv.getValue();
                        if (NullableValueCodec.isTombstone(value)) continue;
                        keys.add(com.google.protobuf.ByteString.copyFrom(key));
                        values.add(com.google.protobuf.ByteString.copyFrom(value));
                        count++;
                        if (keys.size() >= batchSize) {
                            responseObserver.onNext(
                                    SnapshotChunk.newBuilder()
                                            .addAllKeys(keys)
                                            .addAllValues(values)
                                            .setHasMore(true)
                                            .build());
                            keys.clear();
                            values.clear();
                        }
                    }
                } catch (Exception e) {
                    log.warn(
                            "streamSnapshot iterator error for table={}: {}",
                            normalizedSnapshotId,
                            e.getMessage());
                }
            }
            responseObserver.onNext(
                    SnapshotChunk.newBuilder()
                            .addAllKeys(keys)
                            .addAllValues(values)
                            .setHasMore(false)
                            .build());
            log.info(
                    "streamSnapshot completed: table={}, shard={}, records={}",
                    tableId,
                    shardId,
                    count);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("streamSnapshot failed: table={}, shard={}", tableId, shardId, e);
            responseObserver.onError(
                    io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    public void scanKeys(ScanKeysRequest request, StreamObserver<KeyBatch> responseObserver) {
        try {
            String normalizedId = normalizeTableId(request.getTableId());
            StoreEntry entry = getOrCreateStore(normalizedId);
            TableStore ts = entry.ensureTableStore(normalizedId);

            int shardId = request.getShardId();
            int totalShardCount = request.getTotalShardCount();
            int batchSize = Math.max(1, request.getBatchSize());

            List<ByteString> batchKeys = new ArrayList<>();
            List<ByteString> batchValues = new ArrayList<>();
            boolean sentAny = false;

            for (LmdbInstance instance : ts.streamInstances().toList()) {
                try (CloseableIterator<Pair<byte[], byte[]>> iter = instance.closeableIterator()) {
                    while (iter.hasNext()) {
                        Pair<byte[], byte[]> kv = iter.next();
                        byte[] key = kv.getKey();
                        byte[] value = kv.getValue();

                        if (NullableValueCodec.isTombstone(value)) continue;

                        if (totalShardCount > 1) {
                            int hash = MurmurHash3.hash32(key, 0, key.length, SHARD_HASH_SEED);
                            int shard = Math.floorMod(hash, totalShardCount);
                            if (shard != shardId) continue;
                        }

                        batchKeys.add(ByteString.copyFrom(key));
                        batchValues.add(ByteString.copyFrom(value));

                        if (batchKeys.size() >= batchSize) {
                            KeyBatch batch =
                                    KeyBatch.newBuilder()
                                            .addAllKeys(batchKeys)
                                            .addAllValues(batchValues)
                                            .setHasMore(true)
                                            .build();
                            responseObserver.onNext(batch);
                            batchKeys.clear();
                            batchValues.clear();
                            sentAny = true;
                        }
                    }
                } catch (Exception e) {
                    log.warn(
                            "scanKeys iterator error for table={}: {}",
                            normalizedId,
                            e.getMessage());
                }
            }

            if (!batchKeys.isEmpty() || !sentAny) {
                KeyBatch batch =
                        KeyBatch.newBuilder()
                                .addAllKeys(batchKeys)
                                .addAllValues(batchValues)
                                .setHasMore(false)
                                .build();
                responseObserver.onNext(batch);
            }

            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("ScanKeys failed for table={}", request.getTableId(), e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void migrateBatch(
            MigrateBatchRequest request, StreamObserver<MigrateBatchResponse> responseObserver) {
        try {
            String normalizedId = normalizeTableId(request.getTableId());
            StoreEntry entry = getOrCreateStore(normalizedId);
            DynamicMmapStore store = entry.store;

            int migratedCount = 0;
            int failedCount = 0;

            if (request.getDeleteAfterMigrate()) {
                for (ByteString keyBs : request.getKeysList()) {
                    try {
                        byte[] key = keyBs.toByteArray();
                        store.delete(normalizedId, key, entry.ownerContext);
                        migratedCount++;
                    } catch (Exception e) {
                        failedCount++;
                        log.warn("migrateBatch delete failed: {}", e.getMessage());
                    }
                }
            } else {
                List<ByteString> keys = request.getKeysList();
                List<ByteString> values = request.getValuesList();
                for (int i = 0; i < keys.size(); i++) {
                    try {
                        byte[] key = keys.get(i).toByteArray();
                        byte[] rawValue =
                                i < values.size() ? values.get(i).toByteArray() : new byte[0];
                        if (NullableValueCodec.isTombstone(rawValue)) {
                            continue;
                        }
                        byte[] unwrappedValue;
                        try {
                            unwrappedValue = NullableValueCodec.decode(rawValue);
                        } catch (Exception e) {
                            unwrappedValue = rawValue;
                        }
                        if (unwrappedValue == null) {
                            continue;
                        }
                        KvWriteBuilder builder =
                                KvWriteBuilder.builder()
                                        .table(new SimpleTableRef(normalizedId))
                                        .key(key)
                                        .value(unwrappedValue)
                                        .partitionHint(0);
                        store.write(builder, entry.ownerContext);
                        migratedCount++;
                    } catch (Exception e) {
                        failedCount++;
                        log.warn("migrateBatch write failed: {}", e.getMessage());
                    }
                }
            }

            MigrateBatchResponse response =
                    MigrateBatchResponse.newBuilder()
                            .setMigratedCount(migratedCount)
                            .setFailedCount(failedCount)
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("MigrateBatch failed for table={}", request.getTableId(), e);
            responseObserver.onNext(
                    MigrateBatchResponse.newBuilder()
                            .setFailedCount(request.getKeysCount())
                            .setError(
                                    e.getMessage() != null
                                            ? e.getMessage()
                                            : e.getClass().getSimpleName())
                            .build());
            responseObserver.onCompleted();
        }
    }
}
