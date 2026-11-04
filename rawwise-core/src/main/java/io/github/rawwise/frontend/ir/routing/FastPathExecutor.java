package io.github.rawwise.frontend.ir.routing;

import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.ShardAssignment;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.routing.ShardLookupResult;
import io.github.rawwise.common.hash.MurmurHash3;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import io.github.rawwise.engine.storage.table.key.OrderPreservingKeyEncoder;
import io.github.rawwise.frontend.ir.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FastPathExecutor {

    final Map<String, ExecutorStub> executorStubs = new ConcurrentHashMap<>();
    private final RoutingCoordinator routingCoordinator;
    private final Map<Integer, String> shardToExecutor = new ConcurrentHashMap<>();
    private final Map<String, RoutingVersion> lastKnownVersion = new ConcurrentHashMap<>();
    // epoch cache: key = tableId + ":" + shardId, value = epoch.
    // Normal path hits cache, saving getShardEpoch RPC(point query 1 RPC → 0 ).
    // On StaleEpochException, invalidate and retry via getShardEpoch refresh.
    // Correctness guarantee: pointQuery still carries epoch, client still verifies response.epoch
    // vs request.epoch.
    private final Map<String, Long> epochCache = new ConcurrentHashMap<>();
    private static final int MAX_RETRIES = 3;
    private static final int SPARK_HASH_SEED = 42;

    /**
     * Approach A: pass schema to stub via ThreadLocal (avoids adding schema parameter to all 4
     * ExecutorStub interface methods). GrpcExecutorStub reads this schema when serializing
     * PointInsert/PointUpdate/PointDelete/PointQuery requests and writes it to PbStructType field,
     * enabling server side RecordCodec.encode / RecordDecoder.decode.
     */
    public static final ThreadLocal<SchemaContext> CURRENT_SCHEMA = new ThreadLocal<>();

    public record SchemaContext(
            io.github.rawwise.model.record.StructType schema, long numericTableId) {}

    public interface ExecutorStub {
        Optional<byte[]> pointQuery(String tableId, byte[] key, long snapshotTs, long shardEpoch);

        /**
         * Composite PK friendly version: pass pkValues map + primaryKeyCols, server side
         * reconstructs encodedKey using keyCodec. Default implementation falls back to legacy
         * single PK byte[] path.
         */
        default Optional<byte[]> pointQuery(
                String tableId,
                byte[] key,
                java.util.Map<String, Comparable<?>> pkValues,
                java.util.List<String> primaryKeyCols,
                long snapshotTs,
                long shardEpoch) {
            return pointQuery(tableId, key, snapshotTs, shardEpoch);
        }

        List<PointQueryResult> batchPointQuery(
                String tableId, List<byte[]> keys, long snapshotTs, long shardEpoch);

        long getShardEpoch(String tableId, int shardId);

        /**
         * Point update via transactional auto-commit.
         *
         * @param tableId Table identifier
         * @param pkValues PK column name → value
         * @param setClauses SET column name → new value
         * @param primaryKeyCols PK column name list
         * @param snapshotTs Snapshot timestamp
         * @param shardEpoch Shard epoch
         * @return Update result (success=true means success, rowsAffected is usually 1)
         */
        PointWriteResult pointUpdate(
                String tableId,
                java.util.Map<String, Comparable<?>> pkValues,
                java.util.Map<String, Comparable<?>> setClauses,
                java.util.List<String> primaryKeyCols,
                long snapshotTs,
                long shardEpoch);

        /** Point delete via transactional auto-commit. */
        PointWriteResult pointDelete(
                String tableId,
                java.util.Map<String, Comparable<?>> pkValues,
                java.util.List<String> primaryKeyCols,
                long snapshotTs,
                long shardEpoch);

        /** Point insert via transactional auto-commit. */
        PointWriteResult pointInsert(
                String tableId,
                byte[] encodedKey,
                byte[] rowData,
                java.util.Map<String, Comparable<?>> pkValues,
                java.util.List<String> primaryKeyCols,
                long snapshotTs,
                long shardEpoch);

        /**
         * Point insert with full row values (Records / RecordCodec path, Approach A). When
         * rowValues is non-empty, server side uses RecordCodec.encode to write complete Record
         * (including RecordHeader + commitTs + sequenceNumber + Kind=INSERT), consistent with Spark
         * write path. schema is passed via {@link FastPathExecutor#CURRENT_SCHEMA} ThreadLocal
         * (avoids interface explosion). Default implementation falls back to legacy JSON path
         * (maintains compatibility).
         */
        default PointWriteResult pointInsert(
                String tableId,
                byte[] encodedKey,
                byte[] rowData,
                java.util.Map<String, Comparable<?>> pkValues,
                java.util.Map<String, Comparable<?>> rowValues,
                java.util.List<String> primaryKeyCols,
                long snapshotTs,
                long shardEpoch) {
            return pointInsert(
                    tableId, encodedKey, rowData, pkValues, primaryKeyCols, snapshotTs, shardEpoch);
        }
    }

    /** Point write (UPDATE/DELETE) result. */
    public record PointWriteResult(boolean success, int rowsAffected, String error) {
        public static PointWriteResult ok(int rows) {
            return new PointWriteResult(true, rows, null);
        }

        public static PointWriteResult conflict(String reason) {
            return new PointWriteResult(false, 0, reason);
        }

        public static PointWriteResult notFound() {
            return new PointWriteResult(true, 0, null);
        }
    }

    public FastPathExecutor(RoutingCoordinator routingCoordinator) {
        this.routingCoordinator = routingCoordinator;
    }

    public FastPathExecutor() {
        this.routingCoordinator = null;
    }

    public FastPathResult execute(QueryIR ir) {
        // Approach A: extract table schema and numericTableId from IR, write to ThreadLocal for
        // stub to read when serializing requests.
        TableMetaIR tableMeta = extractTableMeta(ir);
        SchemaContext prev = CURRENT_SCHEMA.get();
        if (tableMeta != null && tableMeta.schema() != null) {
            CURRENT_SCHEMA.set(new SchemaContext(tableMeta.schema(), tableMeta.tableId()));
        }
        try {
            return switch (ir) {
                case PointQueryIR pq -> executePointQuery(pq);
                case BatchPointQueryIR bpq -> executeBatchPointQuery(bpq);
                case UpdateQueryIR uq -> executePointUpdate(uq);
                case DeleteQueryIR dq -> executePointDelete(dq);
                case WriteQueryIR wq -> executePointInsert(wq);
                default -> {
                    log.warn("FastPathExecutor received non-fast-path IR: {}", ir.type());
                    yield FastPathResult.fallbackRequired("unsupported IR type: " + ir.type());
                }
            };
        } finally {
            if (prev == null) CURRENT_SCHEMA.remove();
            else CURRENT_SCHEMA.set(prev);
        }
    }

    private static TableMetaIR extractTableMeta(QueryIR ir) {
        return switch (ir) {
            case PointQueryIR pq -> pq.tableMeta();
            case BatchPointQueryIR bpq -> bpq.tableMeta();
            case UpdateQueryIR uq -> uq.tableMeta();
            case DeleteQueryIR dq -> dq.tableMeta();
            case WriteQueryIR wq -> wq.tableMeta();
            default -> null;
        };
    }

    private FastPathResult executeOriginal(QueryIR ir) {
        // legacy entry (retained for possible internal call compatibility; execute is the real
        // entry point)
        return switch (ir) {
            case PointQueryIR pq -> executePointQuery(pq);
            case BatchPointQueryIR bpq -> executeBatchPointQuery(bpq);
            case UpdateQueryIR uq -> executePointUpdate(uq);
            case DeleteQueryIR dq -> executePointDelete(dq);
            case WriteQueryIR wq -> executePointInsert(wq);
            default -> {
                log.warn("FastPathExecutor received non-fast-path IR: {}", ir.type());
                yield FastPathResult.fallbackRequired("unsupported IR type: " + ir.type());
            }
        };
    }

    private FastPathResult executePointQuery(PointQueryIR ir) {
        if (ir.snapshotTs() > 0) {
            invalidateIfStale(
                    ir.tableMeta().tableIdentifier(), new RoutingVersion(ir.snapshotTs(), 0, 0));
        }

        // Distributed mode: select shard by PK hash to distribute read load across multiple BE
        // Nodes
        int shardId = resolveShardByPk(ir.pkValues());
        String executorId = resolveCurrentOwner(shardId);
        if (executorId == null) {
            return FastPathResult.fallbackRequired(
                    "no executor for shard " + shardId + " (point query)");
        }

        // Compute order-preserving encoded key (for range-based routing check)
        // Only encode when pkValues present and routingCoordinator exists; on failure degrade to
        // no-range-check path
        byte[] encodedKeyOp = null;
        if (routingCoordinator != null && ir.pkValues() != null && !ir.pkValues().isEmpty()) {
            try {
                encodedKeyOp = OrderPreservingKeyEncoder.encode(ir.pkValues());
            } catch (Exception e) {
                log.debug(
                        "OrderPreservingKeyEncoder failed for table {}, falling back to no range check: {}",
                        ir.tableMeta().tableIdentifier(),
                        e.getMessage());
            }
        }

        if (!validateOwner(shardId, executorId, encodedKeyOp)) {
            // Distinguish fallback reason —— owner mismatch / unassigned / recovering → fallback;
            // range not cover → NotFound
            ShardLookupResult lookup =
                    (routingCoordinator == null)
                            ? null
                            : routingCoordinator.lookupOwner(new ShardId(shardId));
            if (lookup == null || !lookup.assigned()) {
                String currentOwner = resolveCurrentOwner(shardId);
                if (currentOwner == null) {
                    return FastPathResult.fallbackRequired(
                            "shard " + shardId + " is unassigned, cannot validate owner");
                }
                if (!currentOwner.equals(executorId)) {
                    log.warn(
                            "Owner mismatch for shardId={}: IR has executorId={}, current owner={}. Triggering fallback.",
                            shardId,
                            executorId,
                            currentOwner);
                    refreshShardMapping(shardId, currentOwner);
                    return FastPathResult.fallbackRequired(
                            "owner mismatch: shardId="
                                    + shardId
                                    + " expected="
                                    + executorId
                                    + " actual="
                                    + currentOwner);
                }
                return FastPathResult.fallbackRequired("validateOwner failed for shard " + shardId);
            }
            ShardAssignment assignment = lookup.assignment();
            if (assignment.recovering()) {
                log.info("executePointQuery: shard {} is recovering, triggering fallback", shardId);
                return FastPathResult.fallbackRequired("shard " + shardId + " is recovering");
            }
            if (!assignment.executorId().equals(executorId)) {
                log.warn(
                        "Owner mismatch for shardId={}: IR has executorId={}, current owner={}. Triggering fallback.",
                        shardId,
                        executorId,
                        assignment.executorId());
                refreshShardMapping(shardId, assignment.executorId());
                return FastPathResult.fallbackRequired(
                        "owner mismatch: shardId="
                                + shardId
                                + " expected="
                                + executorId
                                + " actual="
                                + assignment.executorId());
            }
            // owner matches but validateOwner returns false → key not in shard's dual-layer range →
            // NotFound
            // This is the user's explicit semantics: lmdb range ∪ parquet range both do not cover
            // key → NOT_FOUND(no fallback to Spark SQL)
            log.debug(
                    "Range check failed for shard {}: key not in lmdb({}) / parquet({}) ranges, returning NOT_FOUND",
                    shardId,
                    assignment.lmdbRanges().size(),
                    assignment.parquetRanges().size());
            return FastPathResult.notFound();
        }

        ExecutorStub stub = executorStubs.get(executorId);
        if (stub == null) {
            log.warn("Executor {} unreachable for point query, trigger fallback", executorId);
            return FastPathResult.unreachable(executorId);
        }

        long currentEpoch = resolveShardEpoch(ir.tableMeta().tableIdentifier(), shardId);

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                Optional<byte[]> result =
                        stub.pointQuery(
                                ir.tableMeta().tableIdentifier(),
                                ir.encodedKey(),
                                ir.pkValues(),
                                ir.tableMeta().primaryKeyColumns(),
                                ir.snapshotTs(),
                                currentEpoch);
                if (result.isPresent()) {
                    return FastPathResult.hit(result.get());
                }
                return FastPathResult.miss();
            } catch (StaleEpochException e) {
                log.info(
                        "STALE_EPOCH on attempt {} for executor {}, refreshing cache",
                        attempt + 1,
                        executorId);
                long freshEpoch =
                        refreshEpochAfterStale(
                                ir.tableMeta().tableIdentifier(), shardId, currentEpoch);
                if (freshEpoch < 0) {
                    return FastPathResult.staleEpoch(shardId, currentEpoch, currentEpoch);
                }
                currentEpoch = freshEpoch;
            } catch (Exception e) {
                log.warn(
                        "Executor {} unreachable on attempt {}: {}",
                        executorId,
                        attempt + 1,
                        e.getMessage());
                if (attempt == MAX_RETRIES - 1) {
                    return FastPathResult.unreachable(executorId);
                }
            }
        }
        return FastPathResult.fallbackRequired("max retries exceeded");
    }

    private FastPathResult executeBatchPointQuery(BatchPointQueryIR ir) {
        if (ir.snapshotTs() > 0) {
            invalidateIfStale(
                    ir.tableMeta().tableIdentifier(), new RoutingVersion(ir.snapshotTs(), 0, 0));
        }

        if (!validateOwner(ir.shardId(), ir.targetExecutorId())) {
            String currentOwner = resolveCurrentOwner(ir.shardId());
            if (currentOwner == null) {
                return FastPathResult.fallbackRequired(
                        "shard " + ir.shardId() + " is unassigned, cannot validate owner");
            }
            if (!currentOwner.equals(ir.targetExecutorId())) {
                log.warn(
                        "Owner mismatch for shardId={}: IR has executorId={}, current owner={}. Triggering fallback.",
                        ir.shardId(),
                        ir.targetExecutorId(),
                        currentOwner);
                refreshShardMapping(ir.shardId(), currentOwner);
                return FastPathResult.fallbackRequired(
                        "owner mismatch: shardId="
                                + ir.shardId()
                                + " expected="
                                + ir.targetExecutorId()
                                + " actual="
                                + currentOwner);
            }
        }

        ExecutorStub stub = executorStubs.get(ir.targetExecutorId());
        if (stub == null) {
            return FastPathResult.unreachable(ir.targetExecutorId());
        }
        long currentEpoch = resolveShardEpoch(ir.tableMeta().tableIdentifier(), ir.shardId());
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                List<PointQueryResult> results =
                        stub.batchPointQuery(
                                ir.tableMeta().tableIdentifier(),
                                ir.encodedKeys(),
                                ir.snapshotTs(),
                                currentEpoch);
                return FastPathResult.batchHit(results);
            } catch (StaleEpochException e) {
                log.info(
                        "STALE_EPOCH on batch query attempt {} for executor {}",
                        attempt + 1,
                        ir.targetExecutorId());
                long freshEpoch =
                        refreshEpochAfterStale(
                                ir.tableMeta().tableIdentifier(), ir.shardId(), currentEpoch);
                if (freshEpoch < 0) {
                    return FastPathResult.staleEpoch(ir.shardId(), currentEpoch, currentEpoch);
                }
                currentEpoch = freshEpoch;
            } catch (Exception e) {
                log.warn(
                        "Batch point query to {} failed on attempt {}: {}",
                        ir.targetExecutorId(),
                        attempt + 1,
                        e.getMessage());
                if (attempt == MAX_RETRIES - 1) {
                    return FastPathResult.unreachable(ir.targetExecutorId());
                }
            }
        }
        return FastPathResult.fallbackRequired("max retries exceeded");
    }

    private FastPathResult executePointUpdate(UpdateQueryIR ir) {
        // Distributed mode: select shard by PK hash to distribute write load across multiple BE
        // Nodes
        int shardId = resolveShardByPk(ir.pkValues());
        String executorId = resolveCurrentOwner(shardId);
        if (executorId == null) {
            return FastPathResult.fallbackRequired(
                    "no executor for shard " + shardId + " (point update)");
        }
        ExecutorStub stub = executorStubs.get(executorId);
        if (stub == null) {
            return FastPathResult.unreachable(executorId);
        }
        long currentEpoch = ir.shardEpoch();
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                PointWriteResult result =
                        stub.pointUpdate(
                                ir.tableMeta().tableIdentifier(),
                                ir.pkValues(),
                                ir.setClauses(),
                                ir.tableMeta().primaryKeyColumns(),
                                ir.snapshotId(),
                                currentEpoch);
                if (result.success()) {
                    return FastPathResult.updated(result.rowsAffected());
                }
                if (result.error() != null && result.error().contains("not found")) {
                    return FastPathResult.notFound();
                }
                return FastPathResult.writeConflict(result.error());
            } catch (StaleEpochException e) {
                long freshEpoch =
                        refreshEpochAfterStale(
                                ir.tableMeta().tableIdentifier(), shardId, currentEpoch);
                if (freshEpoch < 0) {
                    return FastPathResult.staleEpoch(shardId, currentEpoch, currentEpoch);
                }
                currentEpoch = freshEpoch;
            } catch (Exception e) {
                log.warn(
                        "Point update to {} failed on attempt {}: {}",
                        executorId,
                        attempt + 1,
                        e.getMessage());
                if (attempt == MAX_RETRIES - 1) {
                    return FastPathResult.unreachable(executorId);
                }
            }
        }
        return FastPathResult.fallbackRequired("max retries exceeded");
    }

    private FastPathResult executePointDelete(DeleteQueryIR ir) {
        // Distributed mode: select shard by PK hash to distribute write load across multiple BE
        // Nodes
        int shardId = resolveShardByPk(ir.pkValues());
        String executorId = resolveCurrentOwner(shardId);
        if (executorId == null) {
            return FastPathResult.fallbackRequired(
                    "no executor for shard " + shardId + " (point delete)");
        }
        ExecutorStub stub = executorStubs.get(executorId);
        if (stub == null) {
            return FastPathResult.unreachable(executorId);
        }
        long currentEpoch = ir.shardEpoch();
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                PointWriteResult result =
                        stub.pointDelete(
                                ir.tableMeta().tableIdentifier(),
                                ir.pkValues(),
                                ir.tableMeta().primaryKeyColumns(),
                                ir.snapshotId(),
                                currentEpoch);
                if (result.success()) {
                    return FastPathResult.deleted(result.rowsAffected());
                }
                if (result.error() != null && result.error().contains("not found")) {
                    return FastPathResult.notFound();
                }
                return FastPathResult.writeConflict(result.error());
            } catch (StaleEpochException e) {
                long freshEpoch =
                        refreshEpochAfterStale(
                                ir.tableMeta().tableIdentifier(), shardId, currentEpoch);
                if (freshEpoch < 0) {
                    return FastPathResult.staleEpoch(shardId, currentEpoch, currentEpoch);
                }
                currentEpoch = freshEpoch;
            } catch (Exception e) {
                log.warn(
                        "Point delete to {} failed on attempt {}: {}",
                        executorId,
                        attempt + 1,
                        e.getMessage());
                if (attempt == MAX_RETRIES - 1) {
                    return FastPathResult.unreachable(executorId);
                }
            }
        }
        return FastPathResult.fallbackRequired("max retries exceeded");
    }

    private FastPathResult executePointInsert(WriteQueryIR ir) {
        if (ir.writeType() != WriteType.INSERT && ir.writeType() != WriteType.BATCH_INSERT) {
            return FastPathResult.fallbackRequired("not a point insert");
        }
        if (ir.rows() == null || ir.rows().isEmpty()) {
            return FastPathResult.fallbackRequired("point insert requires at least 1 row");
        }

        List<String> pkCols = ir.tableMeta().primaryKeyColumns();
        if (pkCols == null || pkCols.isEmpty()) {
            return FastPathResult.fallbackRequired("no PK columns");
        }

        // Multi-row INSERT: each row independently routed to corresponding shard by PK hash,
        // distributing write load across multiple BE Nodes.
        // Any row failure immediately returns conflict(no partial rollback——OCC path not enabled,
        // multi-row batch is not transactional atomic itself).
        int totalWritten = 0;
        for (int rowIdx = 0; rowIdx < ir.rows().size(); rowIdx++) {
            Map<String, Comparable<?>> rowValues = ir.rows().get(rowIdx);

            // encodedKey left empty: server side will reconstruct via keyCodec.encodePrimaryKey
            // using pkOnlyValues + pkCols.
            // Do not compute 4-byte int in frontend (avoids silent truncation collision for BIGINT
            // PK > Integer.MAX_VALUE).
            byte[] encodedKey = new byte[0];

            // Validate PK columns are not null
            for (String pkCol : pkCols) {
                if (rowValues.get(pkCol) == null) {
                    return FastPathResult.fallbackRequired(
                            "PK column \""
                                    + pkCol
                                    + "\" not found in row "
                                    + rowIdx
                                    + " (available: "
                                    + rowValues.keySet()
                                    + ")");
                }
            }

            // pkValues only contains PK columns.
            Map<String, Comparable<?>> pkOnlyValues = new java.util.LinkedHashMap<>();
            for (String pk : pkCols) {
                Comparable<?> v = rowValues.get(pk);
                if (v != null) pkOnlyValues.put(pk, v);
            }

            // Distributed mode: select shard by PK hash to distribute write load across multiple BE
            // Nodes
            int shardId = resolveShardByPk(pkOnlyValues);
            String executorId = resolveCurrentOwner(shardId);
            if (executorId == null) {
                return FastPathResult.fallbackRequired(
                        "no executor for shard " + shardId + " (point insert row " + rowIdx + ")");
            }
            ExecutorStub stub = executorStubs.get(executorId);
            if (stub == null) {
                return FastPathResult.unreachable(executorId);
            }

            // Approach A: pass schema via ThreadLocal, server side uses RecordCodec.encode to write
            // complete Record.
            boolean rowOk = false;
            String lastErr = null;
            for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
                try {
                    PointWriteResult result =
                            stub.pointInsert(
                                    ir.tableMeta().tableIdentifier(),
                                    encodedKey, /* rowData (legacy) */
                                    null,
                                    pkOnlyValues,
                                    rowValues,
                                    pkCols,
                                    0,
                                    0);
                    if (result.success()) {
                        rowOk = true;
                        totalWritten++;
                        break;
                    }
                    lastErr = result.error();
                    break;
                } catch (Exception e) {
                    lastErr = e.getMessage();
                    log.warn(
                            "Point insert row {} to {} (shard {}) failed on attempt {}: {}",
                            rowIdx,
                            executorId,
                            shardId,
                            attempt + 1,
                            e.getMessage());
                    if (attempt == MAX_RETRIES - 1) {
                        return FastPathResult.unreachable(executorId);
                    }
                }
            }
            if (!rowOk) {
                return FastPathResult.writeConflict(
                        "row "
                                + rowIdx
                                + " failed: "
                                + lastErr
                                + " (written="
                                + totalWritten
                                + "/"
                                + ir.rows().size()
                                + ")");
            }
        }

        return FastPathResult.updated(totalWritten);
    }

    private static final com.fasterxml.jackson.databind.ObjectMapper JSON_MAPPER =
            new com.fasterxml.jackson.databind.ObjectMapper();

    /**
     * @deprecated After Approach A, INSERT no longer uses JSON byte path; server side uses
     *     RecordCodec.encode via rowValues to write complete Record; this method is retained only
     *     for possible legacy stub fallback.
     */
    @Deprecated
    private static byte[] recordToJsonBytes(Map<String, Comparable<?>> record) {
        try {
            return JSON_MAPPER.writeValueAsBytes(record);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new RuntimeException("recordToJsonBytes failed: " + e.getMessage(), e);
        }
    }

    /**
     * Enhanced validateOwner, third parameter encodedKeyOp for range-based routing check.
     *
     * <p>Logic:
     *
     * <ol>
     *   <li>owner mismatch / unassigned / recovering → return false(caller falls back)
     *   <li>encodedKeyOp != null and shard has range info (lmdbRanges/parquetRanges non-empty) and
     *       key not in dual-layer range → return false(caller should return NotFound)
     *   <li>Other cases(owner matches, or no range info)→ return true
     * </ol>
     *
     * <p>Note: when encodedKeyOp is null (batch query or encoding failure), skip range check,
     * behavior is identical to original hash routing (maintains compatibility).
     */
    boolean validateOwner(int shardId, String expectedExecutorId, byte[] encodedKeyOp) {
        if (routingCoordinator == null) {
            String cached = shardToExecutor.get(shardId);
            if (cached != null) {
                return cached.equals(expectedExecutorId);
            }
            log.warn(
                    "validateOwner: no routingCoordinator and no cache entry for shardId={}, triggering fallback",
                    shardId);
            return false;
        }
        ShardLookupResult lookup = routingCoordinator.lookupOwner(new ShardId(shardId));
        if (lookup == null || !lookup.assigned()) {
            return false;
        }
        ShardAssignment assignment = lookup.assignment();
        // Shard in recovering state should not receive queries (data may be incomplete), trigger
        // fallback
        if (assignment.recovering()) {
            log.info("validateOwner: shard {} is recovering, triggering fallback", shardId);
            return false;
        }
        if (!assignment.executorId().equals(expectedExecutorId)) {
            return false;
        }
        // range check —— only enabled when shard has range info and encodedKeyOp is non-null
        if (encodedKeyOp != null
                && assignment.hasRangeInfo()
                && !assignment.coversKey(encodedKeyOp)) {
            log.debug(
                    "validateOwner: key not in shard {} ranges (lmdb={}, parquet={}), returning false → NOT_FOUND",
                    shardId,
                    assignment.lmdbRanges().size(),
                    assignment.parquetRanges().size());
            return false;
        }
        return true;
    }

    /** Compatible with old callers (no encodedKeyOp, skip range check). */
    boolean validateOwner(int shardId, String expectedExecutorId) {
        return validateOwner(shardId, expectedExecutorId, null);
    }

    private String resolveCurrentOwner(int shardId) {
        if (routingCoordinator == null) {
            return shardToExecutor.get(shardId);
        }
        ShardLookupResult lookup = routingCoordinator.lookupOwner(new ShardId(shardId));
        if (lookup == null || !lookup.assigned()) {
            return null;
        }
        // Shard in recovering state treated as unavailable owner, trigger fallback
        if (lookup.assignment().recovering()) {
            log.info("resolveCurrentOwner: shard {} is recovering, treating as no owner", shardId);
            return null;
        }
        return lookup.assignment().executorId();
    }

    /**
     * In distributed mode, selects shard by PK hash to distribute read/write load across multiple
     * BE Nodes. Single-node mode (shardToExecutor.size <= 1) returns 0 to preserve original
     * behavior. Same PK produces consistent hash, ensuring read-after-write routes to the same
     * shard.
     *
     * <p>Hash algorithm must be consistent with Spark SQL write path's {@code
     * SparkMurmur3ShardResolver} consistent:
     *
     * <ol>
     *   <li>Use {@code KeyCodec.encodePrimaryKey(pkValues)} to encode to byte[] (consistent with BE
     *       Node KeyCodec)
     *   <li>{@code MurmurHash3.hash32(keyBytes, 0, keyBytes.length, seed=42)}(consistent with Spark
     *       {@code Murmur3_x86_32} seed=42)
     *   <li>{@code Math.floorMod(hash, shardCount)}(consistent with {@code
     *       SparkMurmur3ShardResolver}, correctly handles negative numbers)
     * </ol>
     *
     * Otherwise FastPath SELECT and Spark SQL INSERT would route to different BE Nodes, causing
     * rowCount=0.
     */
    private int resolveShardByPk(Map<String, Comparable<?>> pkValues) {
        int shardCount = shardToExecutor.size();
        if (shardCount <= 1) return 0;
        if (pkValues == null || pkValues.isEmpty()) return 0;
        try {
            List<String> pkCols = new ArrayList<>(pkValues.keySet());
            KeyCodec keyCodec = TableStore.createKeyCodec(pkCols);
            Map<String, Comparable> casted = new LinkedHashMap<>();
            for (var e : pkValues.entrySet()) {
                casted.put(e.getKey(), e.getValue());
            }
            byte[] keyBytes = keyCodec.encodePrimaryKey(casted);
            int hash = MurmurHash3.hash32(keyBytes, 0, keyBytes.length, SPARK_HASH_SEED);
            return Math.floorMod(hash, shardCount);
        } catch (Exception e) {
            log.warn(
                    "resolveShardByPk: keyCodec encode failed, falling back to Java hashCode: {}",
                    e.getMessage());
            int hash = 0;
            for (Comparable<?> v : pkValues.values()) {
                hash = 31 * hash + (v == null ? 0 : v.hashCode());
            }
            return Math.abs(hash) % shardCount;
        }
    }

    private void refreshShardMapping(int shardId, String currentOwner) {
        if (currentOwner != null) {
            shardToExecutor.put(shardId, currentOwner);
        }
    }

    public void invalidateIfStale(String tableId, RoutingVersion currentVersion) {
        if (currentVersion == null) return;
        RoutingVersion last = lastKnownVersion.get(tableId);
        if (last != null && last.isStaleComparedTo(currentVersion)) {
            log.info(
                    "RoutingVersion stale for tableId={}, invalidating caches. last={}, current={}",
                    tableId,
                    last,
                    currentVersion);
            lastKnownVersion.put(tableId, currentVersion);
        } else if (last == null) {
            lastKnownVersion.put(tableId, currentVersion);
        }
    }

    public void registerExecutor(String executorId, ExecutorStub stub) {
        executorStubs.put(executorId, stub);
    }

    public boolean hasAvailableExecutors() {
        return !executorStubs.isEmpty();
    }

    public Set<String> getRegisteredExecutorIds() {
        return executorStubs.keySet();
    }

    public String lookupExecutor(int shardId) {
        return shardToExecutor.get(shardId);
    }

    public void registerShardExecutor(int shardId, String executorId) {
        shardToExecutor.put(shardId, executorId);
    }

    public long resolveShardEpoch(String tableId, int shardId) {
        String cacheKey = tableId + ":" + shardId;
        Long cached = epochCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }
        String executorId = shardToExecutor.get(shardId);
        if (executorId == null) return 0L;
        ExecutorStub stub = executorStubs.get(executorId);
        if (stub == null) return 0L;
        long epoch = stub.getShardEpoch(tableId, shardId);
        if (epoch > 0) {
            epochCache.put(cacheKey, epoch);
        }
        return epoch;
    }

    /**
     * Invalidate epoch cache entry (called on StaleEpochException, forces next resolveShardEpoch to
     * go through RPC refresh).
     */
    public void invalidateEpochCache(String tableId, int shardId) {
        epochCache.remove(tableId + ":" + shardId);
    }

    /**
     * Unified handling after StaleEpochException: invalidate cache → re-resolve epoch(including RPC
     * + cache update)→ check if changed.
     *
     * <p>Correctness guarantee:
     *
     * <ol>
     *   <li>First invalidate cache to ensure resolveShardEpoch goes through RPC instead of
     *       returning stale cache
     *   <li>resolveShardEpoch internally puts cache (when freshEpoch > 0), fixing the bug where
     *       batch/update/delete paths don't update cache
     *   <li>freshEpoch == currentEpoch → epoch unchanged, caller should give up(return staleEpoch)
     *   <li>freshEpoch <= 0 → RPC failed or executor unreachable, caller should give up
     *   <li>freshEpoch > 0 and != currentEpoch → caller retries with new epoch
     * </ol>
     *
     * @param tableId Table identifier
     * @param shardId Shard ID
     * @param currentEpoch currently failed epoch
     * @return fresh epoch(>0 and != currentEpoch), or -1 means should give up
     */
    private long refreshEpochAfterStale(String tableId, int shardId, long currentEpoch) {
        invalidateEpochCache(tableId, shardId);
        long freshEpoch = resolveShardEpoch(tableId, shardId);
        if (freshEpoch <= 0 || freshEpoch == currentEpoch) {
            return -1L;
        }
        return freshEpoch;
    }

    public static class StaleEpochException extends RuntimeException {
        public StaleEpochException(String msg) {
            super(msg);
        }
    }
}
