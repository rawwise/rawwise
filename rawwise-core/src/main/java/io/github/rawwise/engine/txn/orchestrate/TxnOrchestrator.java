package io.github.rawwise.engine.txn.orchestrate;

import io.github.rawwise.backend.lazyupdate.UpdateLog;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnResult;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * unifiedtransactionorchestrator, single entry point for all transaction paths.
 *
 * <p>combination of 4 responsibility interfaces:
 *
 * <ul>
 *   <li>{@link TxnLifecycle} — transactionlifecyclemanage
 *   <li>{@link TxnRecorder} — read/write record tracking
 *   <li>{@link TxnBatchCoordinator} — Batch commitcoordination
 *   <li>{@link TxnReader} — Read path
 * </ul>
 *
 * <p>Implementation class: {@link TxnOrchestratorImpl}
 */
public interface TxnOrchestrator extends TxnLifecycle, TxnRecorder, TxnBatchCoordinator, TxnReader {

    // ================================================================
    // Batch idempotency tracking
    // ================================================================

    /**
     * batch idempotent record, for crash recovery and duplicate commit detection.
     *
     * @param batchId batchidentifier
     * @param idempotencyKey idempotent key(for deduplication)
     * @param tableId table identifier
     * @param state batchcurrentlystate
     * @param shardCount shardcount
     * @param shardInstanceIds shard LMDB instance ID list
     * @param txnId associatetransaction ID
     * @param commitTs commit timestamp
     */
    record BatchIdempotencyRecord(
            String batchId,
            String idempotencyKey,
            String tableId,
            BatchState state,
            int shardCount,
            List<String> shardInstanceIds,
            long txnId,
            long commitTs) {

        public BatchIdempotencyRecord {
            if (batchId == null || batchId.isBlank())
                throw new IllegalArgumentException("batchId must not be blank");
            if (idempotencyKey == null || idempotencyKey.isBlank())
                throw new IllegalArgumentException("idempotencyKey must not be blank");
            if (tableId == null || tableId.isBlank())
                throw new IllegalArgumentException("tableId must not be blank");
            if (state == null) throw new IllegalArgumentException("state must not be null");
            if (shardInstanceIds == null) shardInstanceIds = List.of();
        }
    }

    // ================================================================
    // WRITE_THROUGH flush
    // ================================================================

    /**
     * Flush pending batch for WRITE_THROUGH ingestion mode. Ensures pending writes are flushed to
     * LMDB staging.
     *
     * <p>Uses the {@code io.github.rawwise.engine.txn.state.TxnContext} type because the bridge
     * resolves TxnContext from {@code TxnContextHolder}.
     *
     * @param ctx transaction context from TxnContextHolder
     * @param tableId table identifier
     */
    void flushPending(TxnContext ctx, String tableId);

    /**
     * Functional WRITE_THROUGH flush with LMDB instance suppliers. Performs actual batchPut to the
     * pending LMDB instance.
     *
     * <p>Uses the {@code io.github.rawwise.engine.txn.state.TxnContext} type. Handles MAP_FULL by
     * creating a new instance via supplier.
     *
     * @param ctx transaction context
     * @param tableId table identifier
     * @param pendingInstanceSupplier supplies the current pending LMDB instance
     * @param newPendingInstanceSupplier supplies a new pending LMDB instance on MAP_FULL
     * @return list of instance IDs that were written to (for subsequent commitPendingInstances
     *     call)
     */
    List<String> flushPendingWithInstance(
            TxnContext ctx,
            String tableId,
            Function<String, LmdbInstance> pendingInstanceSupplier,
            Function<String, LmdbInstance> newPendingInstanceSupplier);

    /**
     * Flush residual pending batch at commit time (tail batch). Called when the pending batch is
     * not full but a commit is imminent.
     *
     * @param tableId table identifier
     * @param pendingInstanceSupplier supplies the current pending LMDB instance
     * @param newPendingInstanceSupplier supplies a new pending LMDB instance on MAP_FULL
     * @return list of instance IDs that were written to (for subsequent commitPendingInstances
     *     call)
     */
    List<String> commitTailBatch(
            String tableId,
            Function<String, LmdbInstance> pendingInstanceSupplier,
            Function<String, LmdbInstance> newPendingInstanceSupplier);
}

/** transactionlifecyclemanage. */
interface TxnLifecycle {

    /**
     * start transaction.
     *
     * <p>step:
     *
     * <ol>
     *   <li>via clock.next() allocation txnId ≡ commitTs
     *   <li>create ReadSnapshot(REPEATABLE_READ/SERIALIZABLE)
     *   <li>create TxnContext
     *   <li>write WAL TXN_BEGIN
     * </ol>
     *
     * @param tableIds set of involved table IDs
     * @param level isolationlevel
     * @return new transaction context
     */
    TxnContext begin(Set<String> tableIds, IsolationLevel level);

    /**
     * commit transaction (6-step pipeline).
     *
     * @param ctx transaction context
     * @return commitresult
     */
    TxnResult commit(TxnContext ctx);

    /**
     * rollbacktransaction.
     *
     * @param ctx transaction context
     * @return rollbackresult
     */
    TxnResult abort(TxnContext ctx);

    /**
     * get EXCLUSIVE rowlock(SELECT FOR UPDATE semantics).
     *
     * <p>typical usage:
     *
     * <pre>{@code
     * TxnContext ctx = orchestrator.begin(tables, IsolationLevel.READ_COMMITTED);
     * orchestrator.acquireForUpdate(ctx, encodedKey, 5_000L); // block at most 5s
     * byte[] v = orchestrator.pointRead(tableId, encodedKey, ctx);
     * orchestrator.recordWrite(ctx, tableId, encodedKey, newValue, baseVersion);
     * orchestrator.commit(ctx); // commit/abort/cleanupTxn end auto releases all
     * }</pre>
     *
     * <p>multiple calls to same key within same transaction are reentrant-safe.
     *
     * <p><b>lock order convention (prevent deadlock)</b>: caller must acquire locks by key
     * lexicographic ascending. see {@link
     * io.github.rawwise.engine.txn.ShardVersionTracker#acquireForUpdate}.
     *
     * <p>default implementation: throw {@link UnsupportedOperationException}, let specific
     * orchestrator decide whether to enable.
     *
     * @param ctx transaction context
     * @param key row key
     * @param timeoutMs timeout (milliseconds)
     * @throws InterruptedException wait interrupted
     * @throws RuntimeException timeout({@code ShardVersionTracker.LockTimeoutException})
     */
    default void acquireForUpdate(TxnContext ctx, byte[] key, long timeoutMs)
            throws InterruptedException {
        throw new UnsupportedOperationException(
                "acquireForUpdate not supported by " + getClass().getSimpleName());
    }

    /**
     * crash recovery: recover specified txnId's transaction state.
     *
     * @param txnId transaction ID
     * @return post-recovery state description
     */
    String recover(long txnId);
}

/** read/write record tracking. */
interface TxnRecorder {

    /**
     * recordpoint read.
     *
     * @param ctx transaction context
     * @param tableId table ID
     * @param key key
     * @param value read value
     */
    void recordRead(TxnContext ctx, String tableId, byte[] key, byte[] value);

    /**
     * recordrange read.
     *
     * @param ctx transaction context
     * @param tableId table ID
     * @param start start key (inclusive)
     * @param end end key (inclusive)
     */
    void recordReadRange(TxnContext ctx, String tableId, byte[] start, byte[] end);

    /**
     * recordwrite.
     *
     * @param ctx transaction context
     * @param tableId table ID
     * @param key key
     * @param value value
     * @param baseVersion base version
     */
    void recordWrite(TxnContext ctx, String tableId, byte[] key, byte[] value, long baseVersion);

    /**
     * Record UpdateLog (cross-domain TXN-LU-002 contract).
     *
     * <p>When a write operation produces a LazyUpdate UpdateLog file, call this method to UpdateLog
     * associate it with the current transaction. On commit, via {@code
     * LazyUpdateManager.appendCommitRecord()} writes commit record to active.nwl; On rollback, via
     * {@code appendAbortRecord()} writes abort record.
     *
     * @param ctx transaction context
     * @param tableId table ID
     * @param updateLog update log
     * @throws IllegalStateException if ctx is null or not in ACTIVE state
     * @throws IllegalArgumentException if tableId is blank or updateLog is null
     */
    void recordUpdateLog(TxnContext ctx, String tableId, UpdateLog updateLog);
}

/** Batch commit coordination (Spark/Flink ingestion). */
interface TxnBatchCoordinator {

    /** Batch commit request. */
    record BatchCommitRequest(
            String tableId,
            String batchId,
            String idempotencyKey,
            long baseSnapshotId,
            int expectedShardCount,
            List<byte[]> keys,
            List<byte[]> values) {

        public BatchCommitRequest {
            if (tableId == null || tableId.isBlank())
                throw new IllegalArgumentException("tableId must not be blank");
            if (batchId == null || batchId.isBlank())
                throw new IllegalArgumentException("batchId must not be blank");
            if (keys == null) keys = List.of();
            if (values == null) values = List.of();
        }
    }

    /**
     * Batch commit.
     *
     * @param request Batch commit request
     * @return commitresult
     */
    TxnResult commitBatch(BatchCommitRequest request);

    /**
     * Batch rollback.
     *
     * @param idempotencyKey idempotent key
     * @return rollbackresult
     */
    TxnResult abortBatch(String idempotencyKey);

    /**
     * Batch recovery decision.
     *
     * @param idempotencyKey idempotent key
     * @return recovery decision
     */
    String decideBatchRecovery(String idempotencyKey);
}

/** Read path. */
interface TxnReader {

    /**
     * Point read (with transaction context).
     *
     * @param tableId table ID
     * @param key key
     * @param ctx transaction context
     * @return visible value, or Optional.empty() if none
     */
    Optional<byte[]> pointRead(String tableId, byte[] key, TxnContext ctx);

    /**
     * Range read (with transaction context).
     *
     * @param tableId table ID
     * @param start start key (inclusive)
     * @param end end key (inclusive)
     * @param ctx transaction context
     * @return list of visible key-value pairs
     */
    List<KeyValuePair> rangeRead(String tableId, byte[] start, byte[] end, TxnContext ctx);

    /**
     * PIT point read (specified snapshot timestamp).
     *
     * @param tableId table ID
     * @param key key
     * @param snapshotTs snapshot timestamp
     * @return visible value, or Optional.empty() if none
     */
    Optional<byte[]> pointReadAtSnapshot(String tableId, byte[] key, long snapshotTs);

    /** Key-value pair. */
    record KeyValuePair(byte[] key, byte[] value) {
        public KeyValuePair {
            if (key == null) throw new IllegalArgumentException("key must not be null");
            if (value == null) throw new IllegalArgumentException("value must not be null");
        }
    }
}
