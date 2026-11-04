package io.github.rawwise.engine.txn.orchestrate;

import io.github.rawwise.backend.lazyupdate.UpdateLog;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.ShardVersionTracker;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnResult;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

/**
 * Catalog level TxnOrchestrator default implementation.
 *
 * <h3>current stage(B1)routingstrategy</h3>
 *
 * <ul>
 *   <li>validates all ctx.tableIds() registered at begin
 *   <li>all transaction lifecycle methods(commit / abort / recover)delegate to the same delegate
 *       orchestrator -- this orchestrator is bound at creation time to "representative table"(first
 *       lexicographic in registeredTableIds)
 *   <li>exposes {@link #lmdbInstanceFor(String)} / {@link #trackerFor(String)} for subsequent B2
 *       stage WriteStage / DurableStage implementation to get resources by tableId
 * </ul>
 *
 * <h3>B1 single-table semantics remain</h3>
 *
 * <p>current stage, cross-table transaction multi-table commit pipeline <b>not yet
 * implemented</b>.if caller passed in ctx includes multiple tableIds, this class still routes to
 * delegate orchestrator(bound to single-table LMDB) — behavior fully consistent with pre-B2
 * refactoring.this means:
 *
 * <ul>
 *   <li>registermultipletable → ✓ OK
 *   <li>begin(Set.of(t1, t2)) multipletable ctx → ✓ OK(WAL TxN_BEGINRecord already supports
 *       multiple tableIds)
 *   <li>same transaction recordWrite to multiple tables → ⚠ actually only writes to delegate bound
 *       table
 * </ul>
 *
 * <p>B2 stage will rewrite commit pipeline, dispatch WriteIntents to respective LmdbInstances by
 * tableIds, and write GlobalCommitMarker to Metastore as unique commit decision point.
 */
@Slf4j
public final class CatalogLevelTxnOrchestratorImpl implements CatalogLevelTxnOrchestrator {

    private final String catalogName;
    private final String databaseName;
    private final Map<String, TableResource> tables;
    private final TxnOrchestrator delegate;
    private final String delegateTableId;

    public CatalogLevelTxnOrchestratorImpl(
            CatalogResources resources, TxnOrchestrator delegate, String delegateTableId) {
        Objects.requireNonNull(resources, "resources");
        this.catalogName = resources.catalogName();
        this.databaseName = resources.databaseName();
        this.tables = resources.tables();
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.delegateTableId = Objects.requireNonNull(delegateTableId, "delegateTableId");
        if (!this.tables.containsKey(delegateTableId)) {
            throw new IllegalArgumentException(
                    "delegateTableId="
                            + delegateTableId
                            + " not in registered tables "
                            + tables.keySet());
        }
        // inject tableLmdbResolver to delegate(only when delegate is TxnOrchestratorImpl).
        // this enables delegate to route to correct LmdbInstance by primaryTableId in multi-table
        // commit scenario.
        if (delegate instanceof TxnOrchestratorImpl impl) {
            impl.setTableLmdbResolver(this::lmdbInstanceFor);
            impl.setTableTrackerResolver(this::trackerFor);
            log.info(
                    "Injected tableLmdbResolver + tableTrackerResolver into delegate (catalog={})",
                    catalogName);
        } else {
            log.warn(
                    "Delegate is not TxnOrchestratorImpl ({}), multi-table dispatch disabled",
                    delegate.getClass().getName());
        }
        log.info(
                "CatalogLevelTxnOrchestrator created: catalog={}, db={}, registered={} tables, delegate-bound-to={}",
                catalogName,
                databaseName,
                tables.size(),
                delegateTableId);
    }

    @Override
    public String catalogName() {
        return catalogName;
    }

    @Override
    public String databaseName() {
        return databaseName;
    }

    @Override
    public Set<String> registeredTableIds() {
        return tables.keySet();
    }

    @Override
    public LmdbInstance lmdbInstanceFor(String tableId) {
        TableResource res = tables.get(tableId);
        if (res == null) {
            throw new IllegalArgumentException(
                    "tableId " + tableId + " not registered; registered=" + tables.keySet());
        }
        return res.getLmdbInstance();
    }

    @Override
    public ShardVersionTracker trackerFor(String tableId) {
        TableResource res = tables.get(tableId);
        if (res == null) {
            throw new IllegalArgumentException(
                    "tableId " + tableId + " not registered; registered=" + tables.keySet());
        }
        return res.getTracker();
    }

    // ================================================================
    // TxnLifecycle — delegate + multipletableregistervalidate
    // ================================================================

    @Override
    public TxnContext begin(Set<String> tableIds, IsolationLevel level) {
        Objects.requireNonNull(tableIds, "tableIds");
        if (tableIds.isEmpty()) {
            throw new IllegalArgumentException("tableIds must not be empty");
        }
        for (String tableId : tableIds) {
            if (!tables.containsKey(tableId)) {
                throw new IllegalArgumentException(
                        "tableId "
                                + tableId
                                + " not registered in catalog "
                                + catalogName
                                + "/"
                                + databaseName
                                + "; registered="
                                + tables.keySet());
            }
        }
        if (tableIds.size() > 1) {
            log.warn(
                    "Cross-table begin requested (tableIds={}). B1 stage routes to delegate orchestrator "
                            + "bound to {}; multi-table commit pipeline will be implemented in B2.",
                    tableIds,
                    delegateTableId);
        }
        return delegate.begin(tableIds, level);
    }

    @Override
    public TxnResult commit(TxnContext ctx) {
        return delegate.commit(ctx);
    }

    @Override
    public TxnResult abort(TxnContext ctx) {
        return delegate.abort(ctx);
    }

    /**
     * row lock acquisition forwarded to delegate(bound to representative table's
     * ShardVersionTracker).
     */
    @Override
    public void acquireForUpdate(TxnContext ctx, byte[] key, long timeoutMs)
            throws InterruptedException {
        delegate.acquireForUpdate(ctx, key, timeoutMs);
    }

    @Override
    public String recover(long txnId) {
        return delegate.recover(txnId);
    }

    // ================================================================
    // TxnRecorder — delegate
    // ================================================================

    @Override
    public void recordRead(TxnContext ctx, String tableId, byte[] key, byte[] value) {
        delegate.recordRead(ctx, tableId, key, value);
    }

    @Override
    public void recordReadRange(TxnContext ctx, String tableId, byte[] start, byte[] end) {
        delegate.recordReadRange(ctx, tableId, start, end);
    }

    @Override
    public void recordWrite(
            TxnContext ctx, String tableId, byte[] key, byte[] value, long baseVersion) {
        delegate.recordWrite(ctx, tableId, key, value, baseVersion);
    }

    @Override
    public void recordUpdateLog(TxnContext ctx, String tableId, UpdateLog updateLog) {
        delegate.recordUpdateLog(ctx, tableId, updateLog);
    }

    // ================================================================
    // TxnBatchCoordinator — delegate
    // ================================================================

    @Override
    public TxnResult commitBatch(BatchCommitRequest request) {
        return delegate.commitBatch(request);
    }

    @Override
    public TxnResult abortBatch(String idempotencyKey) {
        return delegate.abortBatch(idempotencyKey);
    }

    @Override
    public String decideBatchRecovery(String idempotencyKey) {
        return delegate.decideBatchRecovery(idempotencyKey);
    }

    // ================================================================
    // TxnReader — delegate
    // ================================================================

    @Override
    public Optional<byte[]> pointRead(String tableId, byte[] key, TxnContext ctx) {
        return delegate.pointRead(tableId, key, ctx);
    }

    @Override
    public java.util.List<TxnReader.KeyValuePair> rangeRead(
            String tableId, byte[] start, byte[] end, TxnContext ctx) {
        return delegate.rangeRead(tableId, start, end, ctx);
    }

    @Override
    public Optional<byte[]> pointReadAtSnapshot(String tableId, byte[] key, long snapshotTs) {
        return delegate.pointReadAtSnapshot(tableId, key, snapshotTs);
    }

    // ================================================================
    // Pending flush APIs(Spark/Flink ingestion, B1 all delegated)
    // ================================================================

    @Override
    public void flushPending(TxnContext ctx, String tableId) {
        delegate.flushPending(ctx, tableId);
    }

    @Override
    public java.util.List<String> flushPendingWithInstance(
            TxnContext ctx,
            String tableId,
            Function<String, LmdbInstance> pendingInstanceSupplier,
            Function<String, LmdbInstance> newPendingInstanceSupplier) {
        return delegate.flushPendingWithInstance(
                ctx, tableId, pendingInstanceSupplier, newPendingInstanceSupplier);
    }

    @Override
    public java.util.List<String> commitTailBatch(
            String tableId,
            Function<String, LmdbInstance> pendingInstanceSupplier,
            Function<String, LmdbInstance> newPendingInstanceSupplier) {
        return delegate.commitTailBatch(
                tableId, pendingInstanceSupplier, newPendingInstanceSupplier);
    }
}
