package io.github.rawwise.engine.txn.orchestrate;

import io.github.rawwise.backend.lazyupdate.LazyUpdateManager;
import io.github.rawwise.backend.lazyupdate.UpdateLog;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.mvcc.LmdbVersionChainStore;
import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.ShardVersionTracker;
import io.github.rawwise.engine.txn.TxnMode;
import io.github.rawwise.engine.txn.TxnReadCache;
import io.github.rawwise.engine.txn.WriteBuffer;
import io.github.rawwise.engine.txn.clock.LogicalClock;
import io.github.rawwise.engine.txn.isolate.RangeReadSet;
import io.github.rawwise.engine.txn.isolate.ReadSnapshot;
import io.github.rawwise.engine.txn.isolate.VisibilityChecker;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnContextHolder;
import io.github.rawwise.engine.txn.state.TxnResult;
import io.github.rawwise.engine.txn.state.TxnState;
import io.github.rawwise.engine.txn.store.PITReader;
import io.github.rawwise.engine.txn.store.VersionChainStore;
import io.github.rawwise.engine.txn.wal.TxnWALReader;
import io.github.rawwise.engine.txn.wal.TxnWALRecord;
import io.github.rawwise.engine.txn.wal.TxnWALWriter;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * unifiedtransactionorchestratorimplementation — single entry point for all transaction paths.
 *
 * <p>Implements 6-step commit pipeline:
 *
 * <pre>
 * begin() → WriteStage → ValidateStage → DurableStage
 * → PublishStage → WAL COMMITTED → ACK
 * </pre>
 *
 * <p>Design principlesthen:
 *
 * <ul>
 *   <li>All write operations go through the same transaction state machine pipeline
 *   <li>commitTs is pre-allocated in begin() via clock.next() (unified strategy)
 *   <li>OCC validation in ValidateStage, lock order: timestamp first, lock second (INV-001)
 *   <li>DurableStage ensures atomicity via LMDB batchPut + CommitMarker (INV-006)
 *   <li>PublishStage ensures global atomicity via Metastore CAS (CCI-011)
 * </ul>
 */
@Slf4j
public final class TxnOrchestratorImpl implements TxnOrchestrator {

    private final LogicalClock clock;
    private final Metastore metastore;
    private final LmdbInstance lmdbInstance;
    private final VersionChainStore versionChainStore;
    private final PITReader pitReader;
    private final ValidateStage validateStage;
    private final TxnWALWriter walWriter;
    private final WriteStage writeStage;
    private final DurableStage durableStage;
    private final PublishStage publishStage;

    @Setter private ShardVersionTracker tracker;

    // Active transaction index
    private final ConcurrentHashMap<Long, TxnContext> activeTxnIndex = new ConcurrentHashMap<>();

    // transaction ID → RangeReadSet mapping
    private final ConcurrentHashMap<Long, RangeReadSet> rangeReadSets = new ConcurrentHashMap<>();

    // transaction ID → WriteIntent listmapping
    private final ConcurrentHashMap<Long, List<TxnWALRecord.TxnWriteIntent>> writeIntentsIndex =
            new ConcurrentHashMap<>();

    // transaction ID → (tableId → (key → baseVersion)) mapping, for OCC conflictdetection
    private final ConcurrentHashMap<
                    Long, ConcurrentHashMap<String, ConcurrentHashMap<ByteBuffer, Long>>>
            keyBaseVersions = new ConcurrentHashMap<>();

    // batchidempotentindex: idempotencyKey → BatchIdempotencyRecord
    private final ConcurrentHashMap<String, BatchIdempotencyRecord> batchIndex =
            new ConcurrentHashMap<>();

    // cross-domain TXN-LU-002: txnId → (tableId → UpdateLog list)
    private final ConcurrentHashMap<Long, ConcurrentHashMap<String, List<UpdateLog>>>
            pendingUpdateLogPaths = new ConcurrentHashMap<>();

    // timeoutcleanup: txnId → deadlineMs
    private final ConcurrentHashMap<Long, Long> txnTimeoutIndex = new ConcurrentHashMap<>();

    // Timeout cleanup scheduler
    private final ScheduledExecutorService cleanupScheduler;
    private final long txnTimeoutMs;

    // Cross-domain TXN-LU-002: LazyUpdate manager
    @Setter private LazyUpdateManager lazyUpdateManager;

    // LMDB-backed version chain store for MVCC persistence
    @Setter private LmdbVersionChainStore lmdbVersionChainStore;

    // TxnWALReader for complete WAL-based crash recovery (H4).
    // When null, recover() falls back to marker-only check.
    @Setter private TxnWALReader walReader;

    /**
     * Multi-table LMDB resolver. When set, the commit path uses this resolver instead of
     * lmdbInstance for multi-table batchPutWithMarker. Injected by CatalogLevelTxnOrchestratorImpl
     * during assembly.
     */
    @Setter
    private java.util.function.Function<String, io.github.rawwise.engine.storage.LmdbInstance>
            tableLmdbResolver;

    /**
     * Multi-table transaction ShardVersionTracker resolver. When injected, ValidateStage and
     * cleanup paths use the corresponding table's tracker by intent.primaryTableId, instead of the
     * global singleton tracker.
     */
    @Setter private java.util.function.Function<String, ShardVersionTracker> tableTrackerResolver;

    // Catalog/Database info
    private final String catalogName;
    private final String databaseName;

    /**
     * Create TxnOrchestratorImpl.
     *
     * @param clock logical clock
     * @param metastore Metastore instance
     * @param lmdbInstance LMDB instance
     * @param versionChainStore version chain store
     * @param walWriter WAL writer( defaults to InMemoryTxnWALWriter)
     * @param validateStage validation stage (must not be null, default implementation provided by
     *     Factory)
     * @param writeStage write stage (must not be null, default implementation provided by Factory)
     * @param durableStage durable stage (must not be null, default implementation provided by
     *     Factory)
     * @param publishStage publish stage (must not be null, default implementation provided by
     *     Factory)
     * @param catalogName Catalog name
     * @param databaseName Database name
     * @throws NullPointerException if any parameter is null
     */
    public TxnOrchestratorImpl(
            LogicalClock clock,
            Metastore metastore,
            LmdbInstance lmdbInstance,
            VersionChainStore versionChainStore,
            TxnWALWriter walWriter,
            ValidateStage validateStage,
            WriteStage writeStage,
            DurableStage durableStage,
            PublishStage publishStage,
            String catalogName,
            String databaseName,
            long txnTimeoutMs) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.metastore = Objects.requireNonNull(metastore, "metastore");
        this.lmdbInstance = Objects.requireNonNull(lmdbInstance, "lmdbInstance");
        this.versionChainStore = Objects.requireNonNull(versionChainStore, "versionChainStore");
        this.pitReader = new PITReader(versionChainStore);
        this.validateStage = Objects.requireNonNull(validateStage, "validateStage");
        this.walWriter = Objects.requireNonNull(walWriter, "walWriter");
        this.writeStage = Objects.requireNonNull(writeStage, "writeStage");
        this.durableStage = Objects.requireNonNull(durableStage, "durableStage");
        this.publishStage = Objects.requireNonNull(publishStage, "publishStage");
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
        this.databaseName = Objects.requireNonNull(databaseName, "databaseName");
        this.txnTimeoutMs = txnTimeoutMs;
        this.cleanupScheduler = initCleanupScheduler();
    }

    public TxnOrchestratorImpl(
            LogicalClock clock,
            Metastore metastore,
            LmdbInstance lmdbInstance,
            VersionChainStore versionChainStore,
            TxnWALWriter walWriter,
            ValidateStage validateStage,
            WriteStage writeStage,
            DurableStage durableStage,
            PublishStage publishStage,
            String catalogName,
            String databaseName) {
        this(
                clock,
                metastore,
                lmdbInstance,
                versionChainStore,
                walWriter,
                validateStage,
                writeStage,
                durableStage,
                publishStage,
                catalogName,
                databaseName,
                300_000L);
    }

    private ScheduledExecutorService initCleanupScheduler() {
        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "TxnOrchestrator-Timeout-Cleanup");
                            t.setDaemon(true);
                            return t;
                        });
        scheduler.scheduleAtFixedRate(this::cleanupExpiredTransactions, 60, 60, TimeUnit.SECONDS);
        return scheduler;
    }

    public void cleanupExpiredTransactions() {
        long now = System.currentTimeMillis();
        List<Long> expired = new ArrayList<>();
        for (Map.Entry<Long, Long> entry : txnTimeoutIndex.entrySet()) {
            if (entry.getValue() <= now) {
                expired.add(entry.getKey());
            }
        }
        for (long txnId : expired) {
            try {
                TxnContext ctx = activeTxnIndex.get(txnId);
                if (ctx == null) {
                    cleanupTxn(txnId);
                    continue;
                }
                // H-1 fix: only allow timeout cleanup when transaction is in BEGINNING/ACTIVE
                // state.
                // once entering VALIDATING/PREPARING/DATA_DURABLE, commit path has started
                // advancing pipeline(partially already in commitLock), cleanup intervention will
                // contend with commit
                // contention(e.g., tombstone marker and PublishStage writing TxnCommitRecord
                // causing silent data loss).let the transaction naturally complete commit.
                TxnState state = ctx.currentState();
                if (state != TxnState.BEGINNING && state != TxnState.ACTIVE) {
                    log.debug(
                            "cleanupExpiredTransactions: txnId={} state={} is mid-commit, skipping",
                            txnId,
                            state);
                    continue;
                }
                log.warn(
                        "cleanupExpiredTransactions: txnId={} exceeded timeout {}ms, force-aborting",
                        txnId,
                        txnTimeoutMs);
                abort(ctx);
            } catch (Exception e) {
                log.warn(
                        "cleanupExpiredTransactions: txnId={} cleanup failed: {}",
                        txnId,
                        e.getMessage());
                txnTimeoutIndex.remove(txnId);
            }
        }
    }

    public void close() {
        cleanupScheduler.shutdown();
        try {
            if (!cleanupScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        try {
            lmdbInstance.close();
        } catch (Exception e) {
            log.warn("Failed to close LMDB instance: {}", e.getMessage());
        }
    }

    // ================================================================
    // TxnLifecycle
    // ================================================================

    @Override
    public TxnContext begin(Set<String> tableIds, IsolationLevel level) {
        long txnId = clock.next(); // commitTs ≡ txnId

        long now = clock.now();
        long baseSnapshotId = (txnId > 1) ? txnId - 1 : 0;
        ReadSnapshot readSnapshot = ReadSnapshot.forLevel(level, now, baseSnapshotId);

        WriteBuffer writeBuffer = new WriteBuffer();
        TxnReadCache readCache = new TxnReadCache();

        TxnContext ctx =
                new TxnContext(txnId, readSnapshot, level, tableIds, writeBuffer, readCache);

        activeTxnIndex.put(txnId, ctx);
        rangeReadSets.put(txnId, new RangeReadSet());
        keyBaseVersions.put(txnId, new ConcurrentHashMap<>());
        txnTimeoutIndex.put(txnId, System.currentTimeMillis() + txnTimeoutMs);

        log.info(
                "begin: {} tables={} level={} readSnapshot={}",
                ctx.identity(),
                tableIds,
                level,
                readSnapshot);

        // WAL TXN_BEGIN
        walWriter.writeBegin(
                new TxnWALRecord.TxnBeginRecord(
                        txnId, now, level, tableIds, System.currentTimeMillis()));

        // State: BEGINNING → ACTIVE (WAL BEGIN fsync → ACTIVE)
        ctx.transitionTo(TxnState.ACTIVE);

        return ctx;
    }

    @Override
    public TxnResult commit(TxnContext ctx) {
        long txnId = ctx.txnId();
        log.info("commit: {} -- starting 6-step pipeline", ctx.identity());

        try {
            return doCommit(ctx, txnId);
        } catch (Exception e) {
            log.error("commit: {} unexpected error: {}", ctx.identity(), e.getMessage(), e);
            // B-2 fix: exception cleanup path must not swallow Exception.each step failure must
            // log.error
            // for diagnosis; but any step failure should not block subsequent cleanup(therefore
            // independent try-catch).
            try {
                rollbackPendingInstances(txnId);
            } catch (RuntimeException rollbackEx) {
                log.error(
                        "commit: {} rollbackPendingInstances failed in error path",
                        ctx.identity(),
                        rollbackEx);
            }
            try {
                walWriter.writeAbort(
                        new TxnWALRecord.TxnAbort(
                                txnId,
                                "commit-failed:" + e.getMessage(),
                                System.currentTimeMillis()));
            } catch (RuntimeException walEx) {
                log.error("commit: {} WAL writeAbort failed in error path", ctx.identity(), walEx);
            }
            cleanupTxn(txnId);
            return new TxnResult.CommitConflict("commit failed: " + e.getMessage());
        }
    }

    private TxnResult doCommit(TxnContext ctx, long txnId) {
        Map<String, Map<ByteBuffer, Long>> baseVersions = new HashMap<>();
        ConcurrentHashMap<String, ConcurrentHashMap<ByteBuffer, Long>> txnBaseVersions =
                keyBaseVersions.get(txnId);
        if (txnBaseVersions != null) {
            for (var entry : txnBaseVersions.entrySet()) {
                baseVersions.put(entry.getKey(), new HashMap<>(entry.getValue()));
            }
        }
        WriteStage.WriteResult writeResult =
                writeStage.execute(ctx, TxnMode.DEFERRED, baseVersions);
        if (writeResult.intents().isEmpty()) {
            log.info("commit: {} no writes (read-only), returning OK", ctx.identity());
            // M-12: read-only transaction skips OCC/Metastore CAS.WAL COMMITTED still requires
            // write
            // to mark transaction complete(required for crash recovery), but when walMode=NONE,
            // naturally skipped by NoOpTxnWALWriter.
            walWriter.writeCommitted(new TxnWALRecord.TxnCommitted(txnId, txnId, new byte[0]));
            cleanupTxn(txnId);
            return new TxnResult.CommitOk(txnId);
        }
        writeIntentsIndex.put(txnId, writeResult.intents());

        for (TxnWALRecord.TxnWriteIntent intent : writeResult.intents()) {
            walWriter.writeWriteIntent(intent);
        }

        // State: ACTIVE → VALIDATING → PREPARING
        ctx.transitionTo(TxnState.VALIDATING);
        ctx.transitionTo(TxnState.PREPARING);

        // --- PREPARE + DurableStage ---
        log.debug("commit: {} Step 3/6 DurableStage", ctx.identity());
        try {
            DurableStage.DurableResult durableResult;
            // multi-table transaction uses tableLmdbResolver; single-table transaction keeps
            // original path
            if (tableLmdbResolver != null && hasMultipleTables(writeResult.intents())) {
                log.info(
                        "commit: {} multi-table durable path ({} intents across {} tables)",
                        ctx.identity(),
                        writeResult.intents().size(),
                        countDistinctTables(writeResult.intents()));
                durableResult =
                        durableStage.executeMultiTable(
                                ctx,
                                writeResult.intents(),
                                tableLmdbResolver,
                                TxnMode.DEFERRED,
                                walWriter);
            } else {
                durableResult =
                        durableStage.execute(
                                ctx,
                                writeResult.intents(),
                                lmdbInstance,
                                TxnMode.DEFERRED,
                                walWriter);
            }
            log.debug(
                    "commit: {} durable: {} keys, {} bytes, marker={}",
                    ctx.identity(),
                    durableResult.persistedKeyCount(),
                    durableResult.persistedByteCount(),
                    durableResult.commitMarkerWritten());
        } finally {
        }

        ctx.transitionTo(TxnState.DATA_DURABLE);

        // --- ValidateStage + PublishStage under commitLock ---
        // multi-table transaction injects tableTrackerResolver
        if (tableTrackerResolver != null && hasMultipleTables(writeResult.intents())) {
            validateStage.setTableTrackerResolver(tableTrackerResolver);
        }

        java.util.concurrent.locks.ReentrantLock commitLock =
                tracker != null ? tracker.getCommitLock() : null;
        if (commitLock != null) commitLock.lock();
        try {
            log.debug("commit: {} Step 2/6 ValidateStage", ctx.identity());
            ValidateStage.ValidateResult validateResult =
                    validateStage.execute(
                            ctx, writeResult.intents(), rangeReadSets.get(txnId), baseVersions);
            if (validateResult instanceof ValidateStage.ValidateResult.Conflict conflict) {
                log.warn(
                        "commit: {} OCC conflict after durable: {}",
                        ctx.identity(),
                        conflict.reason());
                rollbackPendingInstances(txnId);
                walWriter.writeAbort(
                        new TxnWALRecord.TxnAbort(
                                txnId,
                                "occ-conflict:" + conflict.reason(),
                                System.currentTimeMillis()));
                cleanupTxn(txnId);
                return new TxnResult.CommitConflict(conflict.reason());
            }

            log.debug("commit: {} Step 4/6 PublishStage", ctx.identity());
            PublishStage.PublishResult publishResult =
                    publishStage.execute(
                            ctx,
                            writeResult.intents(),
                            metastore,
                            catalogName,
                            databaseName,
                            ctx.primaryTableId(),
                            validateStage);
            if (!publishResult.success()) {
                log.error(
                        "commit: {} PublishStage failed: {}",
                        ctx.identity(),
                        publishResult.errorMessage());
                rollbackPendingInstances(txnId);
                walWriter.writeAbort(
                        new TxnWALRecord.TxnAbort(
                                txnId,
                                "publish-failed:" + publishResult.errorMessage(),
                                System.currentTimeMillis()));
                cleanupTxn(txnId);
                return new TxnResult.CommitConflict(
                        "PublishStage failed: " + publishResult.errorMessage());
            }
        } finally {
            if (commitLock != null) commitLock.unlock();
        }

        // State: DATA_DURABLE → MANIFEST_COMMITTED → PUBLISHED
        ctx.transitionTo(TxnState.MANIFEST_COMMITTED);
        ctx.transitionTo(TxnState.PUBLISHED);

        // --- Write Version Chains (C1 fix: moved AFTER PublishStage) ---
        // Version chain writes must only happen after PublishStage succeeds,
        // otherwise phantom versions could exist for a transaction whose
        // PublishStage failed, violating MVCC invariants.
        for (TxnWALRecord.TxnWriteIntent intent : writeResult.intents()) {
            VersionChainStore.VersionEntry versionEntry =
                    new VersionChainStore.VersionEntry(
                            txnId, intent.valueRef() != null ? intent.valueRef() : new byte[0], 0);
            versionChainStore.put(intent.primaryTableId(), intent.keyBytes(), versionEntry);
        }

        // Also persist version chain entries to LMDB for snapshot reads
        // FIX-2: put() returns boolean (false on MAP_FULL), not throws.
        // Must check the return value instead of relying on RuntimeException.
        if (lmdbVersionChainStore != null) {
            for (TxnWALRecord.TxnWriteIntent intent : writeResult.intents()) {
                boolean ok =
                        lmdbVersionChainStore.put(
                                intent.primaryTableId(),
                                intent.keyBytes(),
                                txnId,
                                intent.valueRef() != null ? intent.valueRef() : new byte[0]);
                if (!ok) {
                    log.error(
                            "CRITICAL: Failed to persist version chain to LMDB for tableId={} commitTs={}. "
                                    + "Main data is committed but MVCC version chain is missing.",
                            intent.primaryTableId(),
                            txnId);
                }
            }
            log.debug(
                    "commit: {} persisted {} version chain entries to LMDB",
                    ctx.identity(),
                    writeResult.intents().size());
        }

        // --- Commit UpdateLogs (cross-domain TXN-LU-002) ---
        // Best-effort: main data already globally visible via PublishStage.
        // UpdateLog commit failure should not block transaction completion, only log for subsequent
        // recovery.
        try {
            commitUpdateLogsIfPresent(txnId);
        } catch (RuntimeException e) {
            log.error(
                    "commit: {} UpdateLog commit failed after data published. "
                            + "Main data is committed and visible. "
                            + "UpdateLogs may require manual recovery.",
                    ctx.identity(),
                    e);
        }

        // --- WAL TXN_COMMITTED ---
        log.debug("commit: {} Step 5/6 WAL COMMITTED", ctx.identity());
        walWriter.writeCommitted(new TxnWALRecord.TxnCommitted(txnId, txnId, new byte[0]));

        // State: PUBLISHED → COMMITTED
        ctx.transitionTo(TxnState.COMMITTED);

        // --- ACK ---
        log.info("commit: {} Step 6/6 ACK — committed at ts={}", ctx.identity(), txnId);
        cleanupTxn(txnId);
        return new TxnResult.CommitOk(txnId);
    }

    @Override
    public TxnResult abort(TxnContext ctx) {
        long txnId = ctx.txnId();
        log.info("abort: {} reason=user-requested", ctx.identity());

        TxnState currentState = ctx.currentState();
        if (currentState == TxnState.DATA_DURABLE) {
            byte[] markerKey =
                    ("__commit_marker__:" + txnId)
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] markerValue = lmdbInstance.get(markerKey);
            if (markerValue != null && markerValue.length > 0 && markerValue[0] == 1) {
                log.warn(
                        "abort: txn={} DATA_DURABLE with active CommitMarker, refusing unsafe abort",
                        txnId);
                return new TxnResult.CommitConflict(
                        "Transaction is durable with active marker, cannot safely abort");
            }
        }

        ctx.transitionTo(TxnState.ABORTING);

        try {
            rollbackPendingInstances(txnId);
            ctx.writeBuffer().clear();
            ctx.readCache().clear();
            walWriter.writeAbort(
                    new TxnWALRecord.TxnAbort(txnId, "user-requested", System.currentTimeMillis()));
            rollbackUpdateLogsIfPresent(txnId);
        } finally {
            cleanupTxn(txnId);
        }

        ctx.transitionTo(TxnState.ABORTED);
        return new TxnResult.AbortResult(txnId, "user-requested abort");
    }

    @Override
    public String recover(long txnId) {
        log.info("recover: txnId={}", txnId);

        // Check if txn is still active
        TxnContext ctx = activeTxnIndex.get(txnId);
        if (ctx != null) {
            abort(ctx);
            return "aborted-active-txn:" + txnId;
        }

        // H4: When TxnWALReader is configured, perform complete WAL-based recovery
        // (read WAL → replay WriteStage → replay DurableStage →
        // check PublishStage → decide commit/abort).
        if (walReader != null) {
            return recoverWithWAL(txnId);
        }

        // Fallback: marker-only check (when TxnWALReader not configured)
        return recoverWithMarkerOnly(txnId);
    }

    /**
     * Replay PublishStage on recovery: update per-key versions and write TxnCommitRecord.
     *
     * <p>When recovering a transaction whose DurableStage completed (marker present), we replay the
     * PublishStage steps that were missed: 1. Update ShardVersionTracker per-key commitTs 2. Write
     * TxnCommitRecord in metastore 3. Advance logical clock
     */
    private void replayPublishStageOnRecovery(
            long txnId, List<TxnWALRecord.TxnWriteIntent> walIntents) {
        List<TxnWALRecord.TxnWriteIntent> intents =
                (walIntents != null && !walIntents.isEmpty())
                        ? walIntents
                        : writeIntentsIndex.get(txnId);

        // B-1 fix: tracker version update must maintain monotonicity.
        // if another transaction writes updated version to same key during recovery(commitTs >
        // txnId),
        // directly setCommitTs will fall back version number, breaking ShardVersionTracker's
        // monotonic invariant
        // and invalidating subsequent OCC check.here only update when current < txnId.
        if (tracker != null && intents != null) {
            int updatedCount = 0;
            int skippedCount = 0;
            for (TxnWALRecord.TxnWriteIntent intent : intents) {
                long currentTs = tracker.getCommitTs(intent.keyBytes());
                if (currentTs < txnId) {
                    tracker.setCommitTs(intent.keyBytes(), txnId, txnId);
                    updatedCount++;
                } else {
                    skippedCount++;
                }
            }
            log.info(
                    "replayPublishStage: txnId={} tracker updated={} skipped={} (skip preserves monotonicity)",
                    txnId,
                    updatedCount,
                    skippedCount);
        }

        // M-3 fix: supplement latestSnapshot update(CCI-011).
        // otherwise after crash recovery /snapshot_latest/... will not point to recovered
        // transaction,
        // subsequent read path may not see this commitTs.
        String txnTableName =
                (intents != null && !intents.isEmpty())
                        ? intents.get(0).primaryTableId()
                        : "_default";

        try {
            String keyStr =
                    "/txn_commit_record/"
                            + catalogName
                            + "/"
                            + databaseName
                            + "/"
                            + txnTableName
                            + "/"
                            + txnId;
            byte[] key = keyStr.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] valueBytes =
                    String.valueOf(txnId).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            List<io.github.rawwise.metastore.Precondition> preconditions =
                    List.of(new io.github.rawwise.metastore.Precondition.NotExists(key));
            List<io.github.rawwise.metastore.Mutation> mutations =
                    List.of(new io.github.rawwise.metastore.Mutation.Put(key, valueBytes));
            metastore.compareAndSwap(preconditions, mutations);
        } catch (IOException | RuntimeException e) {
            log.warn(
                    "replayPublishStage: txnId={} failed to write TxnCommitRecord: {}",
                    txnId,
                    e.getMessage());
        }

        try {
            String latestKey =
                    "/snapshot_latest/" + catalogName + "/" + databaseName + "/" + txnTableName;
            byte[] keyBytes = latestKey.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] newValue = java.nio.ByteBuffer.allocate(Long.BYTES).putLong(txnId).array();

            Optional<byte[]> currentValueOpt = metastore.get(keyBytes);
            if (currentValueOpt.isPresent()
                    && java.nio.ByteBuffer.wrap(currentValueOpt.get()).getLong() >= txnId) {
                log.debug(
                        "replayPublishStage: latestSnapshot already advanced past {} (idempotent)",
                        txnId);
            } else {
                io.github.rawwise.metastore.Precondition precondition =
                        currentValueOpt.isPresent()
                                ? new io.github.rawwise.metastore.Precondition.ValueEquals(
                                        keyBytes, currentValueOpt.get())
                                : new io.github.rawwise.metastore.Precondition.NotExists(keyBytes);
                metastore.compareAndSwap(
                        List.of(precondition),
                        List.of(new io.github.rawwise.metastore.Mutation.Put(keyBytes, newValue)));
            }
        } catch (IOException | RuntimeException e) {
            log.warn(
                    "replayPublishStage: txnId={} failed to update latestSnapshot: {}",
                    txnId,
                    e.getMessage());
        }
    }

    /**
     * H4: Complete WAL-based crash recovery for a single txnId.
     *
     * <p>Protocol:
     *
     * <ol>
     *   <li>Read uncommitted WAL records via {@link TxnWALReader#readUncommitted()}
     *   <li>If txnId is uncommitted: check LMDB commit marker
     *       <ul>
     *         <li>marker present → DurableStage completed, advance clock (redo commit)
     *         <li>marker missing → abort: rollbackPendingInstances + cleanup + WAL ABORT
     *       </ul>
     *   <li>If txnId is not uncommitted: check WAL final state (COMMITTED/ABORT)
     *   <li>If no WAL records: fall back to marker-only check
     * </ol>
     */
    private String recoverWithWAL(long txnId) {
        // Step 1: Read uncommitted WAL records to identify transactions needing recovery
        List<Object> uncommittedRecords = walReader.readUncommitted();
        Set<Long> uncommittedTxnIds = new HashSet<>();
        for (Object record : uncommittedRecords) {
            uncommittedTxnIds.add(TxnWALReader.extractTxnId(record));
        }

        if (uncommittedTxnIds.contains(txnId)) {
            // Step 2: Transaction is uncommitted — check LMDB commit marker
            byte[] markerKey =
                    ("__commit_marker__:" + txnId)
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] markerValue = lmdbInstance.get(markerKey);

            if (markerValue != null && markerValue.length > 0 && markerValue[0] == 1) {
                clock.advanceToAtLeast(txnId);
                log.info(
                        "recoverWithWAL: txnId={} — DurableStage completed (marker present), "
                                + "replaying PublishStage",
                        txnId);

                List<TxnWALRecord.TxnWriteIntent> walIntents = new ArrayList<>();
                for (Object rec : uncommittedRecords) {
                    if (rec instanceof TxnWALRecord.TxnWriteIntent wi && wi.txnId() == txnId) {
                        walIntents.add(wi);
                    }
                }
                replayPublishStageOnRecovery(txnId, walIntents);

                walWriter.writeCommitted(new TxnWALRecord.TxnCommitted(txnId, txnId, new byte[0]));
                return "recovered-committed:" + txnId;
            } else {
                // Commit marker missing → abort: cleanup WriteBuffer + rollbackPendingInstances
                log.warn(
                        "recoverWithWAL: txnId={} — uncommitted with no commit marker, aborting",
                        txnId);
                rollbackPendingInstances(txnId);
                cleanupTxn(txnId);
                walWriter.writeAbort(
                        new TxnWALRecord.TxnAbort(
                                txnId, "crash-recovery-no-marker", System.currentTimeMillis()));
                return "recovered-aborted:" + txnId;
            }
        }

        // Step 3: Transaction is not in uncommitted set — check WAL final state
        List<Object> txnRecords = walReader.readByTxnId(txnId);
        boolean hasCommitted = false;
        boolean hasAborted = false;
        for (Object record : txnRecords) {
            if (record instanceof TxnWALRecord.TxnCommitted) {
                hasCommitted = true;
            } else if (record instanceof TxnWALRecord.TxnAbort) {
                hasAborted = true;
            }
        }

        if (hasCommitted) {
            // WAL says committed, but verify LMDB marker for durability proof.
            // In-memory WAL (e.g., InMemoryTxnWALWriter) is non-persistent;
            // only the LMDB commit marker survives a crash.
            byte[] markerKey =
                    ("__commit_marker__:" + txnId)
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] markerValue = lmdbInstance.get(markerKey);
            if (markerValue != null && markerValue.length > 0 && markerValue[0] == 1) {
                clock.advanceToAtLeast(txnId);
                return "already-committed:" + txnId;
            }
            // WAL has commit record but LMDB marker missing -> no durable state
            return "no-state-found:" + txnId;
        }
        if (hasAborted) {
            return "already-aborted:" + txnId;
        }

        // Step 4: No WAL records for this txnId — fall back to marker-only check
        return recoverWithMarkerOnly(txnId);
    }

    /** Marker-only recovery fallback (legacy path when TxnWALReader is not configured). */
    private String recoverWithMarkerOnly(long txnId) {
        byte[] markerKey =
                ("__commit_marker__:" + txnId).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] markerValue = lmdbInstance.get(markerKey);
        if (markerValue != null && markerValue.length > 0 && markerValue[0] == 1) {
            clock.advanceToAtLeast(txnId);
            return "already-committed:" + txnId;
        }
        return "no-state-found:" + txnId;
    }

    // ================================================================
    // TxnRecorder
    // ================================================================

    @Override
    public void recordRead(TxnContext ctx, String tableId, byte[] key, byte[] value) {
        if (ctx == null) return;
        ctx.readCache().put(tableId, key, value);
        log.debug("recordRead: txn={} table={} keySize={}", ctx.txnId(), tableId, key.length);
    }

    @Override
    public void recordReadRange(TxnContext ctx, String tableId, byte[] start, byte[] end) {
        if (ctx == null) return;
        RangeReadSet rangeReadSet = rangeReadSets.get(ctx.txnId());
        if (rangeReadSet != null) {
            RangeReadSet.RangeReadEntry entry =
                    new RangeReadSet.RangeReadEntry(
                            tableId,
                            start,
                            end,
                            true,
                            true,
                            "",
                            0,
                            ctx.readSnapshot() != null ? ctx.readSnapshot().snapshotTs() : 0);
            rangeReadSet.record(entry);
            log.debug(
                    "recordReadRange: txn={} table={} range=[{},{}]",
                    ctx.txnId(),
                    tableId,
                    bytesToHex(start),
                    bytesToHex(end));
        }
    }

    @Override
    public void recordWrite(
            TxnContext ctx, String tableId, byte[] key, byte[] value, long baseVersion) {
        if (ctx == null) return;
        ctx.writeBuffer().put(tableId, key, value);
        log.debug(
                "recordWrite: txn={} table={} keySize={} baseVersion={}",
                ctx.txnId(),
                tableId,
                key.length,
                baseVersion);
    }

    @Override
    public void recordUpdateLog(TxnContext ctx, String tableId, UpdateLog updateLog) {
        if (ctx == null || ctx.currentState() != TxnState.ACTIVE) {
            throw new IllegalStateException(
                    "Cannot recordUpdateLog outside active transaction. ctx=" + ctx);
        }
        if (tableId == null || tableId.isBlank()) {
            throw new IllegalArgumentException("tableId must not be blank");
        }
        if (updateLog == null) {
            throw new IllegalArgumentException("updateLog must not be null");
        }
        pendingUpdateLogPaths
                .computeIfAbsent(ctx.txnId(), k -> new ConcurrentHashMap<>())
                .computeIfAbsent(tableId, k -> new CopyOnWriteArrayList<>())
                .add(updateLog);
        log.debug(
                "recordUpdateLog: txnId={} tableId={} filePath={}",
                ctx.txnId(),
                tableId,
                updateLog.filePath());
    }

    // ================================================================
    // TxnBatchCoordinator
    // ================================================================

    @Override
    public TxnResult commitBatch(BatchCommitRequest request) {
        String batchId = request.batchId();
        String idempotencyKey = request.idempotencyKey();
        log.info(
                "commitBatch: batchId={} idempotencyKey={} tableId={} expectedShards={} keys={}",
                batchId,
                idempotencyKey,
                request.tableId(),
                request.expectedShardCount(),
                request.keys().size());

        // --- Idempotency guard (atomic putIfAbsent to prevent TOCTOU race) ---
        BatchIdempotencyRecord candidate =
                new BatchIdempotencyRecord(
                        batchId,
                        idempotencyKey,
                        request.tableId(),
                        BatchState.OPEN,
                        request.expectedShardCount(),
                        List.of(),
                        0,
                        0);
        BatchIdempotencyRecord existing = batchIndex.putIfAbsent(idempotencyKey, candidate);
        if (existing != null) {
            if (existing.state() == BatchState.COMMITTED) {
                log.info(
                        "commitBatch: idempotent replay detected, batch already COMMITTED idempotencyKey={} txnId={}",
                        idempotencyKey,
                        existing.txnId());
                return new TxnResult.CommitOk(existing.txnId());
            }
            if (existing.state() == BatchState.ABORTED) {
                log.warn(
                        "commitBatch: idempotent replay detected, batch was ABORTED idempotencyKey={}",
                        idempotencyKey);
                return new TxnResult.CommitConflict("Batch previously aborted: " + idempotencyKey);
            }
            return new TxnResult.CommitConflict(
                    "Batch with idempotencyKey="
                            + idempotencyKey
                            + " is already in progress, state="
                            + existing.state());
        }
        BatchIdempotencyRecord record = candidate;

        // --- Begin batch transaction (H1: guard against orphan record) ---
        TxnContext ctx;
        try {
            ctx = begin(Set.of(request.tableId()), IsolationLevel.READ_COMMITTED);
        } catch (RuntimeException e) {
            batchIndex.remove(idempotencyKey);
            throw e;
        }

        // Update record with actual txnId (CR1: atomic replace to prevent abortBatch race)
        BatchIdempotencyRecord prev = record;
        record =
                new BatchIdempotencyRecord(
                        batchId,
                        idempotencyKey,
                        request.tableId(),
                        BatchState.OPEN,
                        request.expectedShardCount(),
                        List.of(),
                        ctx.txnId(),
                        0);
        if (!batchIndex.replace(idempotencyKey, prev, record)) {
            abort(ctx);
            log.warn(
                    "commitBatch: concurrent abort detected after begin idempotencyKey={}",
                    idempotencyKey);
            return new TxnResult.CommitConflict(
                    "Batch was aborted concurrently: " + idempotencyKey);
        }

        // --- Record all writes ---
        for (int i = 0; i < request.keys().size(); i++) {
            byte[] value = i < request.values().size() ? request.values().get(i) : new byte[0];
            recordWrite(ctx, request.tableId(), request.keys().get(i), value, 0);
        }

        // --- Transition OPEN → SHARDS_PREPARED (CR1: atomic replace) ---
        prev = record;
        record =
                new BatchIdempotencyRecord(
                        batchId,
                        idempotencyKey,
                        request.tableId(),
                        BatchState.SHARDS_PREPARED,
                        request.expectedShardCount(),
                        record.shardInstanceIds(),
                        ctx.txnId(),
                        0);
        if (!batchIndex.replace(idempotencyKey, prev, record)) {
            abort(ctx);
            log.warn(
                    "commitBatch: concurrent abort detected during shard prepare idempotencyKey={}",
                    idempotencyKey);
            return new TxnResult.CommitConflict(
                    "Batch was aborted concurrently: " + idempotencyKey);
        }

        // --- Commit via single pipeline ---
        try {
            TxnResult result = commit(ctx);

            if (result instanceof TxnResult.CommitOk ok) {
                // commit() internally already completed DurableStage + PublishStage
                // dataalreadyglobalvisible → directlyatomicjumpto COMMITTED, skip DATA_DURABLE
                // eliminate race window with abortBatch
                long commitTs = ok.commitTs();
                prev = record;
                record =
                        new BatchIdempotencyRecord(
                                batchId,
                                idempotencyKey,
                                request.tableId(),
                                BatchState.COMMITTED,
                                request.expectedShardCount(),
                                record.shardInstanceIds(),
                                ctx.txnId(),
                                commitTs);
                if (!batchIndex.replace(idempotencyKey, prev, record)) {
                    // concurrent abortBatch in the tiny window after commit() returns CommitOk
                    // preemptively set ABORTED.data committed, must force correct index.
                    // first try replace(ABORTED, COMMITTED) as final check, fail then put() force
                    // override.
                    log.error(
                            "commitBatch: data committed but abortBatch set ABORTED, "
                                    + "force-correcting index idempotencyKey={}",
                            idempotencyKey);
                    BatchIdempotencyRecord abortedRecord =
                            new BatchIdempotencyRecord(
                                    batchId,
                                    idempotencyKey,
                                    request.tableId(),
                                    BatchState.ABORTED,
                                    request.expectedShardCount(),
                                    record.shardInstanceIds(),
                                    ctx.txnId(),
                                    0);
                    if (!batchIndex.replace(idempotencyKey, abortedRecord, record)) {
                        batchIndex.put(idempotencyKey, record);
                    }
                }

                // Write WAL TXN_BATCH record (best-effort)
                try {
                    walWriter.writeWriteIntent(
                            new TxnWALRecord.TxnWriteIntent(
                                    ctx.txnId(),
                                    Set.of(request.tableId()),
                                    request.tableId(),
                                    0,
                                    0L,
                                    IsolationLevel.READ_COMMITTED,
                                    0,
                                    ("__batch__:" + batchId)
                                            .getBytes(java.nio.charset.StandardCharsets.UTF_8),
                                    new byte[0],
                                    TxnWALRecord.WriteType.UPSERT,
                                    0,
                                    0,
                                    0));
                } catch (RuntimeException e) {
                    log.warn(
                            "commitBatch: WAL write failed, batch already committed idempotencyKey={}",
                            idempotencyKey,
                            e);
                }

                log.info(
                        "commitBatch: batchId={} idempotencyKey={} COMMITTED txnId={} commitTs={}",
                        batchId,
                        idempotencyKey,
                        ctx.txnId(),
                        commitTs);
                return result;
            }

            // Commit failed (OCC conflict etc.) — mark ABORTED (CR1: atomic replace)
            prev = record;
            record =
                    new BatchIdempotencyRecord(
                            batchId,
                            idempotencyKey,
                            request.tableId(),
                            BatchState.ABORTED,
                            request.expectedShardCount(),
                            record.shardInstanceIds(),
                            ctx.txnId(),
                            0);
            batchIndex.replace(idempotencyKey, prev, record);
            log.warn(
                    "commitBatch: batchId={} idempotencyKey={} failed: {}",
                    batchId,
                    idempotencyKey,
                    result.message());
            return result;

        } catch (RuntimeException e) {
            log.error(
                    "commitBatch: batchId={} idempotencyKey={} commit failed, aborting",
                    batchId,
                    idempotencyKey,
                    e);
            abort(ctx);

            prev = record;
            record =
                    new BatchIdempotencyRecord(
                            batchId,
                            idempotencyKey,
                            request.tableId(),
                            BatchState.ABORTED,
                            request.expectedShardCount(),
                            record.shardInstanceIds(),
                            ctx.txnId(),
                            0);
            batchIndex.replace(idempotencyKey, prev, record);

            return new TxnResult.CommitConflict("Batch commit failed: " + e.getMessage());
        }
    }

    @Override
    public TxnResult abortBatch(String idempotencyKey) {
        log.info("abortBatch: idempotencyKey={}", idempotencyKey);

        BatchIdempotencyRecord existing = batchIndex.get(idempotencyKey);
        if (existing != null) {
            if (existing.state() == BatchState.COMMITTED) {
                log.warn(
                        "abortBatch: cannot abort already COMMITTED batch idempotencyKey={}",
                        idempotencyKey);
                return new TxnResult.CommitConflict(
                        "Cannot abort committed batch: " + idempotencyKey);
            }
            if (existing.state().isAbortable()) {
                BatchIdempotencyRecord aborted =
                        new BatchIdempotencyRecord(
                                existing.batchId(),
                                idempotencyKey,
                                existing.tableId(),
                                BatchState.ABORTED,
                                existing.shardCount(),
                                existing.shardInstanceIds(),
                                existing.txnId(),
                                existing.commitTs());
                if (batchIndex.replace(idempotencyKey, existing, aborted)) {
                    log.info(
                            "abortBatch: idempotencyKey={} transitioned to ABORTED",
                            idempotencyKey);
                } else {
                    // CAS fail: state changed by concurrent commitBatch, re-read to confirm
                    BatchIdempotencyRecord current = batchIndex.get(idempotencyKey);
                    if (current != null && current.state() == BatchState.COMMITTED) {
                        log.warn(
                                "abortBatch: batch already COMMITTED by concurrent commitBatch idempotencyKey={}",
                                idempotencyKey);
                        return new TxnResult.CommitConflict(
                                "Cannot abort committed batch: " + idempotencyKey);
                    }
                    log.info(
                            "abortBatch: idempotencyKey={} state already changed by concurrent commitBatch",
                            idempotencyKey);
                }
            }
        }

        return new TxnResult.AbortResult(0, "batch aborted: " + idempotencyKey);
    }

    @Override
    public String decideBatchRecovery(String idempotencyKey) {
        log.info("decideBatchRecovery: idempotencyKey={}", idempotencyKey);

        // Check in-memory batch index first
        BatchIdempotencyRecord record = batchIndex.get(idempotencyKey);
        if (record != null) {
            if (record.state() == BatchState.COMMITTED) {
                return "already-committed";
            }
            if (record.state() == BatchState.ABORTED) {
                return "already-aborted";
            }
            return "needs-recovery:" + record.state();
        }

        // Check Metastore for batch commit record
        try {
            String keyStr =
                    "/txn_commit_record/"
                            + catalogName
                            + "/"
                            + databaseName
                            + "/batch/"
                            + idempotencyKey;
            byte[] key = keyStr.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            if (metastore.get(key).isPresent()) {
                return "already-committed";
            }
        } catch (IOException | RuntimeException e) {
            log.warn("decideBatchRecovery: Metastore read failed for {}", idempotencyKey, e);
        }
        return "needs-abort";
    }

    // ================================================================
    // TxnReader
    // ================================================================

    @Override
    public Optional<byte[]> pointRead(String tableId, byte[] key, TxnContext ctx) {
        // 3-tier read (INV-008): WriteBuffer → TxnReadCache → LMDB
        Optional<byte[]> cached = VisibilityChecker.check(ctx, tableId, key);
        if (cached.isPresent()) {
            return cached;
        }

        // Tier 3: LMDB (VersionChainStore)
        Optional<VersionChainStore.VersionEntry> version =
                pitReader.pointReadWithVersion(ctx, tableId, key);

        if (version.isPresent()) {
            byte[] value = version.get().valueCopy();
            long baseVersion = version.get().commitTs();
            // Cache for REPEATABLE_READ
            recordRead(ctx, tableId, key, value);
            // Store baseVersion for OCC conflict detection
            recordBaseVersion(ctx, tableId, key, baseVersion);
            return Optional.of(value);
        }

        return Optional.empty();
    }

    @Override
    public List<KeyValuePair> rangeRead(String tableId, byte[] start, byte[] end, TxnContext ctx) {
        if (ctx == null) {
            throw new IllegalArgumentException("ctx must not be null for rangeRead");
        }
        if (tableId == null || tableId.isBlank()) {
            throw new IllegalArgumentException("tableId must not be blank for rangeRead");
        }
        if (start == null || end == null) {
            throw new IllegalArgumentException("start and end must not be null for rangeRead");
        }

        // Step 1: record range to RangeReadSet(SERIALIZABLE isolation level for phantom detection)
        // recordReadRange internally checks ctx != null and records to rangeReadSets
        recordReadRange(ctx, tableId, start, end);

        // Step 2: determine snapshot timestamp
        // - READ_COMMITTED: readSnapshot is null, use snapshotTs=0 meaning"latest committed
        // version"
        // - REPEATABLE_READ/SERIALIZABLE: use readSnapshot.snapshotTs() as visibility boundary
        ReadSnapshot snapshot = ctx.readSnapshot();
        long snapshotTs = (snapshot != null) ? snapshot.snapshotTs() : 0L;

        log.debug(
                "rangeRead: txn={} table={} range=[{},{}] isolation={} snapshotTs={}",
                ctx.txnId(),
                tableId,
                bytesToHex(start),
                bytesToHex(end),
                ctx.isolationLevel(),
                snapshotTs);

        // Step 3: Tier 1 — scan entries in range within WriteBuffer
        // WriteBuffer is the single trusted source for this transaction's uncommitted writes
        // (INV-008)
        Map<ByteBuffer, byte[]> writeBufferResults =
                scanWriteBufferInRange(ctx, tableId, start, end);

        // Step 4: Tier 3 — scan visible versions in range within LMDB VersionChainStore
        // use LMDB cursor scan + MVCC visibility filter
        List<VersionChainStore.RangeScanEntry> lmdbResults =
                versionChainStore.scanRangeAtSnapshot(tableId, start, end, snapshotTs);

        // Step 5: merge Tier 1 (WriteBuffer) + Tier 2 (TxnReadCache) + Tier 3 (LMDB) result
        // priority (INV-008): WriteBuffer > TxnReadCache > LMDB
        // DELETED type WriteBuffer entries indicate this transaction has deleted, should not appear
        // in result
        Map<ByteBuffer, KeyValuePair> merged = new HashMap<>();

        // first put LMDB result (Tier 3)
        for (VersionChainStore.RangeScanEntry entry : lmdbResults) {
            merged.put(ByteBuffer.wrap(entry.pk()), new KeyValuePair(entry.pk(), entry.value()));
        }

        // H5: Tier 2 — TxnReadCache application.
        // TxnReadCache is point-query cache(unsupported range query API), therefore Tier 2 in
        // rangeRead path
        // applies by checking TxnReadCache for each key in LMDB result:
        // - cachecommandinand exists=true → usecachevalue(ensure REPEATABLE_READ consistency)
        // - cachecommandinand exists=false → the key insnapshotwhenmomentdoes not exist, remove
        // from result
        // - cachenotcommandin → remain LMDB value
        // note: TxnReadCache will not introduce new keys already in WriteBuffer, because
        // WriteBuffer has higher priority,
        // will override Tier 2 result in next step.
        TxnReadCache readCache = ctx.readCache();
        if (readCache != null) {
            Iterator<Map.Entry<ByteBuffer, KeyValuePair>> it = merged.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<ByteBuffer, KeyValuePair> e = it.next();
                byte[] keyBytes = e.getValue().key();
                TxnReadCache.CachedRead cached = readCache.get(tableId, keyBytes);
                if (cached != null) {
                    if (cached.exists()) {
                        // cache hit: replace LMDB value with cache value(ensure repeatable read)
                        e.setValue(new KeyValuePair(keyBytes, cached.getValue()));
                    } else {
                        // cache marked as"does not exist": remove from result
                        it.remove();
                    }
                }
            }
        }

        // override with WriteBuffer result(Tier 1 has highest priority)
        for (Map.Entry<ByteBuffer, byte[]> wbEntry : writeBufferResults.entrySet()) {
            byte[] keyBytes = wbEntry.getKey().array();
            merged.put(ByteBuffer.wrap(keyBytes), new KeyValuePair(keyBytes, wbEntry.getValue()));
        }

        // sort by unsigned bytes order, ensure rangeRead result is ordered
        List<KeyValuePair> results = new ArrayList<>(merged.values());
        results.sort((a, b) -> java.util.Arrays.compareUnsigned(a.key(), b.key()));

        log.debug(
                "rangeRead: txn={} table={} range=[{},{}] — writeBuffer={} lmdb={} merged={} results={}",
                ctx.txnId(),
                tableId,
                bytesToHex(start),
                bytesToHex(end),
                writeBufferResults.size(),
                lmdbResults.size(),
                merged.size(),
                results.size());

        return results;
    }

    /**
     * scan entries within specified range in WriteBuffer (Tier 1, INV-008).
     *
     * <p>WriteBuffer is the single trusted source for this transaction's uncommitted writes. For
     * rangeRead:
     *
     * <ul>
     *   <li>NEW_VALUE type: include in result(this transaction's new write/update)
     *   <li>DELETED type: exclude from result(this transaction has deleted)
     * </ul>
     *
     * @param ctx transaction context
     * @param tableId table ID
     * @param start start key (inclusive)
     * @param end end key (inclusive)
     * @return key → value mapping(onlyinclude NEW_VALUE typeandinrangeentry)
     */
    private Map<ByteBuffer, byte[]> scanWriteBufferInRange(
            TxnContext ctx, String tableId, byte[] start, byte[] end) {
        Map<ByteBuffer, byte[]> results = new HashMap<>();
        Map<ByteBuffer, WriteBuffer.BufferEntry> entries =
                ctx.writeBuffer().snapshotEntries(tableId);

        for (Map.Entry<ByteBuffer, WriteBuffer.BufferEntry> entry : entries.entrySet()) {
            byte[] key = entry.getKey().array();

            // unsigned bytes comparison, check whether key is in [start, end] range
            if (java.util.Arrays.compareUnsigned(key, start) < 0) continue;
            if (java.util.Arrays.compareUnsigned(key, end) > 0) continue;

            WriteBuffer.BufferEntry bufferEntry = entry.getValue();
            if (bufferEntry.type() == WriteBuffer.BufferEntryType.NEW_VALUE) {
                byte[] value = bufferEntry.getValue();
                if (value != null) {
                    results.put(ByteBuffer.wrap(key.clone()), value);
                }
            }
            // DELETED type: not included in result(this transaction has deleted)
        }

        return results;
    }

    @Override
    public Optional<byte[]> pointReadAtSnapshot(String tableId, byte[] key, long snapshotTs) {
        return pitReader.pointRead(tableId, key, snapshotTs).flatMap(v -> Optional.ofNullable(v));
    }

    // ================================================================
    // TxnStoreBridge — SPI bridge methods ()
    // ================================================================

    @Override
    public void flushPending(TxnContext ctx, String tableId) {
        if (ctx == null) return;

        var keys = ctx.pendingBatchKeys();
        if (keys.isEmpty()) return;

        throw new UnsupportedOperationException(
                "flushPending without LMDB instance suppliers is not supported. "
                        + "Use flushPendingWithInstance() for actual LMDB writes. "
                        + "tableId="
                        + tableId
                        + ", txnId="
                        + ctx.txnId()
                        + ", entries="
                        + keys.size());
    }

    @Override
    public List<String> flushPendingWithInstance(
            TxnContext ctx,
            String tableId,
            Function<String, LmdbInstance> pendingInstanceSupplier,
            Function<String, LmdbInstance> newPendingInstanceSupplier) {
        if (ctx == null || tableId == null || tableId.isBlank()) return List.of();
        if (pendingInstanceSupplier == null) {
            throw new IllegalStateException(
                    "pendingInstanceSupplier must not be null for tableId=" + tableId);
        }
        if (newPendingInstanceSupplier == null) {
            throw new IllegalStateException(
                    "newPendingInstanceSupplier must not be null for tableId=" + tableId);
        }

        var keys = ctx.pendingBatchKeys();
        var values = ctx.pendingBatchValues();
        if (keys.isEmpty()) {
            log.debug(
                    "flushPendingWithInstance: txn={} tableId={} empty pending batch, no-op",
                    ctx.txnId(),
                    tableId);
            return List.of();
        }

        log.info(
                "flushPendingWithInstance: txn={} tableId={} entries={} bytes={}",
                ctx.txnId(),
                tableId,
                keys.size(),
                estimatePendingBytes(ctx));

        // Get or create pending LMDB instance
        LmdbInstance pendingInstance = pendingInstanceSupplier.apply(tableId);
        if (pendingInstance == null) {
            throw new IllegalStateException(
                    "pendingInstanceSupplier returned null for tableId=" + tableId);
        }

        // Batch put to pending instance
        LmdbInstance.BatchPutResult result = pendingInstance.batchPut(keys, values);

        if (result.status() == LmdbInstance.PutStatus.MAP_FULL) {
            log.warn(
                    "flushPendingWithInstance: MAP_FULL on instance={}, creating new instance",
                    pendingInstance.instanceId());
            LmdbInstance newInstance = newPendingInstanceSupplier.apply(tableId);
            if (newInstance == null) {
                throw new IllegalStateException(
                        "newPendingInstanceSupplier returned null for tableId=" + tableId);
            }

            LmdbInstance.BatchPutResult retryResult = newInstance.batchPut(keys, values);
            if (!retryResult.isAllSuccess()) {
                log.error(
                        "flushPendingWithInstance: batchPut retry failed after MAP_FULL: {}",
                        retryResult.status());
                throw new RuntimeException(
                        "WriteThrough batchPut retry failed after MAP_FULL: "
                                + retryResult.status());
            }
            ctx.addPendingInstanceId(newInstance.instanceId());

            // Clear pending batch after successful write
            ctx.clearPendingBatch();
            log.info(
                    "flushPendingWithInstance: txn={} tableId={} flushed {} entries to new instance {}",
                    ctx.txnId(),
                    tableId,
                    keys.size(),
                    newInstance.instanceId());
            return List.of(newInstance.instanceId());
        } else if (!result.isAllSuccess()) {
            log.error("flushPendingWithInstance: batchPut failed: {}", result.status());
            throw new RuntimeException("WriteThrough batchPut failed: " + result.status());
        }

        // Only add instance ID after successful batchPut (H2 fix: no orphan tracking)
        ctx.addPendingInstanceId(pendingInstance.instanceId());

        // Clear pending batch after successful write
        ctx.clearPendingBatch();
        log.info(
                "flushPendingWithInstance: txn={} tableId={} flushed {} entries successfully",
                ctx.txnId(),
                tableId,
                keys.size());
        return List.of(pendingInstance.instanceId());
    }

    @Override
    public List<String> commitTailBatch(
            String tableId,
            Function<String, LmdbInstance> pendingInstanceSupplier,
            Function<String, LmdbInstance> newPendingInstanceSupplier) {
        if (tableId == null || tableId.isBlank()) return List.of();
        if (pendingInstanceSupplier == null) {
            throw new IllegalStateException(
                    "pendingInstanceSupplier must not be null for tableId=" + tableId);
        }
        if (newPendingInstanceSupplier == null) {
            throw new IllegalStateException(
                    "newPendingInstanceSupplier must not be null for tableId=" + tableId);
        }

        // Resolve TxnContext from TxnContextHolder
        TxnContext ctx = TxnContextHolder.current();
        if (ctx == null || !ctx.isActive()) {
            log.debug("commitTailBatch: no active transaction, tableId={}", tableId);
            return List.of();
        }

        var keys = ctx.pendingBatchKeys();
        if (keys.isEmpty()) {
            log.debug(
                    "commitTailBatch: txn={} tableId={} empty tail batch, no-op",
                    ctx.txnId(),
                    tableId);
            return List.of();
        }

        log.info(
                "commitTailBatch: txn={} tableId={} flushing {} residual entries",
                ctx.txnId(),
                tableId,
                keys.size());

        return flushPendingWithInstance(
                ctx, tableId, pendingInstanceSupplier, newPendingInstanceSupplier);
    }

    /**
     * Create a TxnStoreBridge instance that bridges storage/ layer calls to this orchestrator via
     * TxnContextHolder.
     *
     * @return a new TxnStoreBridgeImpl wrapping this orchestrator
     */
    public io.github.rawwise.common.spi.TxnStoreBridge createStoreBridge() {
        return new io.github.rawwise.engine.txn.bridge.TxnStoreBridgeImpl(this);
    }

    // ================================================================
    // Internal helpers
    // ================================================================

    /** implementation EXCLUSIVE row lock acquisition, forward to ShardVersionTracker. */
    @Override
    public void acquireForUpdate(TxnContext ctx, byte[] key, long timeoutMs)
            throws InterruptedException {
        if (ctx == null) throw new IllegalArgumentException("ctx must not be null");
        if (key == null) throw new IllegalArgumentException("key must not be null");
        if (tracker == null) {
            throw new UnsupportedOperationException(
                    "acquireForUpdate requires ShardVersionTracker (tracker is null)");
        }
        tracker.acquireForUpdate(key, ctx.txnId(), timeoutMs);
    }

    private void cleanupTxn(long txnId) {
        activeTxnIndex.remove(txnId);
        rangeReadSets.remove(txnId);
        writeIntentsIndex.remove(txnId);
        keyBaseVersions.remove(txnId);
        pendingUpdateLogPaths.remove(txnId);
        txnTimeoutIndex.remove(txnId);
        // fallback release all row locks held by this transaction, prevent exception path leak
        if (tracker != null) {
            try {
                tracker.releaseAll(txnId);
            } catch (Exception e) {
                log.warn("releaseAll failed for txn {}: {}", txnId, e.getMessage());
            }
        }
        // multipletabletransaction — releasealltable ShardVersionTracker inlock
        if (tableTrackerResolver != null) {
            try {
                TxnContext ctx = activeTxnIndex.get(txnId);
                if (ctx != null) {
                    for (String tableId : ctx.tableIds()) {
                        ShardVersionTracker t = tableTrackerResolver.apply(tableId);
                        if (t != null) t.releaseAll(txnId);
                    }
                }
            } catch (Exception e) {
                log.warn("Multi-table releaseAll failed for txn {}: {}", txnId, e.getMessage());
            }
        }
    }

    /**
     * TXN-LU-002: atomically commit all pending UpdateLog files to snapshot. unified NWL format:
     * write commit record to active.nwl and trigger rollover check.
     */
    private void commitUpdateLogsIfPresent(long txnId) {
        ConcurrentHashMap<String, List<UpdateLog>> tableUpdateLogs =
                pendingUpdateLogPaths.get(txnId);
        if (tableUpdateLogs == null || tableUpdateLogs.isEmpty()) {
            return;
        }
        if (lazyUpdateManager == null) {
            log.warn(
                    "commitUpdateLogs: txnId={} has {} pending UpdateLog(s) across {} table(s) "
                            + "but lazyUpdateManager is not configured",
                    txnId,
                    tableUpdateLogs.values().stream().mapToInt(List::size).sum(),
                    tableUpdateLogs.size());
            return;
        }
        List<String> failedTables = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        for (Map.Entry<String, List<UpdateLog>> entry : tableUpdateLogs.entrySet()) {
            String tableId = entry.getKey();
            List<UpdateLog> updateLogs = entry.getValue();
            if (updateLogs.isEmpty()) {
                continue;
            }
            log.info(
                    "commitUpdateLogs: txnId={} tableId={} updateLogCount={}",
                    txnId,
                    tableId,
                    updateLogs.size());
            try {
                long txnMaxSeq =
                        updateLogs.stream().mapToLong(UpdateLog::toSequence).max().orElse(0L);
                lazyUpdateManager.appendCommitRecord(tableId, txnId, txnMaxSeq);
                lazyUpdateManager.rolloverIfNecessary(tableId);
                log.info(
                        "commitUpdateLogs: txnId={} tableId={} appended commit record (maxSeq={}) in {} ms",
                        txnId,
                        tableId,
                        txnMaxSeq,
                        System.currentTimeMillis() - startTime);
            } catch (IOException | RuntimeException e) {
                log.error(
                        "commitUpdateLogs: txnId={} tableId={} failed, will continue with remaining tables",
                        txnId,
                        tableId,
                        e);
                failedTables.add(tableId);
            }
        }
        if (!failedTables.isEmpty()) {
            log.error(
                    "commitUpdateLogs: txnId={} completed with {} failed table(s): {}. "
                            + "Main data is committed and visible. "
                            + "UpdateLogs for these tables require manual recovery.",
                    txnId,
                    failedTables.size(),
                    failedTables);
            throw new RuntimeException(
                    "UpdateLog commit partially failed for "
                            + failedTables.size()
                            + " table(s) in txn "
                            + txnId
                            + ": "
                            + failedTables);
        }
    }

    /**
     * TXN-LU-002: rollback all pending UpdateLog. unified NWL format: write abort record to
     * active.nwl (do not delete file).
     */
    private void rollbackUpdateLogsIfPresent(long txnId) {
        ConcurrentHashMap<String, List<UpdateLog>> tableUpdateLogs =
                pendingUpdateLogPaths.get(txnId);
        if (tableUpdateLogs == null || tableUpdateLogs.isEmpty() || lazyUpdateManager == null) {
            return;
        }
        for (Map.Entry<String, List<UpdateLog>> entry : tableUpdateLogs.entrySet()) {
            String tableId = entry.getKey();
            try {
                lazyUpdateManager.appendAbortRecord(tableId, txnId);
                log.info(
                        "rollbackUpdateLog: txnId={} tableId={} appended abort record",
                        txnId,
                        tableId);
            } catch (Exception e) {
                log.error(
                        "rollbackUpdateLog: txnId={} tableId={} failed to append abort record",
                        txnId,
                        tableId,
                        e);
            }
        }
    }

    /** check whether WriteIntent list involves multiple tables. */
    private static boolean hasMultipleTables(List<TxnWALRecord.TxnWriteIntent> intents) {
        if (intents == null || intents.isEmpty()) return false;
        String first = intents.get(0).primaryTableId();
        for (TxnWALRecord.TxnWriteIntent intent : intents) {
            if (!java.util.Objects.equals(first, intent.primaryTableId())) return true;
        }
        return false;
    }

    /** count distinct tableId count overridden by WriteIntent list. */
    private static int countDistinctTables(List<TxnWALRecord.TxnWriteIntent> intents) {
        if (intents == null || intents.isEmpty()) return 0;
        java.util.Set<String> set = new java.util.HashSet<>();
        for (TxnWALRecord.TxnWriteIntent intent : intents) {
            set.add(intent.primaryTableId());
        }
        return set.size();
    }

    private void rollbackPendingInstances(long txnId) {
        // Clean up PENDING data for WRITE_THROUGH mode — tombstone CommitMarker in LMDB
        byte[] markerKey =
                ("__commit_marker__:" + txnId).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try {
            if (lmdbInstance != null && !lmdbInstance.readOnly()) {
                // Write empty tombstone to invalidate CommitMarker
                lmdbInstance.put(markerKey, new byte[0]);
                log.info("rollbackPendingInstances: txnId={} CommitMarker tombstoned", txnId);
            }
        } catch (RuntimeException e) {
            log.error(
                    "rollbackPendingInstances: txnId={} failed to tombstone CommitMarker",
                    txnId,
                    e);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        if (bytes == null) return "null";
        if (bytes.length > 16) {
            // Truncated hex for long keys
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 8; i++) sb.append(String.format("%02x", bytes[i]));
            sb.append("..");
            return sb.toString();
        }
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    /** Estimate pending batch bytes for logging purposes. */
    private static long estimatePendingBytes(TxnContext ctx) {
        long total = 0L;
        var keys = ctx.pendingBatchKeys();
        var values = ctx.pendingBatchValues();
        int size = Math.min(keys.size(), values.size());
        for (int i = 0; i < size; i++) {
            total += keys.get(i).length + values.get(i).length;
        }
        return total;
    }

    // ================================================================
    // Accessors for testing
    // ================================================================

    public LogicalClock clock() {
        return clock;
    }

    Metastore metastore() {
        return metastore;
    }

    LmdbInstance lmdbInstance() {
        return lmdbInstance;
    }

    VersionChainStore versionChainStore() {
        return versionChainStore;
    }

    ValidateStage validateStage() {
        return validateStage;
    }

    int activeTxnCount() {
        return activeTxnIndex.size();
    }

    long txnTimeoutMs() {
        return txnTimeoutMs;
    }

    public java.util.Optional<TxnContext> currentActiveTxn() {
        if (activeTxnIndex.size() == 1) {
            return activeTxnIndex.values().stream().findFirst();
        }
        return java.util.Optional.empty();
    }

    // ================================================================
    // OCC baseVersion tracking
    // ================================================================

    /**
     * baseVersion of record key (commitTs captured from LMDB point read). for OCC conflict
     * detection: WriteStage carries baseVersion when generating TxnWriteIntent, ValidateStage
     * checks if other transactions committed updated version during this period.
     */
    private void recordBaseVersion(TxnContext ctx, String tableId, byte[] key, long baseVersion) {
        ConcurrentHashMap<String, ConcurrentHashMap<ByteBuffer, Long>> txnBaseVersions =
                keyBaseVersions.get(ctx.txnId());
        if (txnBaseVersions != null) {
            ConcurrentHashMap<ByteBuffer, Long> tableVersions =
                    txnBaseVersions.computeIfAbsent(tableId, k -> new ConcurrentHashMap<>());
            tableVersions.put(ByteBuffer.wrap(key.clone()), baseVersion);
        }
    }
}
