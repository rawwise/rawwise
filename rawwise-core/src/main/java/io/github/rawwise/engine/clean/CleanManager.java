package io.github.rawwise.engine.clean;

import io.github.rawwise.common.PlanState;
import io.github.rawwise.engine.clean.scanner.OrphanFileScanner;
import io.github.rawwise.engine.clean.strategy.CleanStrategy;
import io.github.rawwise.engine.clean.strategy.OrphanFileStrategy;
import io.github.rawwise.engine.clean.strategy.SnapshotExpiryStrategy;
import io.github.rawwise.engine.clean.strategy.TTLExpiryStrategy;
import io.github.rawwise.engine.compaction.cleanup.FileGarbageCollector;
import io.github.rawwise.engine.compaction.lock.TableLockManager;
import io.github.rawwise.engine.compaction.rewriter.CleanDataFileRewriter;
import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.LogFile;
import io.github.rawwise.engine.storage.manifest.SnapshotCommitConflictException;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.model.record.Snapshot;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CleanManager {

    private static final long RETRY_BACKOFF_BASE_MS = 100L;
    private static final long RETRY_BACKOFF_MAX_MS = 5000L;

    private final SnapshotManager snapshotManager;
    private final FileGarbageCollector garbageCollector;
    private final CleanDataFileRewriter dataFileRewriter;
    private final OrphanFileScanner orphanFileScanner;
    private final int maxRetries;
    private final CleanMetrics metrics;
    private final CleanSafetyPolicy safetyPolicy;

    public CleanManager(
            SnapshotManager snapshotManager,
            FileGarbageCollector garbageCollector,
            CleanDataFileRewriter dataFileRewriter,
            OrphanFileScanner orphanFileScanner,
            int maxRetries,
            CleanMetrics metrics,
            CleanSafetyPolicy safetyPolicy) {
        this.snapshotManager =
                Objects.requireNonNull(snapshotManager, "snapshotManager must not be null");
        this.garbageCollector =
                Objects.requireNonNull(garbageCollector, "garbageCollector must not be null");
        this.dataFileRewriter = dataFileRewriter;
        this.orphanFileScanner =
                Objects.requireNonNull(orphanFileScanner, "orphanFileScanner must not be null");
        if (maxRetries < 1) {
            throw new IllegalArgumentException("maxRetries must be at least 1");
        }
        this.maxRetries = maxRetries;
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
        this.safetyPolicy = Objects.requireNonNull(safetyPolicy, "safetyPolicy must not be null");
    }

    public CleanResult cleanOrphanFiles(String tableId, OrphanFileStrategy strategy)
            throws IOException {
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(strategy, "strategy must not be null");
        ReentrantLock lock = TableLockManager.getInstance().getLock(tableId);
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while acquiring table lock for " + tableId, e);
        }
        try {
            return doCleanOrphanFiles(tableId, strategy);
        } finally {
            lock.unlock();
        }
    }

    public CleanResult cleanTTLExpired(String tableId, TTLExpiryStrategy strategy)
            throws IOException {
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(strategy, "strategy must not be null");
        ReentrantLock lock = TableLockManager.getInstance().getLock(tableId);
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while acquiring table lock for " + tableId, e);
        }
        try {
            return doCleanTTLExpired(tableId, strategy);
        } finally {
            lock.unlock();
        }
    }

    public CleanResult cleanSnapshots(String tableId, SnapshotExpiryStrategy strategy)
            throws IOException {
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(strategy, "strategy must not be null");
        ReentrantLock lock = TableLockManager.getInstance().getLock(tableId);
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while acquiring table lock for " + tableId, e);
        }
        try {
            return doCleanSnapshots(tableId, strategy);
        } finally {
            lock.unlock();
        }
    }

    private CleanResult doCleanOrphanFiles(String tableId, OrphanFileStrategy strategy)
            throws IOException {
        long currentTimestampMs = System.currentTimeMillis();
        List<StorageFileEntry> orphanCandidates =
                orphanFileScanner.scanOrphanFiles(
                        tableId, strategy.minOrphanAgeMs(), currentTimestampMs);
        if (orphanCandidates.isEmpty()) return CleanResult.empty();

        metrics.recordOrphanScan(orphanCandidates.size());

        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                Optional<Snapshot> snapshotOpt = snapshotManager.latestSnapshot(tableId);
                if (snapshotOpt.isEmpty()) return CleanResult.empty();
                long snapshotId = snapshotOpt.get().snapshotId();

                List<DataFile> manifestDataFiles =
                        snapshotManager.listDataFiles(tableId, snapshotId);
                List<LogFile> manifestLogFiles = snapshotManager.listLogFiles(tableId, snapshotId);

                CleanContext context =
                        CleanContext.forOrphan(
                                tableId,
                                orphanCandidates,
                                manifestDataFiles,
                                manifestLogFiles,
                                currentTimestampMs);
                List<CleanAction> actions = strategy.plan(context);
                if (actions.isEmpty()) return CleanResult.empty();

                CleanPlan plan =
                        new CleanPlan(
                                UUID.randomUUID().toString(),
                                tableId,
                                snapshotId,
                                Set.of(),
                                CleanStrategy.StrategyType.ORPHAN_FILE,
                                actions,
                                computeEstimatedDeleteBytes(actions),
                                System.currentTimeMillis(),
                                PlanState.PENDING);

                return executeCleanPlan(plan);
            } catch (CleanRetryableException e) {
                log.warn(
                        "Clean OCC conflict on attempt {}/{} for orphan clean",
                        attempt + 1,
                        maxRetries,
                        e);
                if (attempt == maxRetries - 1) {
                    throw e;
                }
                applyRetryBackoff(attempt);
            }
        }
        throw new CleanFatalException("Orphan clean failed: unreachable");
    }

    private CleanResult doCleanTTLExpired(String tableId, TTLExpiryStrategy strategy)
            throws IOException {
        boolean scanMetricsRecorded = false;
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                Optional<Snapshot> snapshotOpt = snapshotManager.latestSnapshot(tableId);
                if (snapshotOpt.isEmpty()) return CleanResult.empty();
                long snapshotId = snapshotOpt.get().snapshotId();
                List<DataFile> dataFiles = snapshotManager.listDataFiles(tableId, snapshotId);

                if (!scanMetricsRecorded) {
                    metrics.recordTTLScan(dataFiles.size());
                    scanMetricsRecorded = true;
                }

                long nowMs = System.currentTimeMillis();
                CleanContext context = CleanContext.forTTL(tableId, dataFiles, nowMs);
                List<CleanAction> actions = strategy.plan(context);
                if (actions.isEmpty()) return CleanResult.empty();

                CleanPlan plan =
                        new CleanPlan(
                                UUID.randomUUID().toString(),
                                tableId,
                                snapshotId,
                                Set.of(),
                                CleanStrategy.StrategyType.TTL_EXPIRY,
                                actions,
                                computeEstimatedDeleteBytes(actions),
                                System.currentTimeMillis(),
                                PlanState.PENDING);

                return executeCleanPlan(plan);
            } catch (CleanRetryableException e) {
                log.warn(
                        "Clean OCC conflict on attempt {}/{} for TTL clean",
                        attempt + 1,
                        maxRetries,
                        e);
                if (attempt == maxRetries - 1) {
                    throw e;
                }
                applyRetryBackoff(attempt);
            }
        }
        throw new CleanFatalException("TTL clean failed: unreachable");
    }

    private CleanResult doCleanSnapshots(String tableId, SnapshotExpiryStrategy strategy)
            throws IOException {
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                List<Snapshot> snapshots = snapshotManager.getSnapshots(tableId);
                long currentTimestampMs = System.currentTimeMillis();
                CleanContext context =
                        CleanContext.forSnapshot(tableId, snapshots, currentTimestampMs);
                List<CleanAction> actions = strategy.plan(context);
                if (actions.isEmpty()) return CleanResult.empty();

                long auditSnapshotId =
                        snapshots.stream().mapToLong(Snapshot::snapshotId).max().orElse(-1);

                Set<Long> expiredIds =
                        actions.stream()
                                .filter(ExpireSnapshotAction.class::isInstance)
                                .map(ExpireSnapshotAction.class::cast)
                                .map(ExpireSnapshotAction::snapshotId)
                                .collect(Collectors.toSet());

                Set<Long> retainedIds =
                        snapshots.stream()
                                .map(Snapshot::snapshotId)
                                .filter(sid -> !expiredIds.contains(sid))
                                .collect(Collectors.toSet());

                CleanPlan plan =
                        new CleanPlan(
                                UUID.randomUUID().toString(),
                                tableId,
                                auditSnapshotId,
                                retainedIds,
                                CleanStrategy.StrategyType.SNAPSHOT_EXPIRY,
                                actions,
                                0L,
                                System.currentTimeMillis(),
                                PlanState.PENDING);

                return executeCleanPlan(plan);
            } catch (CleanRetryableException e) {
                log.warn(
                        "Clean OCC conflict on attempt {}/{} for snapshot clean",
                        attempt + 1,
                        maxRetries,
                        e);
                if (attempt == maxRetries - 1) {
                    throw e;
                }
                applyRetryBackoff(attempt);
            }
        }
        throw new CleanFatalException("Snapshot clean failed: unreachable");
    }

    private CleanResult executeCleanPlan(CleanPlan plan) throws IOException {
        long startTime = System.currentTimeMillis();

        long reclaimedBytes = 0;
        long deletedBytes = 0;
        int deletedCount = 0;
        int rewrittenCount = 0;
        long rewrittenBytes = 0;
        int expiredSnapshotCount = 0;
        int failedCount = 0;

        CleanSafetyPolicy.SafetyCheck check = safetyPolicy.evaluate(plan, snapshotManager);
        if (check.verdict() == CleanSafetyPolicy.Verdict.BLOCK) {
            log.warn(
                    "Clean blocked by safety policy for table={}: {}",
                    plan.tableId(),
                    check.reason());
            return CleanResult.empty();
        }
        if (check.verdict() == CleanSafetyPolicy.Verdict.REPLAN_REQUIRED) {
            log.info("Clean requires replan for table={}: {}", plan.tableId(), check.reason());
            throw new CleanRetryableException(check.reason());
        }

        try {
            List<String> deletablePaths = plan.collectDeletableFilePaths();
            List<String> rewritePaths = plan.collectRewriteFilePaths();
            List<Long> expiredSnapshotIds = plan.collectExpiredSnapshotIds();

            if (!deletablePaths.isEmpty()) {
                snapshotManager.commitCleanSnapshot(plan.tableId(), deletablePaths);
                garbageCollector.scheduleGc(plan.tableId(), deletablePaths, plan.auditSnapshotId());
                deletedCount = deletablePaths.size();
                deletedBytes = computeEstimatedDeleteBytes(plan.actions());
                reclaimedBytes += deletedBytes;
                recordDeleteMetrics(plan.strategyType(), deletedCount);
                metrics.addBytesScheduledForDeletion(deletedBytes);
            }

            if (!rewritePaths.isEmpty() && dataFileRewriter != null) {
                RewriteFileAction firstRwa =
                        plan.actions().stream()
                                .filter(RewriteFileAction.class::isInstance)
                                .map(RewriteFileAction.class::cast)
                                .findFirst()
                                .orElse(null);
                if (firstRwa != null) {
                    try {
                        CleanDataFileRewriter.RewriteResult result =
                                dataFileRewriter.rewriteWithTTLFilter(
                                        plan.tableId(),
                                        firstRwa.expireColumn(),
                                        firstRwa.logicalType(),
                                        firstRwa.expiryThresholdMs(),
                                        rewritePaths);
                        rewrittenCount += result.rewrittenFileCount();
                        rewrittenBytes += result.rewrittenBytes();
                        metrics.recordTTLRewrite(result.rewrittenFileCount());
                    } catch (UnsupportedOperationException e) {
                        log.warn(
                                "REWRITE_FILE batch not supported for table={}, skipping",
                                plan.tableId(),
                                e);
                    } catch (Exception e) {
                        log.warn(
                                "REWRITE_FILE batch failed for {} paths, table={}",
                                rewritePaths.size(),
                                plan.tableId(),
                                e);
                        failedCount += rewritePaths.size();
                    }
                }
            }

            if (!expiredSnapshotIds.isEmpty()) {
                Set<Long> retainedIds = plan.retainedSnapshotIds();
                for (Long sid : expiredSnapshotIds) {
                    try {
                        SnapshotManager.SnapshotExpiration result =
                                snapshotManager.expireSnapshot(plan.tableId(), sid, retainedIds);
                        expiredSnapshotCount++;
                        reclaimedBytes += result.orphanFileBytes();
                        metrics.recordSnapshotExpire(1);
                        metrics.addBytesScheduledForDeletion(result.orphanFileBytes());
                    } catch (Exception e) {
                        log.warn(
                                "EXPIRE_SNAPSHOT failed for snapshot {}, table={}",
                                sid,
                                plan.tableId(),
                                e);
                        failedCount++;
                    }
                }
            }

            long durationMs = System.currentTimeMillis() - startTime;
            metrics.setDurationMs(durationMs);

            return new CleanResult(
                    deletedCount,
                    deletedBytes,
                    rewrittenCount,
                    rewrittenBytes,
                    expiredSnapshotCount,
                    reclaimedBytes,
                    deletedBytes,
                    0L,
                    failedCount,
                    durationMs);

        } catch (SnapshotCommitConflictException e) {
            metrics.recordCleanCommitConflict();
            throw new CleanRetryableException("Clean OCC conflict: " + e.getMessage(), e);
        } catch (CleanRetryableException | CleanFatalException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new CleanFatalException("Clean failed: " + e.getMessage(), e);
        }
    }

    private long computeEstimatedDeleteBytes(List<CleanAction> actions) {
        return actions.stream()
                .filter(DeleteFileAction.class::isInstance)
                .mapToLong(a -> ((DeleteFileAction) a).estimatedExpiredBytes())
                .sum();
    }

    private void recordDeleteMetrics(CleanStrategy.StrategyType strategyType, int deletedCount) {
        switch (strategyType) {
            case ORPHAN_FILE -> metrics.recordOrphanDelete(deletedCount);
            case TTL_EXPIRY -> metrics.recordTTLDelete(deletedCount);
            case SNAPSHOT_EXPIRY -> {
                log.warn(
                        "SNAPSHOT_EXPIRY strategy produced DeleteFileAction, recording as orphan delete");
                metrics.recordOrphanDelete(deletedCount);
            }
            default -> {
                log.warn(
                        "Unknown strategyType {} for delete metrics, recording as orphan delete",
                        strategyType);
                metrics.recordOrphanDelete(deletedCount);
            }
        }
    }

    private void applyRetryBackoff(int attempt) throws CleanFatalException {
        long backoffMs = Math.min(RETRY_BACKOFF_BASE_MS * (1L << attempt), RETRY_BACKOFF_MAX_MS);
        try {
            Thread.sleep(backoffMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CleanFatalException("Clean retry interrupted during backoff", e);
        }
    }
}
