package io.github.rawwise.backend.server.txn;

import io.github.rawwise.engine.txn.ingest.CommitLedger;
import io.github.rawwise.engine.txn.ingest.CommitLedgerStore;
import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * Crash recovery executor - on BE Server startup, scans incomplete CommitLedgers, calls {@link
 * DefaultServerTxnCoordinator#recoverBatch(String)} to complete each incomplete batch.
 *
 * <p>Called by: BE Server's {@code Main.start()}, within {@code DefaultServerTxnCoordinator} after
 * initialization, before gRPC server startup.
 *
 * <p>Recovery strategy (delegated to {@link DefaultServerTxnCoordinator#recoverBatch}):
 *
 * <ul>
 *   <li>OPEN / SHARDS_PREPARED -> abort (data not globally durable, safe rollback)
 *   <li>DATA_DURABLE / GLOBAL_MARKER_COMMITTED / PUBLISHING -> continue-commit (data already
 *       persisted, complete commit)
 *   <li>COMMITTED / ACKED / ABORTED -> skip (already finished)
 * </ul>
 */
@Slf4j
public class BatchRecoveryExecutor {

    private final DefaultServerTxnCoordinator coordinator;
    private final CommitLedgerStore commitLedgerStore;

    public BatchRecoveryExecutor(
            DefaultServerTxnCoordinator coordinator, CommitLedgerStore commitLedgerStore) {
        this.coordinator = coordinator;
        this.commitLedgerStore = commitLedgerStore;
    }

    /**
     * Scan all incomplete CommitLedgers and execute recovery.
     *
     * @return recovery result summary (success count, failure count, skip count)
     */
    public RecoverySummary recoverAll() {
        if (commitLedgerStore == null) {
            log.info(
                    "BatchRecoveryExecutor: CommitLedgerStore not configured, skipping crash recovery");
            return new RecoverySummary(0, 0, 0);
        }

        List<CommitLedger> incompleteBatches;
        try {
            incompleteBatches = commitLedgerStore.listIncomplete();
        } catch (IOException e) {
            log.error(
                    "BatchRecoveryExecutor: failed to list incomplete batches: {}",
                    e.getMessage(),
                    e);
            return new RecoverySummary(0, 0, 0);
        }

        if (incompleteBatches.isEmpty()) {
            log.info("BatchRecoveryExecutor: no incomplete batches found, crash recovery skipped");
            return new RecoverySummary(0, 0, 0);
        }

        log.info(
                "BatchRecoveryExecutor: found {} incomplete batches, starting recovery",
                incompleteBatches.size());

        int recovered = 0;
        int failed = 0;
        int skipped = 0;

        for (CommitLedger ledger : incompleteBatches) {
            String idempotencyKey = ledger.idempotencyKey();
            try {
                RecoveryDecision decision = coordinator.recoverBatch(idempotencyKey);
                switch (decision.type()) {
                    case ALREADY_COMMITTED, ALREADY_ABORTED -> {
                        recovered++;
                        log.info(
                                "BatchRecoveryExecutor: recovered idempotencyKey={} → {} ({})",
                                idempotencyKey,
                                decision.type(),
                                decision.detail());
                    }
                    case ACTIVE_BATCH -> {
                        skipped++;
                        log.info(
                                "BatchRecoveryExecutor: skipped idempotencyKey={} (active in memory)",
                                idempotencyKey);
                    }
                    default -> {
                        failed++;
                        log.warn(
                                "BatchRecoveryExecutor: failed to recover idempotencyKey={} → {} ({})",
                                idempotencyKey,
                                decision.type(),
                                decision.detail());
                    }
                }
            } catch (Exception e) {
                failed++;
                log.error(
                        "BatchRecoveryExecutor: exception recovering idempotencyKey={}: {}",
                        idempotencyKey,
                        e.getMessage(),
                        e);
            }
        }

        log.info(
                "BatchRecoveryExecutor: recovery complete — recovered={}, failed={}, skipped={}",
                recovered,
                failed,
                skipped);
        return new RecoverySummary(recovered, failed, skipped);
    }

    public record RecoverySummary(int recovered, int failed, int skipped) {
        public int total() {
            return recovered + failed + skipped;
        }
    }
}
