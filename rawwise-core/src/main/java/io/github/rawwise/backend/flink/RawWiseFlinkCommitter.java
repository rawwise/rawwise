package io.github.rawwise.backend.flink;

import io.github.rawwise.engine.storage.ingestion.IngestionDataSink;
import io.github.rawwise.engine.storage.ingestion.ParquetIngestionDataSink;
import io.github.rawwise.engine.storage.ingestion.SinkFatalException;
import io.github.rawwise.engine.storage.ingestion.SinkRetryableException;
import io.github.rawwise.engine.txn.ingest.BatchState;
import io.github.rawwise.engine.txn.ingest.CommitLedger;
import io.github.rawwise.engine.txn.ingest.CommitLedgerStore;
import io.github.rawwise.engine.txn.ingest.LeaseInfo;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Committer;

/**
 * RawWise Flink Committer — implement Flink 2PC commit idempotency guarantee.
 *
 * <h2>core design</h2>
 *
 * <h3>1. deterministic idempotency key</h3>
 *
 * <pre>
 * idempotencyKey = flink-{appId}-{checkpointId}-{attemptId}
 * </pre>
 *
 * same checkpoint duplicate commit(crash recovery / task retry)producesameidempotentkey, via {@link
 * CommitLedgerStore#load} detectcommitted stateand return idempotently.
 *
 * <h3>2. CommitLedger integration</h3>
 *
 * <p>state machine transition:
 *
 * <pre>
 * prepare() phase:
 * CommitLedger.open(OPEN) → save (CAS NotExists)
 *
 * commit() phase:
 * OPEN → SHARDS_PREPARED → DATA_DURABLE
 * → [sink.commit() / commitByScanning() — idempotent]
 * → GLOBAL_MARKER_COMMITTED → COMMITTED
 * </pre>
 *
 * <h3>3. crash recovery</h3>
 *
 * <ul>
 *   <li>COMMITTED / ACKED → idempotent return(signalAlreadyCommitted)
 *   <li>ABORTED → signal failure(signalFailedWithKnownReason)
 *   <li>OPEN / SHARDS_PREPARED / DATA_DURABLE → continue commit(sink.commit idempotent)
 *   <li>GLOBAL_MARKER_COMMITTED / PUBLISHING → directly finalize to COMMITTED
 * </ul>
 *
 * <h3>4. double commit prevention (Double Deduction Fix)</h3>
 *
 * <ul>
 *   <li>local {@link #committedCheckpointIds} preventsame Committer instanceinsideduplicate commit
 *   <li>CommitLedger idempotencyKey including including checkpointId, preventcross-instance
 *       duplicate commit
 *   <li>checkpointN and checkpointN+1 producedifferentidempotentkey, no mutual conflict
 * </ul>
 *
 * <h3>5. try-finally prepare</h3>
 *
 * <p>prepare phase(create + save CommitLedger)wrapped in try-finally in, auto on failure abort(if
 * state allows).
 *
 * <h2>backward compatibility</h2>
 *
 * <p>when {@code commitLedgerStore} or {@code writerIdentity} is null when, fallbacktoold direct
 * commit behavior(no CommitLedger integration).
 */
@Slf4j
public class RawWiseFlinkCommitter implements Committer<RawWiseCommittable> {

    private static final long LEASE_DURATION_MS = 300_000L;
    private static final int EXPECTED_SHARD_COUNT = 1;

    private final IngestionDataSink sink;
    private final WriterIdentity writerIdentity;
    private final CommitLedgerStore commitLedgerStore;

    private final Set<Long> committedCheckpointIds = new HashSet<>();

    public RawWiseFlinkCommitter(IngestionDataSink sink) {
        this(sink, null, null);
    }

    public RawWiseFlinkCommitter(
            IngestionDataSink sink,
            WriterIdentity writerIdentity,
            CommitLedgerStore commitLedgerStore) {
        this.sink = sink;
        this.writerIdentity = writerIdentity;
        this.commitLedgerStore = commitLedgerStore;
    }

    @Override
    public void commit(Collection<CommitRequest<RawWiseCommittable>> requests)
            throws InterruptedException {
        for (CommitRequest<RawWiseCommittable> request : requests) {
            RawWiseCommittable c = request.getCommittable();
            if (c.isEmpty()) {
                request.signalAlreadyCommitted();
                continue;
            }

            if (isLedgerEnabled()) {
                commitWithLedger(request, c);
            } else {
                commitDirect(request, c);
            }
        }
    }

    /**
     * Best-effort rollback of pending data.
     *
     * <p>When CommitLedger integration is enabled, also updates the CommitLedger state to ABORTED,
     * ensuring that crash recovery can identify aborted batches.
     *
     * <p>Note: This method is not directly called by Flink 2PC lifecycle (Flink silently discards
     * committables on checkpoint failure). Production cleanup relies on {@link
     * ParquetIngestionDataSink#recoverPending(String)} with orphanAgeThresholdMs. This method is
     * used for manual recovery and testing.
     */
    public void abort(Collection<CommitRequest<RawWiseCommittable>> requests) {
        for (CommitRequest<RawWiseCommittable> request : requests) {
            RawWiseCommittable c = request.getCommittable();
            if (c.isEmpty()) continue;
            try {
                sink.rollback(c.tableId(), c.pendingIds(), c.txnId());
            } catch (SinkFatalException e) {
                log.error(
                        "Rollback fatal error for tableId={}, txnId={}", c.tableId(), c.txnId(), e);
            } catch (Exception e) {
                log.error("Rollback failed for tableId={}, txnId={}", c.tableId(), c.txnId(), e);
            }

            if (isLedgerEnabled()) {
                abortLedger(c);
            }
        }
    }

    @Override
    public void close() throws Exception {
        sink.close();
    }

    // ===== Internal methods =====

    private boolean isLedgerEnabled() {
        return writerIdentity != null && commitLedgerStore != null;
    }

    /** Direct commit path (no CommitLedger integration, backward compatible). */
    private void commitDirect(CommitRequest<RawWiseCommittable> request, RawWiseCommittable c)
            throws InterruptedException {
        try {
            doSinkCommit(c);
            request.signalAlreadyCommitted();
        } catch (SinkRetryableException e) {
            log.warn("Retryable commit error for tableId={}, txnId={}", c.tableId(), c.txnId(), e);
            request.retryLater();
        } catch (SinkFatalException e) {
            request.signalFailedWithKnownReason(e);
        }
    }

    /**
     * with CommitLedger idempotent commitpath.
     *
     * <p>flow:
     *
     * <ol>
     *   <li>generate deterministic idempotency key
     *   <li>local dedup check(committedCheckpointIds)
     *   <li>load CommitLedger → state decision
     *   <li>if not exists: prepare phase(create OPEN + save, try-finally)
     *   <li>commit phase: state transition + sink.commit()
     * </ol>
     */
    private void commitWithLedger(CommitRequest<RawWiseCommittable> request, RawWiseCommittable c)
            throws InterruptedException {
        long checkpointId = c.checkpointId();
        String idempotencyKey = writerIdentity.idempotencyKey(checkpointId);
        String batchId = writerIdentity.batchId(checkpointId);

        if (committedCheckpointIds.contains(checkpointId)) {
            log.info(
                    "Commit idempotent (local): checkpointId={} already committed in this committer instance",
                    checkpointId);
            request.signalAlreadyCommitted();
            return;
        }

        Optional<CommitLedger> existingLedger;
        try {
            existingLedger = commitLedgerStore.load(idempotencyKey);
        } catch (IOException e) {
            log.warn(
                    "Failed to load CommitLedger for idempotencyKey={}, falling back to direct commit: {}",
                    idempotencyKey,
                    e.getMessage());
            commitDirect(request, c);
            return;
        }

        if (existingLedger.isPresent()) {
            CommitLedger ledger = existingLedger.get();
            BatchState state = ledger.state();
            log.info("CommitLedger exists: idempotencyKey={} state={}", idempotencyKey, state);

            if (state == BatchState.COMMITTED || state == BatchState.ACKED) {
                log.info(
                        "Commit idempotent (ledger): checkpointId={} already committed",
                        checkpointId);
                committedCheckpointIds.add(checkpointId);
                request.signalAlreadyCommitted();
                return;
            }

            if (state == BatchState.ABORTED) {
                log.warn("Commit rejected: checkpointId={} ledger is ABORTED", checkpointId);
                request.signalFailedWithKnownReason(
                        new SinkFatalException("Batch already aborted: " + idempotencyKey));
                return;
            }

            if (state == BatchState.FAILED_NEEDS_RECOVERY) {
                log.warn(
                        "Commit rejected: checkpointId={} ledger is FAILED_NEEDS_RECOVERY",
                        checkpointId);
                request.signalFailedWithKnownReason(
                        new SinkFatalException("Batch needs manual recovery: " + idempotencyKey));
                return;
            }
        }

        CommitLedger ledger = null;
        boolean committed = false;
        try {
            ledger = prepareCommitLedger(idempotencyKey, batchId, c, existingLedger.orElse(null));
            if (ledger == null) {
                log.warn(
                        "Failed to prepare CommitLedger, falling back to direct commit: idempotencyKey={}",
                        idempotencyKey);
                commitDirect(request, c);
                return;
            }

            ledger = executeCommitPhases(ledger, c);
            committed = true;
            committedCheckpointIds.add(checkpointId);
            log.info(
                    "Commit succeeded: idempotencyKey={} checkpointId={} finalState={}",
                    idempotencyKey,
                    checkpointId,
                    ledger.state());
            request.signalAlreadyCommitted();
        } catch (SinkRetryableException e) {
            log.warn(
                    "Retryable commit error for idempotencyKey={}: {}",
                    idempotencyKey,
                    e.getMessage());
            request.retryLater();
        } catch (SinkFatalException e) {
            log.error("Fatal commit error for idempotencyKey={}", idempotencyKey, e);
            request.signalFailedWithKnownReason(e);
        } catch (Exception e) {
            log.error("Unexpected commit error for idempotencyKey={}", idempotencyKey, e);
            request.signalFailedWithKnownReason(
                    new SinkFatalException("Unexpected commit error: " + e.getMessage(), e));
        } finally {
            if (!committed && ledger != null) {
                abortLedgerIfAbortable(idempotencyKey, ledger.state());
            }
        }
    }

    /**
     * Prepare phase: create or reuse CommitLedger (OPEN state).
     *
     * <p>try-finally guarantee: if save fails or subsequent steps throw, the created ledger will be
     * aborted.
     *
     * @param idempotencyKey idempotency key
     * @param batchId batch ID
     * @param c committable
     * @param existing existing ledger (may be null)
     * @return prepared CommitLedger, or null if cannot prepare
     */
    private CommitLedger prepareCommitLedger(
            String idempotencyKey, String batchId, RawWiseCommittable c, CommitLedger existing) {
        if (existing != null) {
            return existing;
        }

        try {
            LeaseInfo lease =
                    new LeaseInfo(
                            writerIdentity.appId() + "-" + writerIdentity.attemptId(),
                            System.currentTimeMillis() + LEASE_DURATION_MS,
                            1L);
            CommitLedger ledger =
                    CommitLedger.open(
                            idempotencyKey, batchId, c.tableId(), EXPECTED_SHARD_COUNT, 0L, lease);

            boolean saved = commitLedgerStore.save(ledger);
            if (!saved) {
                Optional<CommitLedger> race = commitLedgerStore.load(idempotencyKey);
                if (race.isPresent()) {
                    BatchState raceState = race.get().state();
                    log.info(
                            "CommitLedger CAS race: idempotencyKey={} state={}",
                            idempotencyKey,
                            raceState);
                    if (raceState == BatchState.COMMITTED || raceState == BatchState.ACKED) {
                        return race.get();
                    }
                    return race.get();
                }
                log.warn(
                        "CommitLedger save failed and load returned empty: idempotencyKey={}",
                        idempotencyKey);
                return null;
            }
            log.info(
                    "CommitLedger prepared: idempotencyKey={} batchId={} state=OPEN",
                    idempotencyKey,
                    batchId);
            return ledger;
        } catch (IOException e) {
            log.error("Failed to prepare CommitLedger: idempotencyKey={}", idempotencyKey, e);
            return null;
        }
    }

    /**
     * Commit phase: executestate transition + sink.commit().
     *
     * <p>state transition path:
     *
     * <pre>
     * [current] → SHARDS_PREPARED → DATA_DURABLE
     * → [sink.commit() — idempotent]
     * → GLOBAL_MARKER_COMMITTED → COMMITTED
     * </pre>
     *
     * <p>recoversemantics: if ledger alreadyis inintermediate state(e.g. DATA_DURABLE),
     * fromthisstatecontinue executing remaining steps. sink.commit() via commitByScanning
     * implementidempotent.
     *
     * @param ledger current CommitLedger
     * @param c committable
     * @return final COMMITTED state CommitLedger
     */
    private CommitLedger executeCommitPhases(CommitLedger ledger, RawWiseCommittable c)
            throws SinkRetryableException, SinkFatalException, IOException {
        String idempotencyKey = ledger.idempotencyKey();
        BatchState state = ledger.state();

        if (state == BatchState.OPEN) {
            ledger = commitLedgerStore.updateState(idempotencyKey, BatchState.SHARDS_PREPARED);
            state = ledger.state();
            log.debug(
                    "CommitLedger transitioned to SHARDS_PREPARED: idempotencyKey={}",
                    idempotencyKey);
        }

        if (state == BatchState.SHARDS_PREPARED) {
            ledger = commitLedgerStore.updateState(idempotencyKey, BatchState.DATA_DURABLE);
            state = ledger.state();
            log.debug(
                    "CommitLedger transitioned to DATA_DURABLE: idempotencyKey={}", idempotencyKey);
        }

        if (state == BatchState.DATA_DURABLE) {
            doSinkCommit(c);
            ledger =
                    commitLedgerStore.updateState(
                            idempotencyKey, BatchState.GLOBAL_MARKER_COMMITTED);
            state = ledger.state();
            log.debug(
                    "CommitLedger transitioned to GLOBAL_MARKER_COMMITTED: idempotencyKey={}",
                    idempotencyKey);
        }

        if (state == BatchState.GLOBAL_MARKER_COMMITTED || state == BatchState.PUBLISHING) {
            ledger = commitLedgerStore.updateState(idempotencyKey, BatchState.COMMITTED);
            log.debug("CommitLedger transitioned to COMMITTED: idempotencyKey={}", idempotencyKey);
        }

        return ledger;
    }

    /**
     * execute sink commit(idempotent).
     *
     * <p>ParquetIngestionDataSink use commitByScanning implement 3 state recovery:
     *
     * <ul>
     *   <li>file in pending/ → rename + commit via manifest
     *   <li>file in data/ but not in manifest → rebuild + re-commit
     *   <li>file in data/ and in manifest → idempotent skip
     * </ul>
     */
    private void doSinkCommit(RawWiseCommittable c)
            throws SinkRetryableException, SinkFatalException {
        if (sink instanceof ParquetIngestionDataSink parquetSink) {
            parquetSink.commitByScanning(c.tableId(), c.pendingIds(), c.txnId(), c.checkpointTs());
        } else {
            sink.commit(c.tableId(), c.pendingIds(), c.txnId(), c.checkpointTs());
        }
    }

    /** Abort CommitLedger (if state allows abort). */
    private void abortLedger(RawWiseCommittable c) {
        if (!isLedgerEnabled()) return;
        String idempotencyKey = writerIdentity.idempotencyKey(c.checkpointId());
        try {
            Optional<CommitLedger> optLedger = commitLedgerStore.load(idempotencyKey);
            if (optLedger.isEmpty()) return;
            abortLedgerIfAbortable(idempotencyKey, optLedger.get().state());
        } catch (IOException e) {
            log.warn("Failed to abort CommitLedger: idempotencyKey={}", idempotencyKey, e);
        }
    }

    /**
     * If state allows abort, update CommitLedger state to ABORTED.
     *
     * <p>Cannot abort past the point of no return (after GLOBAL_MARKER_COMMITTED), as data may
     * already be visible to the read path.
     */
    private void abortLedgerIfAbortable(String idempotencyKey, BatchState state) {
        if (state == null) return;
        if (!state.isAbortable()) {
            log.warn(
                    "Cannot abort CommitLedger (past point of no return): idempotencyKey={} state={}",
                    idempotencyKey,
                    state);
            return;
        }
        try {
            commitLedgerStore.updateState(idempotencyKey, BatchState.ABORTED);
            log.info(
                    "CommitLedger aborted: idempotencyKey={} previousState={}",
                    idempotencyKey,
                    state);
        } catch (IOException e) {
            log.warn(
                    "Failed to mark CommitLedger as ABORTED: idempotencyKey={}", idempotencyKey, e);
        } catch (IllegalStateException e) {
            log.warn(
                    "Cannot transition CommitLedger to ABORTED (illegal transition): idempotencyKey={} state={}",
                    idempotencyKey,
                    state);
        }
    }
}
