package io.github.rawwise.engine.txn.twopc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;

/**
 * Cross-table two-phase commit coordinator.
 *
 * <p>Architecture (§16.2):
 *
 * <pre>
 *   Stage 1 Prepare (parallel):
 *     Coordinator -> all Participants: prepare(txnId, keys, values)
 *     Participant: OCC validation + WAL persistence
 *     Any Conflict -> Rollback all
 *
 *   Stage 2 Commit (serial, by tableId lexicographic order to avoid deadlock):
 *     Coordinator -> all Participants: commit(txnId)
 *     Participant: data publish + Metastore commit
 *     Stage 2 failure -> record failure state, retry during crash recovery (idempotent)
 * </pre>
 *
 * <p>Semantics:
 *
 * <ul>
 *   <li>Cross-table atomicity: all tables either all Commit or all Rollback
 *   <li>Deadlock avoidance: Commit stage executes serially by tableId lexicographic order, avoiding
 *       circular waits
 *   <li>Idempotency: Prepare/Commit/Rollback are all idempotent, safe to retry after crash recovery
 *   <li>Timeout control: Prepare 30s timeout -> auto Rollback; Commit 60s timeout -> record failure
 *       for recovery
 *   <li>Transaction state persistence: MVP stage in-memory, production can persist to WAL/Metastore
 * </ul>
 *
 * <p>Thread safety: each transaction's 2PC flow executes synchronously, transactions are isolated
 * by txnId.
 */
@Slf4j
public class TwoPhaseCommitCoordinator {
    public static final long DEFAULT_PREPARE_TIMEOUT_MS = 30_000L;
    public static final long DEFAULT_COMMIT_TIMEOUT_MS = 60_000L;

    private final Map<String, TwoPhaseCommitParticipant> participants = new ConcurrentHashMap<>();
    private final Map<String, MultiTableTxnContext> txnContexts = new ConcurrentHashMap<>();
    private final TxnStateStore stateStore;

    public TwoPhaseCommitCoordinator() {
        this(new InMemoryTxnStateStore());
    }

    public TwoPhaseCommitCoordinator(TxnStateStore stateStore) {
        this.stateStore = stateStore;
    }

    /**
     * Registers a 2PC participant for a table.
     *
     * @param participant the participant
     */
    public void registerParticipant(TwoPhaseCommitParticipant participant) {
        Objects.requireNonNull(participant, "participant");
        String tableId = participant.tableId();
        if (tableId == null || tableId.isBlank()) {
            throw new IllegalArgumentException("participant.tableId() must not be blank");
        }
        participants.put(tableId, participant);
        log.info("2PC participant registered: tableId={}", tableId);
    }

    /**
     * Initiates a cross-table 2PC transaction.
     *
     * <p>Flow:
     *
     * <ol>
     *   <li>Generate txnId + create transaction context
     *   <li>Stage 1 Prepare: call all Participant.prepare() in parallel (by tableId lexicographic
     *       order)
     *   <li>If any Prepare fails -> Rollback all already-Prepared Participants
     *   <li>Stage 2 Commit: call Participant.commit() serially by tableId lexicographic order
     *   <li>Return final result
     * </ol>
     *
     * @param tableWrites write data per table (tableId -> keys/values)
     * @return commit result
     */
    public TwoPhaseCommitResult commit(Map<String, TableWrite> tableWrites) {
        Objects.requireNonNull(tableWrites, "tableWrites");
        if (tableWrites.isEmpty()) {
            return TwoPhaseCommitResult.success("empty", 0);
        }
        String txnId = "2pc-" + UUID.randomUUID();
        List<String> sortedTableIds = new ArrayList<>(tableWrites.keySet());
        Collections.sort(sortedTableIds);
        MultiTableTxnContext ctx = new MultiTableTxnContext(txnId, sortedTableIds);
        txnContexts.put(txnId, ctx);
        stateStore.persist(ctx);
        log.info("2PC begin: txnId={}, tables={}", txnId, sortedTableIds);

        if (!preparePhase(ctx, tableWrites, sortedTableIds)) {
            rollbackPhase(ctx);
            txnContexts.remove(txnId);
            return TwoPhaseCommitResult.conflict(txnId, ctx.failureReason());
        }
        if (!commitPhase(ctx, sortedTableIds)) {
            log.error(
                    "2PC commit phase failed: txnId={}, reason={}. "
                            + "Participants in inconsistent state, recovery needed.",
                    txnId,
                    ctx.failureReason());
            return TwoPhaseCommitResult.commitFailed(txnId, ctx.failureReason());
        }
        log.info("2PC committed: txnId={}, tables={}", txnId, sortedTableIds);
        txnContexts.remove(txnId);
        return TwoPhaseCommitResult.success(txnId, sortedTableIds.size());
    }

    /**
     * Stage 1: Prepare (serial by tableId lexicographic order, to avoid deadlock).
     *
     * <p>The design doc says Prepare is parallel, but parallel execution may cause deadlock
     * (cross-table lock contention). Serial Prepare (by tableId lexicographic order) is used
     * instead to guarantee consistency and simplify implementation.
     */
    private boolean preparePhase(
            MultiTableTxnContext ctx,
            Map<String, TableWrite> tableWrites,
            List<String> sortedTableIds) {
        ctx.transitionTo(MultiTableTxnState.PREPARING);
        stateStore.persist(ctx);
        for (String tableId : sortedTableIds) {
            TwoPhaseCommitParticipant participant = participants.get(tableId);
            if (participant == null) {
                ctx.failureReason = "participant not registered: " + tableId;
                log.warn("2PC prepare failed: txnId={}, {}", ctx.txnId, ctx.failureReason);
                return false;
            }
            TableWrite write = tableWrites.get(tableId);
            try {
                boolean prepared =
                        CompletableFuture.supplyAsync(
                                        () ->
                                                participant.prepare(
                                                        ctx.txnId, write.keys(), write.values()))
                                .orTimeout(DEFAULT_PREPARE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                                .join();
                if (!prepared) {
                    ctx.failureReason = "OCC conflict in table: " + tableId;
                    log.warn("2PC prepare conflict: txnId={}, table={}", ctx.txnId, tableId);
                    return false;
                }
                ctx.preparedTables.add(tableId);
                log.debug("2PC prepared: txnId={}, table={}", ctx.txnId, tableId);
            } catch (CompletionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof TimeoutException) {
                    ctx.failureReason = "prepare timeout in table: " + tableId;
                } else {
                    ctx.failureReason =
                            "prepare exception in table " + tableId + ": " + cause.getMessage();
                }
                log.warn(
                        "2PC prepare failed: txnId={}, table={}, error={}",
                        ctx.txnId,
                        tableId,
                        ctx.failureReason);
                return false;
            }
        }
        ctx.transitionTo(MultiTableTxnState.PREPARED);
        stateStore.persist(ctx);
        return true;
    }

    /**
     * Stage 2: Commit (serial by tableId lexicographic order, to avoid deadlock).
     *
     * <p>Serial Commit guarantee: if a crash occurs mid-way, already-committed tableIds precede the
     * remaining ones in sortedTableIds. Recovery only needs to resume committing from the
     * breakpoint (idempotent retry).
     */
    private boolean commitPhase(MultiTableTxnContext ctx, List<String> sortedTableIds) {
        ctx.transitionTo(MultiTableTxnState.COMMITTING);
        stateStore.persist(ctx);
        for (String tableId : sortedTableIds) {
            TwoPhaseCommitParticipant participant = participants.get(tableId);
            if (participant == null) {
                ctx.failureReason = "participant not registered: " + tableId;
                return false;
            }
            try {
                boolean committed =
                        CompletableFuture.supplyAsync(() -> participant.commit(ctx.txnId))
                                .orTimeout(DEFAULT_COMMIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                                .join();
                if (!committed) {
                    ctx.failureReason = "commit failed in table: " + tableId;
                    return false;
                }
                ctx.committedTables.add(tableId);
                log.debug("2PC committed: txnId={}, table={}", ctx.txnId, tableId);
            } catch (CompletionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof TimeoutException) {
                    ctx.failureReason = "commit timeout in table: " + tableId;
                } else {
                    ctx.failureReason =
                            "commit exception in table " + tableId + ": " + cause.getMessage();
                }
                log.error(
                        "2PC commit failed: txnId={}, table={}, error={}",
                        ctx.txnId,
                        tableId,
                        ctx.failureReason,
                        e);
                return false;
            }
        }
        ctx.transitionTo(MultiTableTxnState.COMMITTED);
        stateStore.persist(ctx);
        return true;
    }

    /** Rollback already-Prepared Participants. */
    private void rollbackPhase(MultiTableTxnContext ctx) {
        ctx.transitionTo(MultiTableTxnState.ABORTED);
        stateStore.persist(ctx);
        for (String tableId : ctx.preparedTables) {
            TwoPhaseCommitParticipant participant = participants.get(tableId);
            if (participant == null) continue;
            try {
                participant.rollback(ctx.txnId);
                log.debug("2PC rolled back: txnId={}, table={}", ctx.txnId, tableId);
            } catch (Exception e) {
                log.warn(
                        "2PC rollback exception: txnId={}, table={}, error={}",
                        ctx.txnId,
                        tableId,
                        e.getMessage());
            }
        }
    }

    /**
     * Crash recovery: query transaction state, retry unfinished Commit or Rollback.
     *
     * @param txnId transaction ID
     * @return recovery result
     */
    public TwoPhaseCommitResult recover(String txnId) {
        MultiTableTxnContext ctx = txnContexts.get(txnId);
        if (ctx == null) {
            ctx = stateStore.load(txnId);
        }
        if (ctx == null) {
            return TwoPhaseCommitResult.unknown(txnId);
        }
        if (ctx.state == MultiTableTxnState.COMMITTED) {
            return TwoPhaseCommitResult.success(txnId, ctx.committedTables.size());
        }
        if (ctx.state == MultiTableTxnState.ABORTED) {
            return TwoPhaseCommitResult.conflict(txnId, "already aborted");
        }
        if (ctx.state == MultiTableTxnState.COMMITTING) {
            log.info("2PC recovery: txnId={}, retrying commit for remaining tables", txnId);
            List<String> remaining = new ArrayList<>(ctx.allTables);
            remaining.removeAll(ctx.committedTables);
            Collections.sort(remaining);
            if (commitPhase(ctx, remaining)) {
                return TwoPhaseCommitResult.success(txnId, ctx.committedTables.size());
            }
            return TwoPhaseCommitResult.commitFailed(txnId, ctx.failureReason());
        }
        if (ctx.state == MultiTableTxnState.PREPARED || ctx.state == MultiTableTxnState.PREPARING) {
            log.info("2PC recovery: txnId={}, rolling back (not fully prepared)", txnId);
            rollbackPhase(ctx);
            return TwoPhaseCommitResult.conflict(txnId, "rolled back during recovery");
        }
        return TwoPhaseCommitResult.unknown(txnId);
    }

    /** Queries the current transaction state. */
    public MultiTableTxnState queryTxnState(String txnId) {
        MultiTableTxnContext ctx = txnContexts.get(txnId);
        return ctx != null ? ctx.state : null;
    }

    /**
     * Single-table write data.
     *
     * @param keys list of keys to write
     * @param values list of values to write
     */
    public record TableWrite(List<byte[]> keys, List<byte[]> values) {
        public TableWrite {
            if (keys == null) keys = List.of();
            if (values == null) values = List.of();
            if (keys.size() != values.size()) {
                throw new IllegalArgumentException("keys.size() must equal values.size()");
            }
        }
    }

    /** 2PC commit result. */
    public record TwoPhaseCommitResult(
            boolean success,
            String txnId,
            int tableCount,
            ResultType resultType,
            String failureReason) {
        public enum ResultType {
            SUCCESS,
            CONFLICT,
            COMMIT_FAILED,
            UNKNOWN
        }

        public static TwoPhaseCommitResult success(String txnId, int tableCount) {
            return new TwoPhaseCommitResult(true, txnId, tableCount, ResultType.SUCCESS, null);
        }

        public static TwoPhaseCommitResult conflict(String txnId, String reason) {
            return new TwoPhaseCommitResult(false, txnId, 0, ResultType.CONFLICT, reason);
        }

        public static TwoPhaseCommitResult commitFailed(String txnId, String reason) {
            return new TwoPhaseCommitResult(false, txnId, 0, ResultType.COMMIT_FAILED, reason);
        }

        public static TwoPhaseCommitResult unknown(String txnId) {
            return new TwoPhaseCommitResult(false, txnId, 0, ResultType.UNKNOWN, "txn not found");
        }
    }

    /** Cross-table transaction context (in-memory state, production can persist to WAL). */
    private static class MultiTableTxnContext {
        final String txnId;
        final List<String> allTables;
        volatile MultiTableTxnState state;
        final List<String> preparedTables = new CopyOnWriteArrayList<>();
        final List<String> committedTables = new CopyOnWriteArrayList<>();
        volatile String failureReason;

        MultiTableTxnContext(String txnId, List<String> allTables) {
            this.txnId = txnId;
            this.allTables = new ArrayList<>(allTables);
            this.state = MultiTableTxnState.INIT;
        }

        void transitionTo(MultiTableTxnState target) {
            if (state == target) {
                return;
            }
            if (!state.canTransitionTo(target)) {
                throw new IllegalStateException(
                        "Invalid state transition: "
                                + state
                                + " -> "
                                + target
                                + " for txn "
                                + txnId);
            }
            state = target;
        }

        String failureReason() {
            return failureReason;
        }
    }

    /**
     * Transaction state store interface.
     *
     * <p>TODO: replace with WAL/Metastore persistent implementation in production to ensure
     * transaction state is recoverable after crash.
     */
    interface TxnStateStore {
        void persist(MultiTableTxnContext ctx);

        MultiTableTxnContext load(String txnId);
    }

    private static class InMemoryTxnStateStore implements TxnStateStore {
        private final Map<String, MultiTableTxnContext> store = new ConcurrentHashMap<>();

        @Override
        public void persist(MultiTableTxnContext ctx) {
            store.put(ctx.txnId, ctx);
        }

        @Override
        public MultiTableTxnContext load(String txnId) {
            return store.get(txnId);
        }
    }
}
