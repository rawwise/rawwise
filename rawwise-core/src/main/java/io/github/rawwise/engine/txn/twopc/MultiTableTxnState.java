package io.github.rawwise.engine.txn.twopc;

/**
 * Cross-table 2PC transaction state enum.
 *
 * <p>State transition paths (§16.2):
 *
 * <pre>
 *   INIT -> PREPARING -> PREPARED -> COMMITTING -> COMMITTED
 *   INIT -> PREPARING -> ABORTED (Prepare phase Conflict)
 *   PREPARED -> COMMITTING -> ABORTED (Commit phase failed, rolled back after crash recovery)
 * </pre>
 *
 * <p>Terminal states: COMMITTED, ABORTED, cannot transition further.
 */
public enum MultiTableTxnState {
    /** Initial state, transaction created */
    INIT(true, false, "事务已创建"),
    /** Prepare phase in progress */
    PREPARING(true, true, "Prepare 进行中"),
    /** All Participants prepared successfully, waiting for Commit */
    PREPARED(false, true, "所有 Participant 已 Prepared"),
    /** Commit phase in progress */
    COMMITTING(false, true, "Commit 进行中"),
    /** Committed (terminal state) */
    COMMITTED(false, false, "终态：已提交"),
    /** Aborted (terminal state) */
    ABORTED(false, false, "终态：已回滚");

    private final boolean abortable;
    private final boolean transitional;
    private final String description;

    MultiTableTxnState(boolean abortable, boolean transitional, String description) {
        this.abortable = abortable;
        this.transitional = transitional;
        this.description = description;
    }

    public boolean isAbortable() {
        return abortable;
    }

    public boolean isTransitional() {
        return transitional;
    }

    public boolean isTerminal() {
        return this == COMMITTED || this == ABORTED;
    }

    public String description() {
        return description;
    }

    public boolean canTransitionTo(MultiTableTxnState target) {
        return switch (this) {
            case INIT -> target == PREPARING || target == ABORTED;
            case PREPARING -> target == PREPARED || target == ABORTED;
            case PREPARED -> target == COMMITTING || target == ABORTED;
            case COMMITTING -> target == COMMITTED || target == ABORTED;
            case COMMITTED, ABORTED -> false;
        };
    }
}
