package io.github.rawwise.engine.txn.twopc;

import java.util.List;

/**
 * 2PC participant interface (single-table transaction executor).
 *
 * <p>Architecture (§16.1): each Participant handles Prepare/Commit/Rollback for one table, driven
 * by {@link TwoPhaseCommitCoordinator}.
 *
 * <p>Semantics:
 *
 * <ul>
 *   <li>{@link #prepare} performs OCC validation + WAL persistence (Commit Pipeline stages 1-2)
 *   <li>{@link #commit} performs data publish + Metastore commit (Commit Pipeline stages 3-4)
 *   <li>{@link #rollback} cleans up WriteBuffer + writes abort record
 *   <li>All methods are idempotent: safe to retry after crash recovery
 * </ul>
 */
public interface TwoPhaseCommitParticipant {

    /** Returns the table identifier this participant is responsible for. */
    String tableId();

    /**
     * Stage 1: Prepare (performs OCC validation + WAL persistence).
     *
     * @implSpec prepare() MUST complete WAL persistence before returning true (CCI-006 WAL-ahead
     *     principle). Returning true means WAL is flushed; after crash recovery the PREPARED state
     *     can be rebuilt via WAL replay. Violating this contract leads to data loss after crash
     *     (WAL missing but Coordinator assumes prepared).
     * @param txnId transaction ID
     * @param keys list of keys to write
     * @param values list of values to write
     * @return true if Prepare succeeded (WAL persisted); false on OCC conflict
     */
    boolean prepare(String txnId, List<byte[]> keys, List<byte[]> values);

    /**
     * Stage 2: Commit (performs data publish + Metastore commit).
     *
     * <p>Idempotent: if already committed, returns true (safe retry).
     *
     * @param txnId transaction ID
     * @return true if Commit succeeded; false on failure
     */
    boolean commit(String txnId);

    /**
     * Rollback (cleans up WriteBuffer + writes abort record).
     *
     * <p>Idempotent: if already aborted, returns true (safe retry).
     *
     * @param txnId transaction ID
     * @return true if Rollback succeeded
     */
    boolean rollback(String txnId);

    /**
     * Queries the current transaction state (used during crash recovery).
     *
     * @param txnId transaction ID
     * @return transaction state, or null (unknown transaction)
     */
    ParticipantTxnState queryState(String txnId);

    /** Participant-side transaction state. */
    enum ParticipantTxnState {
        UNKNOWN,
        PREPARED,
        COMMITTED,
        ABORTED
    }
}
