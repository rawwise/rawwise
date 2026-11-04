package io.github.rawwise.engine.txn.record;

/**
 * Commit type for {@link TxnCommitRecord}.
 *
 * <p>Distinguishes between OLTP single-transaction commits and BATCH ingestion commits. Used by
 * visibility checks and recovery logic to apply different policies (e.g., batch commits may
 * tolerate looser ordering guarantees for higher throughput).
 */
public enum CommitType {
    /** Single OLTP transaction commit (default path via TxnOrchestrator). */
    OLTP,

    /** Batch ingestion commit (MiniBatch / Group Commit path). */
    BATCH
}
