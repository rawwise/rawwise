package io.github.rawwise.engine.txn.orchestrate;

import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.clock.LogicalClock;
import io.github.rawwise.engine.txn.wal.TxnWALRecord;
import java.util.List;

/**
 * RecoverStage: crash recoverystageinterface.
 *
 * <p>startwhenexecute, scan WAL inall TXN_* record, by 5 stepprioritydeterminerecoveryaction.
 *
 * <h3>recoveryentry point</h3>
 *
 * {@link #recover(LogicalClock, LmdbInstance, List)} receivestartwhen scantoall WAL record, bystate
 * machinedetermineeachentryrecordhandlepartymode.
 */
public interface RecoverStage {

    /** recovery decision. */
    enum RecoveryAction {
        REDO_COMMITTED,
        ABORT,
        SKIP_ALREADY_COMMITTED,
        SKIP_ALREADY_ABORTED,
        NEEDS_MANUAL_INTERVENTION
    }

    /** recoveryresult. */
    record RecoveryResult(long txnId, RecoveryAction action, String detail) {}

    /** recoveryresultlist. */
    record RecoveryReport(List<RecoveryResult> results, int recovered, int aborted, int skipped) {}

    /** WAL record wrapper for recovery. */
    sealed interface WalEntry
            permits WalEntry.Begin,
                    WalEntry.WriteIntent,
                    WalEntry.PrepareAndDurable,
                    WalEntry.Committed,
                    WalEntry.Aborted,
                    WalEntry.VectorPrepared {

        record Begin(TxnWALRecord.TxnBeginRecord record) implements WalEntry {}

        record WriteIntent(TxnWALRecord.TxnWriteIntent record) implements WalEntry {}

        record PrepareAndDurable(TxnWALRecord.TxnPrepareAndDurable record) implements WalEntry {}

        record Committed(TxnWALRecord.TxnCommitted record) implements WalEntry {}

        record Aborted(TxnWALRecord.TxnAbort record) implements WalEntry {}

        record VectorPrepared(TxnWALRecord.TxnVectorPrepared record) implements WalEntry {}
    }

    /**
     * executecrash recovery.
     *
     * @param clock logical clock (for advanceToAtLeast during recovery)
     * @param lmdbInstance LMDB instance (for checking data completeness)
     * @param walEntries all records scanned from WAL
     * @return recovery report
     */
    RecoveryReport recover(
            LogicalClock clock, LmdbInstance lmdbInstance, List<WalEntry> walEntries);
}
