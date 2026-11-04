package io.github.rawwise.engine.txn.wal;

import java.util.List;
import java.util.Optional;

/**
 * transaction WAL readerinterface.
 *
 * <p>independentin {@link TxnWALWriter} readinterface, responsible forfrompersistence WAL
 * inscanandquerytransactionrecord. forcrash recovery ({@link
 * io.github.rawwise.engine.txn.orchestrate.RecoverStage}) and transactionstateaudit.
 *
 * <h3>recordtype</h3>
 *
 * <p>return {@code Object} elementis {@link TxnWALRecord} various defined in record type:
 *
 * <ul>
 *   <li>{@link TxnWALRecord.TxnBeginRecord}
 *   <li>{@link TxnWALRecord.TxnWriteIntent}
 *   <li>{@link TxnWALRecord.TxnPrepareAndDurable}
 *   <li>{@link TxnWALRecord.TxnCommitted}
 *   <li>{@link TxnWALRecord.TxnAbort}
 *   <li>{@link TxnWALRecord.TxnVectorPrepared}
 * </ul>
 *
 * <p>callpartycanbyuse {@code instanceof} modematchenterrowtypebranchhandle, oruse {@link
 * #toWalEntry(Object)} transitionas {@link
 * io.github.rawwise.engine.txn.orchestrate.RecoverStage.WalEntry}.
 *
 * <h3>Implementation class</h3>
 *
 * <ul>
 *   <li>{@link ScanBackedTxnWALReader} — from WAL scan read
 * </ul>
 */
public interface TxnWALReader {

    /**
     * readall WAL record(bywriteorder).
     *
     * @return immutable WAL recordlist
     */
    List<Object> readAll();

    /**
     * readspecified txnId latestoneentry WAL record.
     *
     * <p>forfastdeterminetransactionfinalstate(COMMITTED / ABORT / still in progress).
     *
     * @param txnId transaction ID
     * @return latestrecord, ifthetransactionnorecordthenreturn Optional.empty()
     */
    Optional<Object> readLatest(long txnId);

    /**
     * readspecified txnId all WAL record(bywriteorder).
     *
     * <p>forcrash recoverywhenbytransactiongroupreplay.
     *
     * @param txnId transaction ID
     * @return thetransactionall WAL record, ifnothenreturnemptylist
     */
    List<Object> readByTxnId(long txnId);

    /**
     * readallnotcommit(uncommitted) WAL record.
     *
     * <p>"notcommit"define: transactionhas WAL recordbutneitherhas {@link
     * TxnWALRecord.TxnCommitted} alsonohas {@link TxnWALRecord.TxnAbort}
     * record.returnisthesetransactionall WAL record (including BEGIN / WRITE_INTENT /
     * PREPARE_AND_DURABLE etc).
     *
     * <p>forcrash recoverywhenidentifyrequiresrecoveryorinstoptransaction.
     *
     * @return notcommittransaction WAL recordlist
     */
    List<Object> readUncommitted();

    /**
     * willoriginal WAL recordtoobjecttransitionas {@link
     * io.github.rawwise.engine.txn.orchestrate.RecoverStage.WalEntry}.
     *
     * <p>Utility methods, provide {@link
     * io.github.rawwise.engine.txn.orchestrate.DefaultRecoverStage}
     * etccallpartyuse.ifrecordtypecannotidentify, throw {@link IllegalArgumentException}.
     *
     * @param record original WAL record(TxnWALRecord.* typeone of)
     * @return corresponding WalEntry sealed interface instance
     * @throws IllegalArgumentException ifrecordtypecannotidentify
     */
    static io.github.rawwise.engine.txn.orchestrate.RecoverStage.WalEntry toWalEntry(
            Object record) {
        if (record instanceof TxnWALRecord.TxnBeginRecord b) {
            return new io.github.rawwise.engine.txn.orchestrate.RecoverStage.WalEntry.Begin(b);
        }
        if (record instanceof TxnWALRecord.TxnWriteIntent w) {
            return new io.github.rawwise.engine.txn.orchestrate.RecoverStage.WalEntry.WriteIntent(
                    w);
        }
        if (record instanceof TxnWALRecord.TxnPrepareAndDurable p) {
            return new io.github.rawwise.engine.txn.orchestrate.RecoverStage.WalEntry
                    .PrepareAndDurable(p);
        }
        if (record instanceof TxnWALRecord.TxnCommitted c) {
            return new io.github.rawwise.engine.txn.orchestrate.RecoverStage.WalEntry.Committed(c);
        }
        if (record instanceof TxnWALRecord.TxnAbort a) {
            return new io.github.rawwise.engine.txn.orchestrate.RecoverStage.WalEntry.Aborted(a);
        }
        if (record instanceof TxnWALRecord.TxnVectorPrepared vp) {
            return new io.github.rawwise.engine.txn.orchestrate.RecoverStage.WalEntry
                    .VectorPrepared(vp);
        }
        throw new IllegalArgumentException("Unknown WAL record type: " + record.getClass());
    }

    /**
     * from WAL recordtoobjectinextract txnId.
     *
     * @param record WAL record(TxnWALRecord.* typeone of)
     * @return transaction ID
     * @throws IllegalArgumentException ifrecordtypecannotidentify
     */
    static long extractTxnId(Object record) {
        if (record instanceof TxnWALRecord.TxnBeginRecord b) return b.txnId();
        if (record instanceof TxnWALRecord.TxnWriteIntent w) return w.txnId();
        if (record instanceof TxnWALRecord.TxnPrepareAndDurable p) return p.txnId();
        if (record instanceof TxnWALRecord.TxnCommitted c) return c.txnId();
        if (record instanceof TxnWALRecord.TxnAbort a) return a.txnId();
        if (record instanceof TxnWALRecord.TxnVectorPrepared vp) return vp.txnId();
        throw new IllegalArgumentException("Unknown WAL record type: " + record.getClass());
    }
}
