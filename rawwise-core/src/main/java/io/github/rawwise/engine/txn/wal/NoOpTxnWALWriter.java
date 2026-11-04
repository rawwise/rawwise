package io.github.rawwise.engine.txn.wal;

import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * No-op WAL implementation(H-6: WalMode.NONE).
 *
 * <p>all write* methoddirectlyreturn, notpersistenceanyrecord.{@link #scan()} neverreturnemptylist,
 * this meanscrash recoverywill not be able to based on WAL enterrowreplay — requireddependent LMDB
 * commit marker complete committedtransactionvisibleity、notcommittransactionrollback.
 *
 * <h3>usescenario</h3>
 *
 * <ul>
 *   <li>development/singlemetatest: avoid WAL persistenceoverhead
 *   <li>throughput-sensitive andaccept"crashafternotcommittransactionmust lose"deployment: LMDB
 *       asasuniquepersistencelayer
 * </ul>
 *
 * <h3>andpersistencemode difference</h3>
 *
 * <ul>
 *   <li>notwillremainunder TxnAbort/TxnCommitted recordfordiagnose
 *   <li>notcross-nodereplay
 *   <li>unavailable {@link TxnWALReader} listnotcompletetransaction
 * </ul>
 *
 * <p>Thread safety: thisclassnostate, singleexamplecanglobalshare.
 */
@Slf4j
public final class NoOpTxnWALWriter implements TxnWALWriter {

    public static final NoOpTxnWALWriter INSTANCE = new NoOpTxnWALWriter();

    private NoOpTxnWALWriter() {
        log.info(
                "NoOpTxnWALWriter activated — WAL persistence is DISABLED. "
                        + "Crash recovery will rely solely on LMDB commit marker.");
    }

    @Override
    public void writeBegin(TxnWALRecord.TxnBeginRecord record) {
        // no-op
    }

    @Override
    public void writeWriteIntent(TxnWALRecord.TxnWriteIntent record) {
        // no-op
    }

    @Override
    public void writePrepareAndDurable(TxnWALRecord.TxnPrepareAndDurable record) {
        // no-op
    }

    @Override
    public void writeCommitted(TxnWALRecord.TxnCommitted record) {
        // no-op
    }

    @Override
    public void writeAbort(TxnWALRecord.TxnAbort record) {
        // no-op
    }

    @Override
    public void writeVectorPrepared(TxnWALRecord.TxnVectorPrepared record) {
        // no-op
    }

    @Override
    public List<Object> scan() {
        return Collections.emptyList();
    }
}
