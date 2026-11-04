package io.github.rawwise.engine.txn.wal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * In-memory WAL writer( phase).
 *
 * <p>willall WAL recordstorageinmemory List in, providecrash recoveryscanuse. phase will be
 * replaced with file-based WAL implementation.
 */
@Slf4j
public class InMemoryTxnWALWriter implements TxnWALWriter {

    private final List<Object> records = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void writeBegin(TxnWALRecord.TxnBeginRecord record) {
        records.add(record);
        log.debug("WAL TXN_BEGIN: txnId={}", record.txnId());
    }

    @Override
    public void writeWriteIntent(TxnWALRecord.TxnWriteIntent record) {
        records.add(record);
        log.debug(
                "WAL TXN_WRITE_INTENT: txnId={} table={} keySize={}",
                record.txnId(),
                record.primaryTableId(),
                record.keyBytes().length);
    }

    @Override
    public void writePrepareAndDurable(TxnWALRecord.TxnPrepareAndDurable record) {
        records.add(record);
        log.debug("WAL TXN_PREPARE_AND_DURABLE: txnId={}", record.txnId());
    }

    @Override
    public void writeCommitted(TxnWALRecord.TxnCommitted record) {
        records.add(record);
        log.debug("WAL TXN_COMMITTED: txnId={} commitTs={}", record.txnId(), record.commitTs());
    }

    @Override
    public void writeAbort(TxnWALRecord.TxnAbort record) {
        records.add(record);
        log.debug("WAL TXN_ABORT: txnId={} reason={}", record.txnId(), record.reason());
    }

    @Override
    public void writeVectorPrepared(TxnWALRecord.TxnVectorPrepared record) {
        records.add(record);
        log.debug(
                "WAL TXN_VECTOR_PREPARED: txnId={} namespace={}",
                record.txnId(),
                record.namespace());
    }

    /**
     * scanall WAL record(snapshot).
     *
     * @return immutablelist
     */
    public List<Object> scan() {
        synchronized (records) {
            return List.copyOf(records);
        }
    }
}
