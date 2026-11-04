package io.github.rawwise.engine.txn.wal;

import java.util.List;

/**
 * transaction WAL writerinterface.
 *
 * <p>definetransactionlifecycleeachstage WAL persistenceoperation. forcrash
 * recoveryandidempotentreplay.
 *
 * <p>Implementation class: {@link InMemoryTxnWALWriter}( phase in-memory implementation)
 */
public interface TxnWALWriter {

    /** write TXN_BEGIN record: transactionstart. */
    void writeBegin(TxnWALRecord.TxnBeginRecord record);

    /** write TXN_WRITE_INTENT record: singletimewriteintent. */
    void writeWriteIntent(TxnWALRecord.TxnWriteIntent record);

    /** write TXN_PREPARE_AND_DURABLE record: prepare+datapersistence. */
    void writePrepareAndDurable(TxnWALRecord.TxnPrepareAndDurable record);

    /** write TXN_COMMITTED record: transactioncommittedconfirm. */
    void writeCommitted(TxnWALRecord.TxnCommitted record);

    /** write TXN_ABORT record: transactionrollback. */
    void writeAbort(TxnWALRecord.TxnAbort record);

    /** write TXN_VECTOR_PREPARED record: vectorwritepreparecomplete. */
    void writeVectorPrepared(TxnWALRecord.TxnVectorPrepared record);

    /**
     * scanall WAL record(forcrash recovery).
     *
     * @return allalreadywrite WAL record
     */
    List<Object> scan();
}
