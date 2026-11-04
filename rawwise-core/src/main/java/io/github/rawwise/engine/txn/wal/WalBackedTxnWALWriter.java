package io.github.rawwise.engine.txn.wal;

import io.github.rawwise.backend.wal.DefaultWALRecord;
import io.github.rawwise.backend.wal.TxnWALRecordCodec;
import io.github.rawwise.backend.wal.WAL;
import io.github.rawwise.backend.wal.WALRecord;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WalBackedTxnWALWriter implements TxnWALWriter {

    private final WAL wal;
    private final List<Object> records = Collections.synchronizedList(new ArrayList<>());
    private String currentTableId;

    public WalBackedTxnWALWriter(WAL wal) {
        this.wal = wal;
    }

    @Override
    public void writeBegin(TxnWALRecord.TxnBeginRecord record) {
        if (currentTableId == null && !record.tableIds().isEmpty()) {
            currentTableId = record.tableIds().iterator().next();
        }
        io.github.rawwise.backend.wal.TxnWALRecord backendRecord =
                io.github.rawwise.backend.wal.TxnWALRecord.begin(
                        record.txnId(),
                        record.tableIds().stream().toList(),
                        record.isolationLevel().name(),
                        record.readSnapshotTs(),
                        0L);
        appendWALRecord(backendRecord, false);
        records.add(record);
        log.debug("WAL TXN_BEGIN: txnId={}", record.txnId());
    }

    @Override
    public void writeWriteIntent(TxnWALRecord.TxnWriteIntent record) {
        if (currentTableId == null) {
            currentTableId = record.primaryTableId();
        }
        io.github.rawwise.backend.wal.TxnWALRecord backendRecord =
                io.github.rawwise.backend.wal.TxnWALRecord.data(
                        record.txnId(),
                        record.primaryTableId(),
                        record.keyBytes(),
                        record.valueRef(),
                        record.writeType().name());
        appendWALRecord(backendRecord, false);
        records.add(record);
        log.debug(
                "WAL TXN_WRITE_INTENT: txnId={} table={} keySize={}",
                record.txnId(),
                record.primaryTableId(),
                record.keyBytes().length);
    }

    @Override
    public void writePrepareAndDurable(TxnWALRecord.TxnPrepareAndDurable record) {
        String tableId = resolveTableId();
        io.github.rawwise.backend.wal.TxnWALRecord backendRecord =
                io.github.rawwise.backend.wal.TxnWALRecord.prepared(
                        record.txnId(), tableId, record.durableTimestamp());
        appendWALRecord(backendRecord, true);
        records.add(record);
        log.debug("WAL TXN_PREPARE_AND_DURABLE: txnId={}", record.txnId());
    }

    @Override
    public void writeCommitted(TxnWALRecord.TxnCommitted record) {
        String tableId = resolveTableId();
        io.github.rawwise.backend.wal.TxnWALRecord backendRecord =
                io.github.rawwise.backend.wal.TxnWALRecord.commit(
                        record.txnId(), tableId, record.commitTs(), 0L);
        appendWALRecord(backendRecord, true);
        records.add(record);
        log.debug("WAL TXN_COMMITTED: txnId={} commitTs={}", record.txnId(), record.commitTs());
    }

    @Override
    public void writeVectorPrepared(TxnWALRecord.TxnVectorPrepared record) {
        String tableId = resolveTableId();
        io.github.rawwise.backend.wal.TxnWALRecord backendRecord =
                io.github.rawwise.backend.wal.TxnWALRecord.prepared(
                        record.txnId(), tableId, record.timestamp());
        appendWALRecord(backendRecord, true);
        records.add(record);
        log.debug(
                "WAL TXN_VECTOR_PREPARED: txnId={} namespace={}",
                record.txnId(),
                record.namespace());
    }

    @Override
    public void writeAbort(TxnWALRecord.TxnAbort record) {
        String tableId = resolveTableId();
        io.github.rawwise.backend.wal.TxnWALRecord backendRecord =
                io.github.rawwise.backend.wal.TxnWALRecord.abort(record.txnId(), tableId);
        appendWALRecord(backendRecord, false);
        records.add(record);
        log.debug("WAL TXN_ABORT: txnId={} reason={}", record.txnId(), record.reason());
    }

    @Override
    public List<Object> scan() {
        synchronized (records) {
            return List.copyOf(records);
        }
    }

    private String resolveTableId() {
        return currentTableId != null ? currentTableId : "unknown";
    }

    private void appendWALRecord(
            io.github.rawwise.backend.wal.TxnWALRecord backendRecord, boolean sync) {
        byte[] encoded = TxnWALRecordCodec.encode(backendRecord);
        String tableId;
        if (backendRecord.tableId() != null) {
            tableId = backendRecord.tableId();
        } else if (backendRecord.tableIds() != null && !backendRecord.tableIds().isEmpty()) {
            tableId = backendRecord.tableIds().get(0);
        } else {
            tableId = "unknown";
        }
        WALRecord walRecord =
                new DefaultWALRecord(
                        System.nanoTime(),
                        encoded,
                        System.currentTimeMillis(),
                        -1L,
                        tableId,
                        "DATA");
        wal.append(walRecord, sync);
    }
}
