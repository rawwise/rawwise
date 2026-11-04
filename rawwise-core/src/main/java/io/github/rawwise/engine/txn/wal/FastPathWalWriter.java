package io.github.rawwise.engine.txn.wal;

import io.github.rawwise.backend.wal.DefaultWALRecord;
import io.github.rawwise.backend.wal.TxnWALRecord;
import io.github.rawwise.backend.wal.TxnWALRecordCodec;
import io.github.rawwise.backend.wal.WAL;
import io.github.rawwise.backend.wal.WALRecord;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FastPathWalWriter implements AutoCloseable {

    private static final String FASTPATH_TABLE_ID = "fastpath";

    private final WAL wal;
    private final boolean syncOnAppend;
    private final AtomicLong batchIdSeq = new AtomicLong(0);
    private final AtomicLong appendCount = new AtomicLong(0);
    private final long commitInterval;

    public FastPathWalWriter(WAL wal, boolean syncOnAppend) {
        this(wal, syncOnAppend, 32);
    }

    public FastPathWalWriter(WAL wal, boolean syncOnAppend, long commitInterval) {
        this.wal = wal;
        this.syncOnAppend = syncOnAppend;
        this.commitInterval = commitInterval;
    }

    public void append(
            long commitTs, String tableId, byte[] keyBytes, byte[] valueBytes, String writeType) {
        TxnWALRecord record =
                TxnWALRecord.fastpathWrite(commitTs, tableId, keyBytes, valueBytes, writeType);
        byte[] encoded = TxnWALRecordCodec.encode(record);
        WALRecord walRecord =
                new DefaultWALRecord(
                        System.nanoTime(),
                        encoded,
                        System.currentTimeMillis(),
                        commitTs,
                        FASTPATH_TABLE_ID,
                        "DATA");
        wal.append(walRecord, syncOnAppend);

        long count = appendCount.incrementAndGet();
        if (count % commitInterval == 0) {
            writeCommitMarker();
        }
    }

    private void writeCommitMarker() {
        long batchId = batchIdSeq.incrementAndGet();
        TxnWALRecord commitRecord = TxnWALRecord.fastpathCommit(batchId, FASTPATH_TABLE_ID);
        byte[] encoded = TxnWALRecordCodec.encode(commitRecord);
        WALRecord walRecord =
                new DefaultWALRecord(
                        System.nanoTime(),
                        encoded,
                        System.currentTimeMillis(),
                        batchId,
                        FASTPATH_TABLE_ID,
                        "COMMIT");
        wal.append(walRecord, true);
        log.debug(
                "FastPath WAL commit marker written batchId={} appendCount={}",
                batchId,
                appendCount.get());
    }

    public void flush() {
        writeCommitMarker();
    }

    @Override
    public void close() {
        try {
            flush();
        } catch (Exception e) {
            log.warn("FastPathWalWriter final flush failed", e);
        }
        wal.close();
    }
}
