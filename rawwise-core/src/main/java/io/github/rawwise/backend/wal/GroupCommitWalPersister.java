package io.github.rawwise.backend.wal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GroupCommitWalPersister {

    private static final int DEFAULT_MAX_PENDING = 10_000;

    private final WAL wal;
    private final long syncTimeoutMs;
    private final int maxBatchSize;
    private final int maxPendingTxns;
    private final BlockingQueue<PendingTxn> pendingQueue;
    private volatile Thread syncThread;
    private volatile boolean running;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public GroupCommitWalPersister(WAL wal, long syncTimeoutMs, int maxBatchSize) {
        this(wal, syncTimeoutMs, maxBatchSize, DEFAULT_MAX_PENDING);
    }

    public GroupCommitWalPersister(
            WAL wal, long syncTimeoutMs, int maxBatchSize, int maxPendingTxns) {
        this.wal = wal;
        this.syncTimeoutMs = syncTimeoutMs;
        this.maxBatchSize = maxBatchSize;
        this.maxPendingTxns = maxPendingTxns;
        this.pendingQueue = new LinkedBlockingQueue<>(maxPendingTxns);
        this.running = false;
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        running = true;
        syncThread = new Thread(this::syncLoop, "wal-group-commit");
        syncThread.setDaemon(false);
        syncThread.start();
    }

    public void stop() {
        running = false;
        if (syncThread != null) {
            syncThread.interrupt();
            try {
                syncThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        drainRemaining();
    }

    public void persistWithGroupCommit(long txnId, List<WALRecord> records)
            throws WalSyncFailedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean failed = new AtomicBoolean(false);
        PendingTxn pendingTxn = new PendingTxn(txnId, records, latch, failed);

        if (!pendingQueue.offer(pendingTxn)) {
            throw new WalSyncFailedException(
                    txnId,
                    "WAL pending queue is full (" + maxPendingTxns + "), backpressure applied");
        }

        try {
            if (!latch.await(syncTimeoutMs, TimeUnit.MILLISECONDS)) {
                throw new WalSyncFailedException(txnId, "Timeout waiting for WAL sync");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new WalSyncFailedException(txnId, "Interrupted while waiting for WAL sync", e);
        }

        if (failed.get()) {
            throw new WalSyncFailedException(txnId, "WAL sync failed");
        }
    }

    private void syncLoop() {
        try {
            while (running || !pendingQueue.isEmpty()) {
                PendingTxn first = null;
                try {
                    first = pendingQueue.poll(1, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    if (!running && pendingQueue.isEmpty()) {
                        break;
                    }
                    continue;
                }

                if (first == null) {
                    continue;
                }

                List<PendingTxn> batch = new ArrayList<>();
                batch.add(first);
                pendingQueue.drainTo(batch, maxBatchSize - 1);

                processBatch(batch);
            }
        } finally {
            drainRemaining();
        }
    }

    private void drainRemaining() {
        List<PendingTxn> remaining = new ArrayList<>();
        pendingQueue.drainTo(remaining);
        if (!remaining.isEmpty()) {
            processBatch(remaining);
        }
    }

    private void processBatch(List<PendingTxn> batch) {
        try {
            List<WALRecord> allRecords = new ArrayList<>();
            for (PendingTxn txn : batch) {
                allRecords.addAll(txn.records());
            }

            for (int i = 0; i < allRecords.size(); i++) {
                boolean sync =
                        (i == allRecords.size() - 1) || isTxnBoundaryRecord(allRecords.get(i));
                wal.append(allRecords.get(i), sync);
            }

            for (PendingTxn txn : batch) {
                txn.latch().countDown();
            }
        } catch (Exception e) {
            log.error(
                    "GroupCommitWalPersister batch processing failed for {} txns", batch.size(), e);
            for (PendingTxn txn : batch) {
                txn.failed().set(true);
                txn.latch().countDown();
            }
        }
    }

    private boolean isTxnBoundaryRecord(WALRecord record) {
        if (record == null || record.value() == null || record.value().length == 0) {
            return false;
        }
        byte firstByte = record.value()[0];
        return firstByte >= WALRecordCodec.RECORD_TYPE_TXN_BEGIN
                && firstByte <= WALRecordCodec.RECORD_TYPE_TXN_ABORT;
    }
}
