package io.github.rawwise.engine.storage.ingestion;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LmdbIngestionDataSink extends AbstractIngestionDataSink {

    private final IngestionWriteOperations writeOps;
    private final int maxPendingInstances;
    private final long instanceMappedBytes;
    private final ConcurrentHashMap<PendingBatchKey, CopyOnWriteArrayList<String>>
            pendingInstanceIds = new ConcurrentHashMap<>();

    public LmdbIngestionDataSink(
            IngestionWriteOperations writeOps, int maxPendingInstances, long instanceMappedBytes) {
        if (writeOps == null) {
            throw new IllegalArgumentException("writeOps must not be null");
        }
        if (maxPendingInstances < 0) {
            throw new IllegalArgumentException("maxPendingInstances must not be negative");
        }
        if (instanceMappedBytes <= 0) {
            throw new IllegalArgumentException("instanceMappedBytes must be positive");
        }
        this.writeOps = writeOps;
        this.maxPendingInstances = maxPendingInstances;
        this.instanceMappedBytes = instanceMappedBytes;
    }

    @Override
    public FlushResult flushBatch(
            String tableId,
            long txnId,
            List<byte[]> keys,
            List<byte[]> values,
            long schemaVersion,
            long minSeq,
            long maxSeq)
            throws SinkRetryableException, SinkFatalException {
        return withReadLock(
                () -> {
                    validateFlushArgs(keys, values);
                    CopyOnWriteArrayList<String> currentPending =
                            pendingInstanceIds.get(PendingBatchKey.of(tableId, txnId));
                    int currentCount = currentPending != null ? currentPending.size() : 0;
                    if (maxPendingInstances > 0 && currentCount >= maxPendingInstances) {
                        throw new SinkRetryableException(
                                "Backpressure (soft limit): pending instances "
                                        + currentCount
                                        + " exceed limit "
                                        + maxPendingInstances
                                        + ". Under concurrent flush, actual count may exceed this limit.");
                    }
                    List<String> pendingIds =
                            writeOps.batchPut(
                                    tableId, txnId, keys, values, schemaVersion, minSeq, maxSeq);
                    pendingInstanceIds
                            .computeIfAbsent(
                                    PendingBatchKey.of(tableId, txnId),
                                    k -> new CopyOnWriteArrayList<>())
                            .addAll(pendingIds);
                    return new FlushResult(pendingIds);
                });
    }

    @Override
    public void prepare(String tableId, List<String> pendingIds, long txnId)
            throws SinkRetryableException, SinkFatalException {
        withReadLock(
                () -> {
                    writeOps.prepareWithStore(tableId, pendingIds, txnId);
                });
    }

    @Override
    public void commit(String tableId, List<String> pendingIds, long txnId, long commitTs)
            throws SinkRetryableException, SinkFatalException {
        withWriteLock(
                (SinkRunnable)
                        () -> {
                            writeOps.commitPendingInstances(tableId, pendingIds, txnId, commitTs);
                            CopyOnWriteArrayList<String> current =
                                    pendingInstanceIds.get(PendingBatchKey.of(tableId, txnId));
                            if (current != null) {
                                current.removeAll(pendingIds);
                                if (current.isEmpty()) {
                                    pendingInstanceIds.remove(PendingBatchKey.of(tableId, txnId));
                                }
                            }
                        });
    }

    @Override
    public void rollback(String tableId, List<String> pendingIds, long txnId)
            throws SinkFatalException {
        try {
            withWriteLock(
                    (SinkRunnable)
                            () -> {
                                writeOps.rollbackPendingInstances(tableId, pendingIds, txnId);
                                CopyOnWriteArrayList<String> current =
                                        pendingInstanceIds.get(PendingBatchKey.of(tableId, txnId));
                                if (current != null) {
                                    current.removeAll(pendingIds);
                                    if (current.isEmpty()) {
                                        pendingInstanceIds.remove(
                                                PendingBatchKey.of(tableId, txnId));
                                    }
                                }
                            });
        } catch (SinkRetryableException e) {
            throw new SinkFatalException("Unexpected retryable error during rollback", e);
        }
    }

    @Override
    public RecoveryResult recoverPending(String tableId)
            throws SinkRetryableException, SinkFatalException {
        return withWriteLock(() -> writeOps.recoverPendingInstances(tableId));
    }

    @Override
    public String sinkType() {
        return "lmdb";
    }

    @Override
    protected void cleanupPendingData() {
        for (var entry : pendingInstanceIds.entrySet()) {
            PendingBatchKey key = entry.getKey();
            try {
                writeOps.rollbackPendingInstances(key.tableId(), entry.getValue(), key.txnId());
            } catch (Exception e) {
                log.error(
                        "Failed to rollback pending instances during close: tableId={}, txnId={}",
                        key.tableId(),
                        key.txnId(),
                        e);
            }
        }
        pendingInstanceIds.clear();
    }
}
