package io.github.rawwise.engine.storage.ingestion;

import java.util.List;

public interface IngestionWriteOperations {
    List<String> batchPut(
            String tableId,
            long txnId,
            List<byte[]> keys,
            List<byte[]> values,
            long schemaVersion,
            long minSeq,
            long maxSeq)
            throws SinkRetryableException, SinkFatalException;

    void prepareWithStore(String tableId, List<String> pendingIds, long txnId)
            throws SinkRetryableException, SinkFatalException;

    void commitPendingInstances(String tableId, List<String> pendingIds, long txnId, long commitTs)
            throws SinkRetryableException, SinkFatalException;

    void rollbackPendingInstances(String tableId, List<String> pendingIds, long txnId)
            throws SinkFatalException;

    List<String> getPendingInstanceIds(String tableId, long txnId);

    IngestionDataSink.RecoveryResult recoverPendingInstances(String tableId)
            throws SinkRetryableException, SinkFatalException;
}
