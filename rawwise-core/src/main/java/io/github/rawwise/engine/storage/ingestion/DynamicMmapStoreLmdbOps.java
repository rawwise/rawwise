package io.github.rawwise.engine.storage.ingestion;

import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.table.TableStore;
import java.util.Collections;
import java.util.List;

public class DynamicMmapStoreLmdbOps implements IngestionWriteOperations {

    private final DynamicMmapStore store;

    public DynamicMmapStoreLmdbOps(DynamicMmapStore store) {
        if (store == null) {
            throw new IllegalArgumentException("store must not be null");
        }
        this.store = store;
    }

    @Override
    public List<String> batchPut(
            String tableId,
            long txnId,
            List<byte[]> keys,
            List<byte[]> values,
            long schemaVersion,
            long minSeq,
            long maxSeq)
            throws SinkRetryableException, SinkFatalException {
        TableStore tableStore = store.tableStore(tableId);
        LmdbInstance current = tableStore.getCurrentWritableInstance();
        if (current == null || current.readOnly()) {
            throw new SinkRetryableException(
                    "No writable LMDB instance available for table " + tableId);
        }
        LmdbInstance.BatchPutResult result = current.batchPut(keys, values);
        if (!result.isAllSuccess()) {
            throw new SinkRetryableException("LMDB batch put failed for table " + tableId);
        }
        return Collections.singletonList(current.instanceId());
    }

    @Override
    public void prepareWithStore(String tableId, List<String> pendingIds, long txnId)
            throws SinkRetryableException, SinkFatalException {}

    @Override
    public void commitPendingInstances(
            String tableId, List<String> pendingIds, long txnId, long commitTs)
            throws SinkRetryableException, SinkFatalException {
        TableStore tableStore = store.tableStore(tableId);
        tableStore.commitPendingInstances(pendingIds, txnId, commitTs);
    }

    @Override
    public void rollbackPendingInstances(String tableId, List<String> pendingIds, long txnId)
            throws SinkFatalException {
        TableStore tableStore = store.tableStore(tableId);
        for (String instanceId : pendingIds) {
            tableStore.rollbackPendingInstance(instanceId);
        }
    }

    @Override
    public List<String> getPendingInstanceIds(String tableId, long txnId) {
        return List.of();
    }

    @Override
    public IngestionDataSink.RecoveryResult recoverPendingInstances(String tableId)
            throws SinkRetryableException, SinkFatalException {
        return new IngestionDataSink.RecoveryResult(0, 0, List.of());
    }
}
