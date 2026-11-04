package io.github.rawwise.engine.storage;

import io.github.rawwise.engine.storage.ingestion.IngestionDataSink;
import io.github.rawwise.engine.storage.ingestion.IngestionWriteOperations;
import io.github.rawwise.engine.storage.ingestion.SinkFatalException;
import io.github.rawwise.engine.storage.ingestion.SinkRetryableException;
import io.github.rawwise.engine.storage.table.TableStore;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DynamicMmapStoreLmdbOps implements IngestionWriteOperations {

    private final DynamicMmapStore store;

    DynamicMmapStoreLmdbOps(DynamicMmapStore store) {
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
            if (result.status() == LmdbInstance.PutStatus.MAP_FULL) {
                throw new SinkRetryableException(
                        "LMDB instance full for table " + tableId + ", retry with new instance");
            }
            throw new SinkFatalException(
                    "LMDB batch put failed for table " + tableId + ": " + result.status());
        }

        try {
            tableStore.addPendingInstance(current);
        } catch (Exception e) {
            tableStore.removePendingInstanceReference(current.instanceId());
            throw new SinkRetryableException(
                    "batchPut succeeded but addPendingInstance failed for table " + tableId, e);
        }

        return List.of(current.instanceId());
    }

    @Override
    public void prepareWithStore(String tableId, List<String> pendingIds, long txnId)
            throws SinkRetryableException, SinkFatalException {
        if (pendingIds == null || pendingIds.isEmpty()) {
            return;
        }
        TableStore tableStore = store.tableStore(tableId);
        for (String instanceId : pendingIds) {
            LmdbInstance instance = tableStore.getPendingInstance(instanceId);
            if (instance == null) {
                throw new SinkFatalException(
                        "Pending instance not found: " + instanceId + " for table " + tableId);
            }
        }
    }

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
        List<String> failed = new ArrayList<>();
        for (String instanceId : pendingIds) {
            try {
                tableStore.rollbackPendingInstance(instanceId);
            } catch (Exception e) {
                log.error("Failed to rollback pending instance {}: {}", instanceId, e.getMessage());
                failed.add(instanceId + "(" + e.getMessage() + ")");
            }
        }
        if (!failed.isEmpty()) {
            throw new SinkFatalException(
                    "Failed to rollback instances: " + failed + " for table " + tableId);
        }
    }

    @Override
    public List<String> getPendingInstanceIds(String tableId, long txnId) {
        TableStore tableStore = store.tableStore(tableId);
        return new ArrayList<>(tableStore.pendingInstanceIds());
    }

    @Override
    public IngestionDataSink.RecoveryResult recoverPendingInstances(String tableId)
            throws SinkRetryableException, SinkFatalException {
        TableStore tableStore = store.tableStore(tableId);
        int recovered = 0;
        int cleaned = 0;
        List<String> errors = new ArrayList<>();

        Set<String> committedIds = new HashSet<>();
        CommitMarker marker = tableStore.commitMarker();
        if (marker != null) {
            try {
                for (CommitMarker.CommitRecord record : marker.scanCommitMarkers()) {
                    committedIds.addAll(record.instanceIds());
                }
            } catch (Exception e) {
                throw new SinkRetryableException(
                        "Failed to scan commit markers for table "
                                + tableId
                                + ", cannot safely determine committed instances - retry later",
                        e);
            }
        }

        List<String> instanceIds = new ArrayList<>(tableStore.pendingInstances().keySet());
        for (String instanceId : instanceIds) {
            if (committedIds.contains(instanceId)) {
                try {
                    tableStore.recoverCommittedInstance(instanceId);
                    recovered++;
                } catch (Exception e) {
                    errors.add(instanceId + ": recover failed: " + e.getMessage());
                }
            } else {
                try {
                    tableStore.rollbackPendingInstance(instanceId);
                    cleaned++;
                } catch (Exception e) {
                    errors.add(instanceId + ": rollback failed: " + e.getMessage());
                }
            }
        }

        return new IngestionDataSink.RecoveryResult(recovered, cleaned, errors);
    }
}
