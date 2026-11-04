package io.github.rawwise.engine.txn;

import io.github.rawwise.engine.txn.isolate.RangeReadSet;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnState;
import io.github.rawwise.engine.txn.wal.TxnWALRecord;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Bridges {@code state.TxnContext} for per-key OCC via {@link
 * ShardVersionTracker#validateAndUpdateAtomic}.
 *
 * <p>Populates the {@link OperationSet} with write entries (from write intents) and read entries
 * (from baseVersions + rangeReads), enabling the atomic validate-and-update path to perform
 * write-write, read-write, and phantom conflict detection in a single commit-lock scope.
 */
public final class TxnContextBridge {

    private TxnContextBridge() {}

    /**
     * Prepares a {@code TxnContext} with {@link OperationSet} populated from write intents, read
     * versions, and range reads.
     *
     * @param stateCtx the transaction context (mutated in-place)
     * @param writeIntents the write intents for this transaction
     * @param baseVersions per-key read versions (may be null/empty)
     * @param rangeReadSet range reads for phantom detection (may be null)
     * @return the same stateCtx, ready for validateAndUpdateAtomic
     */
    public static TxnContext bridge(
            TxnContext stateCtx,
            java.util.List<TxnWALRecord.TxnWriteIntent> writeIntents,
            Map<String, Map<ByteBuffer, Long>> baseVersions,
            RangeReadSet rangeReadSet) {
        if (stateCtx.currentState() == TxnState.VALIDATING) stateCtx.markPreparing();

        OperationSet opSet = stateCtx.operationSet();

        for (TxnWALRecord.TxnWriteIntent intent : writeIntents) {
            byte[] keyBytes = intent.keyBytes();
            OperationSet.WriteType opWriteType =
                    switch (intent.writeType()) {
                        case UPSERT, INSERT -> OperationSet.WriteType.INSERT;
                        case UPDATE, REPLACE -> OperationSet.WriteType.UPDATE;
                        case DELETE -> OperationSet.WriteType.DELETE;
                    };
            opSet.recordWrite(keyBytes, opWriteType, intent.baseVersion());
        }

        if (baseVersions != null) {
            for (var tableEntry : baseVersions.entrySet()) {
                for (var kv : tableEntry.getValue().entrySet()) {
                    byte[] keyBytes = new byte[kv.getKey().remaining()];
                    kv.getKey().duplicate().get(keyBytes);
                    opSet.recordRead(keyBytes, kv.getValue());
                }
            }
        }

        if (rangeReadSet != null && !rangeReadSet.isEmpty()) {
            for (RangeReadSet.RangeReadEntry range : rangeReadSet.entries()) {
                opSet.recordRangeRead(range.startKey(), range.endKey(), range.readAtSnapshotTs());
            }
        }

        return stateCtx;
    }
}
