package io.github.rawwise.engine.txn;

public class TxnContext extends io.github.rawwise.engine.txn.state.TxnContext {

    public TxnContext(
            long txnId, long startTs, IsolationLevel isolationLevel, String tableId, int shardId) {
        super(txnId, startTs, isolationLevel, tableId, shardId);
    }

    public TxnContext(
            long txnId,
            long startTs,
            IsolationLevel isolationLevel,
            String tableId,
            int shardId,
            long baseSnapshotId,
            long routeTableVersion,
            String taskAttemptId) {
        super(
                txnId,
                startTs,
                isolationLevel,
                tableId,
                shardId,
                baseSnapshotId,
                routeTableVersion,
                taskAttemptId);
    }

    public TxnContext(
            long txnId,
            long startTs,
            IsolationLevel isolationLevel,
            java.util.Set<String> tableIds,
            int shardId,
            long baseSnapshotId,
            long routeTableVersion,
            String taskAttemptId,
            TxnTimeoutProfile timeoutProfile) {
        super(
                txnId,
                startTs,
                isolationLevel,
                tableIds,
                shardId,
                baseSnapshotId,
                routeTableVersion,
                taskAttemptId,
                timeoutProfile);
    }

    public TxnContext(
            long txnId,
            io.github.rawwise.engine.txn.isolate.ReadSnapshot readSnapshot,
            IsolationLevel isolationLevel,
            java.util.Set<String> tableIds,
            io.github.rawwise.engine.txn.WriteBuffer writeBuffer,
            io.github.rawwise.engine.txn.TxnReadCache readCache) {
        super(txnId, readSnapshot, isolationLevel, tableIds, writeBuffer, readCache);
    }

    public TxnContext(
            long txnId,
            io.github.rawwise.engine.txn.isolate.ReadSnapshot readSnapshot,
            IsolationLevel isolationLevel,
            java.util.Set<String> tableIds,
            io.github.rawwise.engine.txn.WriteBuffer writeBuffer,
            io.github.rawwise.engine.txn.TxnReadCache readCache,
            java.util.concurrent.atomic.AtomicReference<io.github.rawwise.engine.txn.state.TxnState>
                    state) {
        super(txnId, readSnapshot, isolationLevel, tableIds, writeBuffer, readCache, state);
    }
}
