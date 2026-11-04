package io.github.rawwise.backend.server.txn;

import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.state.TxnResult;
import java.util.Set;

public interface ServerTxnCoordinator {

    TxnHandle beginTxn(Set<String> tableIds, IsolationLevel level);

    TxnResult commitTxn(long txnId);

    TxnResult abortTxn(long txnId);

    BatchHandle beginBatch(String tableId, String batchId, int expectedShardCount);

    TxnResult commitBatch(String batchId);

    TxnResult abortBatch(String batchId);

    RecoveryDecision decideBatchRecovery(String idempotencyKey);
}
