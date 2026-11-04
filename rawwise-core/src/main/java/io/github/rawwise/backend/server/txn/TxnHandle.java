package io.github.rawwise.backend.server.txn;

import io.github.rawwise.engine.txn.IsolationLevel;
import java.util.Map;
import java.util.Set;

public record TxnHandle(
        long txnId,
        IsolationLevel isolationLevel,
        Set<String> tableIds,
        Map<String, String> nodeAssignments,
        long createdAtMs) {

    public TxnHandle {
        if (txnId <= 0) {
            throw new IllegalArgumentException("txnId must be positive, got: " + txnId);
        }
        if (isolationLevel == null) {
            throw new IllegalArgumentException("isolationLevel must not be null");
        }
        if (tableIds == null || tableIds.isEmpty()) {
            throw new IllegalArgumentException("tableIds must not be null or empty");
        }
        if (nodeAssignments == null) {
            nodeAssignments = Map.of();
        }
        tableIds = Set.copyOf(tableIds);
        nodeAssignments = Map.copyOf(nodeAssignments);
    }
}
