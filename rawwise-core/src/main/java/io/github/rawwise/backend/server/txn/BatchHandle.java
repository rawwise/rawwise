package io.github.rawwise.backend.server.txn;

import java.util.Map;

public record BatchHandle(
        String batchId,
        String tableId,
        int expectedShardCount,
        Map<Integer, String> shardAssignments,
        long createdAtMs) {

    public BatchHandle {
        if (batchId == null || batchId.isBlank()) {
            throw new IllegalArgumentException("batchId must not be blank");
        }
        if (tableId == null || tableId.isBlank()) {
            throw new IllegalArgumentException("tableId must not be blank");
        }
        if (expectedShardCount < 1) {
            throw new IllegalArgumentException(
                    "expectedShardCount must be >= 1, got: " + expectedShardCount);
        }
        if (shardAssignments == null) {
            shardAssignments = Map.of();
        }
        shardAssignments = Map.copyOf(shardAssignments);
    }
}
