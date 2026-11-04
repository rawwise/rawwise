package io.github.rawwise.backend.routing;

import java.io.Serializable;
import java.util.List;

public record RecoveryTaskMessage(
        String targetExecutorId, List<ShardRecoveryRecord> shards, String basePath, String storeId)
        implements Serializable {
    public RecoveryTaskMessage {
        if (targetExecutorId == null || targetExecutorId.isBlank()) {
            throw new IllegalArgumentException("targetExecutorId must not be blank");
        }
        if (shards == null || shards.isEmpty()) {
            throw new IllegalArgumentException("shards must not be null or empty");
        }
        if (basePath == null || basePath.isBlank()) {
            throw new IllegalArgumentException("basePath must not be blank");
        }
        if (storeId == null || storeId.isBlank()) {
            throw new IllegalArgumentException("storeId must not be blank");
        }
        shards = List.copyOf(shards);
    }
}
