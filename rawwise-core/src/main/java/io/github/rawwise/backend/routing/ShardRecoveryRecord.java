package io.github.rawwise.backend.routing;

import java.io.Serializable;
import java.util.Set;

public record ShardRecoveryRecord(
        ShardId shardId,
        String previousExecutorId,
        String previousNodeSlotId,
        String previousBasePath,
        String walHdfsDir,
        Set<String> tableIdentifiers,
        RoutingEpoch epoch,
        long lostTimestamp)
        implements Serializable {
    public ShardRecoveryRecord {
        if (shardId == null) {
            throw new IllegalArgumentException("shardId must not be null");
        }
        if (previousExecutorId == null || previousExecutorId.isBlank()) {
            throw new IllegalArgumentException("previousExecutorId must not be blank");
        }
        if (previousNodeSlotId == null || previousNodeSlotId.isBlank()) {
            throw new IllegalArgumentException("previousNodeSlotId must not be blank");
        }
        if (epoch == null) {
            throw new IllegalArgumentException("epoch must not be null");
        }
        tableIdentifiers = tableIdentifiers == null ? Set.of() : Set.copyOf(tableIdentifiers);
    }
}
