package io.github.rawwise.backend.routing;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface ShardRoutingTable {
    ShardLookupResult lookup(ShardId shardId);

    Map<ShardId, ShardLookupResult> lookupBatch(Set<ShardId> shardIds);

    void registerExecutor(String executorId, NodeSlot nodeSlot);

    void markExecutorReady(String executorId);

    void unregisterExecutorAndReassign(String executorId);

    default List<ShardAssignment> unregisterExecutorAndCollectAssignments(String executorId) {
        unregisterExecutorAndReassign(executorId);
        return List.of();
    }

    void assignShard(ShardAssignment assignment);

    void reassignShard(ShardId shardId);

    default void reassignShardForRecovery(ShardId shardId) {
        throw new UnsupportedOperationException("reassignShardForRecovery not implemented");
    }

    Collection<ShardAssignment> assignments();

    Collection<ShardLookupResult> routings();

    Optional<NodeSlot> nodeSlotOf(String executorId);

    Optional<NodeSlot> preferredSlotOf(ShardId shardId);

    default java.util.Set<String> registeredExecutors() {
        return java.util.Set.of();
    }

    default long getLastHeartbeatMs(String executorId) {
        return 0L;
    }
}
