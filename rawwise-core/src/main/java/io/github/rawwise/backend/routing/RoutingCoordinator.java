package io.github.rawwise.backend.routing;

import java.util.Map;
import java.util.Set;

public interface RoutingCoordinator {
    void onExecutorUp(String executorId, NodeSlot nodeSlot);

    void onExecutorReady(String executorId);

    void onExecutorDown(String executorId);

    ShardLookupResult lookupOwner(ShardId shardId);

    Map<ShardId, ShardLookupResult> lookupOwners(Set<ShardId> shardIds);

    void assignShard(ShardAssignment assignment);

    default void reassignShard(ShardId shardId) {}

    default void reassignShardForRecovery(ShardId shardId) {
        throw new UnsupportedOperationException("reassignShardForRecovery not implemented");
    }

    /**
     * P4: 返回该表所有已知的 shardId 集合。
     *
     * <p>用于 range-based routing 的 {@link #lookupOwnerForKey} 默认实现遍历所有 shard。 默认返回空 Set（hash 路由模式下无
     * range 信息）。
     */
    default Set<ShardId> allShardIds(String tableId) {
        return Set.of();
    }

    /**
     * P4: 基于 order-preserving 编码的 key 找到对应 shard owner（range-based routing）。
     *
     * <p>语义：
     *
     * <ul>
     *   <li>遍历该表所有 shard，找到第一个 {@link ShardAssignment#coversKey(byte[])} 返回 true 的 shard
     *   <li>如果 key 不在任何 shard 的双层 range（lmdb ∪ parquet）内 → 返回 null（语义：NOT_FOUND）
     *   <li>调用方据此判定：null → {@code FastPathResult.notFound()}，不降级 Spark SQL
     * </ul>
     *
     * <p>默认实现是线性扫描；子类可重写为基于 TreeMap 的二分查找优化（split-threshold 触发后）。
     *
     * @param tableId 表标识（用于过滤 shard 集合，目前默认实现忽略，子类可按表拆分）
     * @param encodedKey OrderPreservingKeyEncoder 编码后的 key（不能为 null）
     * @return 命中 shard 的 ShardLookupResult；未命中任何 range 返回 null
     */
    default ShardLookupResult lookupOwnerForKey(String tableId, byte[] encodedKey) {
        if (encodedKey == null) return null;
        for (ShardId shardId : allShardIds(tableId)) {
            ShardLookupResult r = lookupOwner(shardId);
            if (r != null && r.hasAssignment() && r.assignment().coversKey(encodedKey)) {
                return r;
            }
        }
        return null;
    }

    /** Look up the replica node for a given shard. Returns null if no replica is configured. */
    default ShardLookupResult lookupReplica(ShardId shardId) {
        return null;
    }

    /** Get all registered executor IDs. */
    default Set<String> getRegisteredExecutorIds() {
        return Set.of();
    }

    /** Get the last heartbeat timestamp for an executor. */
    default long getLastHeartbeatMs(String executorId) {
        return 0L;
    }
}
