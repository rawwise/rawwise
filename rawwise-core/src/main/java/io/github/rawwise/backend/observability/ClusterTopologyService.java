package io.github.rawwise.backend.observability;

import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.ShardAssignment;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.routing.ShardLookupResult;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Provides a cluster-wide topology view combining routing assignments with per-shard metrics.
 *
 * <p>Aggregates {@link RoutingCoordinator} shard ownership information with {@link
 * ShardMetricsRegistry} operational metrics into a single snapshot suitable for monitoring
 * dashboards and cluster management APIs.
 */
@Slf4j
public class ClusterTopologyService {

    private final RoutingCoordinator routingCoordinator;
    private final ShardMetricsRegistry metricsRegistry;

    public ClusterTopologyService(
            RoutingCoordinator routingCoordinator, ShardMetricsRegistry metricsRegistry) {
        this.routingCoordinator = routingCoordinator;
        this.metricsRegistry = metricsRegistry;
    }

    /**
     * Build a snapshot of the current cluster topology.
     *
     * @return cluster topology with per-shard info
     */
    public ClusterTopology getClusterTopology() {
        Set<ShardId> shardIds = routingCoordinator.allShardIds(null);
        Map<Integer, ShardTopologyInfo> shards = new LinkedHashMap<>();

        for (ShardId sid : shardIds) {
            ShardLookupResult owner = routingCoordinator.lookupOwner(sid);
            if (owner == null || !owner.hasAssignment()) {
                continue;
            }
            ShardAssignment assignment = owner.assignment();
            int shardId = sid.value();
            String primaryExecutor = assignment.executorId();

            ShardMetricsRegistry.ShardMetrics metrics = metricsRegistry.getOrCreate(shardId);
            shards.put(
                    shardId,
                    new ShardTopologyInfo(
                            shardId, primaryExecutor, metrics.readCount(), metrics.writeCount()));
        }

        log.debug("ClusterTopology built: shardCount={}", shards.size());
        return new ClusterTopology(shards.size(), shards);
    }

    /**
     * Snapshot of the cluster topology.
     *
     * @param shardCount total number of shards
     * @param shards map from shard ID to topology info
     */
    public record ClusterTopology(int shardCount, Map<Integer, ShardTopologyInfo> shards) {
        public ClusterTopology {
            shards = shards == null ? Map.of() : Map.copyOf(shards);
        }
    }

    /**
     * Per-shard topology information.
     *
     * @param shardId shard identifier
     * @param primaryExecutor primary executor ID
     * @param readCount total read operations
     * @param writeCount total write operations
     */
    public record ShardTopologyInfo(
            int shardId, String primaryExecutor, long readCount, long writeCount) {}
}
