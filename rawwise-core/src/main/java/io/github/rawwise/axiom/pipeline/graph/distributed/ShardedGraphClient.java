package io.github.rawwise.axiom.pipeline.graph.distributed;

import io.github.rawwise.axiom.pipeline.entity.Entity;
import io.github.rawwise.axiom.pipeline.graph.DiaRef;
import io.github.rawwise.axiom.pipeline.graph.EdgeFilter;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface ShardedGraphClient {

    Map<String, Double> localTraverse(
            ShardId shard, Set<String> seeds, int maxHops, EdgeFilter filter, int rpcTimeoutMs);

    Optional<Entity> findEntity(ShardId shard, String sampleId, String name, int rpcTimeoutMs);

    Set<DiaRef> lookupDiaIds(ShardId shard, String entityId, int rpcTimeoutMs);

    boolean isHealthy(ShardId shard);
}
