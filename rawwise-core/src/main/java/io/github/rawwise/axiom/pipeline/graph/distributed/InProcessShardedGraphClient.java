package io.github.rawwise.axiom.pipeline.graph.distributed;

import io.github.rawwise.axiom.pipeline.entity.Entity;
import io.github.rawwise.axiom.pipeline.graph.DiaRef;
import io.github.rawwise.axiom.pipeline.graph.EdgeFilter;
import io.github.rawwise.axiom.pipeline.graph.GraphQuery;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InProcessShardedGraphClient implements ShardedGraphClient {

    private final Map<ShardId, GraphQuery> engines;

    public InProcessShardedGraphClient(Map<ShardId, GraphQuery> engines) {
        this.engines = Map.copyOf(Objects.requireNonNull(engines, "engines"));
    }

    @Override
    public Map<String, Double> localTraverse(
            ShardId shard, Set<String> seeds, int maxHops, EdgeFilter filter, int rpcTimeoutMs) {
        ensureTimeout(rpcTimeoutMs);
        GraphQuery engine = engines.get(shard);
        if (engine == null) return Map.of();
        try {
            return engine.traverse(seeds, maxHops, filter);
        } catch (Exception e) {
            log.warn(
                    "InProcessShardedGraphClient.localTraverse shard={} failed: {}",
                    shard,
                    e.getMessage());
            return Map.of();
        }
    }

    @Override
    public Optional<Entity> findEntity(
            ShardId shard, String sampleId, String name, int rpcTimeoutMs) {
        ensureTimeout(rpcTimeoutMs);
        GraphQuery engine = engines.get(shard);
        if (engine == null) return Optional.empty();
        try {
            return engine.findEntity(sampleId, name);
        } catch (Exception e) {
            log.warn(
                    "InProcessShardedGraphClient.findEntity shard={} failed: {}",
                    shard,
                    e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public Set<DiaRef> lookupDiaIds(ShardId shard, String entityId, int rpcTimeoutMs) {
        ensureTimeout(rpcTimeoutMs);
        GraphQuery engine = engines.get(shard);
        if (engine == null) return Set.of();
        try {
            return engine.lookupDiaIds(entityId);
        } catch (Exception e) {
            log.warn(
                    "InProcessShardedGraphClient.lookupDiaIds shard={} failed: {}",
                    shard,
                    e.getMessage());
            return Set.of();
        }
    }

    @Override
    public boolean isHealthy(ShardId shard) {
        return engines.containsKey(shard);
    }

    private static void ensureTimeout(int rpcTimeoutMs) {
        if (rpcTimeoutMs <= 0) {
            throw new CompletionException(new TimeoutException("rpcTimeoutMs=" + rpcTimeoutMs));
        }
    }
}
