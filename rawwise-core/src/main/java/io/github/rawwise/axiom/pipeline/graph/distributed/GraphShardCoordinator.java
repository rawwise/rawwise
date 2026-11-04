package io.github.rawwise.axiom.pipeline.graph.distributed;

import io.github.rawwise.axiom.pipeline.graph.EdgeFilter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphShardCoordinator implements AutoCloseable {

    public static final int DEFAULT_PHASE1_TIMEOUT_MS = 800;
    public static final int DEFAULT_CROSS_HOP_TIMEOUT_MS = 400;
    public static final int DEFAULT_RESULT_TOP_K = 100;

    private final GraphCentroidRouter router;
    private final ShardedGraphClient client;
    private final ExecutorService executor;
    private final int phase1TimeoutMs;
    private final int crossHopTimeoutMs;
    private final int resultTopK;

    public GraphShardCoordinator(GraphCentroidRouter router, ShardedGraphClient client) {
        this(
                router,
                client,
                DEFAULT_PHASE1_TIMEOUT_MS,
                DEFAULT_CROSS_HOP_TIMEOUT_MS,
                DEFAULT_RESULT_TOP_K);
    }

    public GraphShardCoordinator(
            GraphCentroidRouter router,
            ShardedGraphClient client,
            int phase1TimeoutMs,
            int crossHopTimeoutMs,
            int resultTopK) {
        this.router = Objects.requireNonNull(router, "router");
        this.client = Objects.requireNonNull(client, "client");
        this.phase1TimeoutMs = phase1TimeoutMs <= 0 ? DEFAULT_PHASE1_TIMEOUT_MS : phase1TimeoutMs;
        this.crossHopTimeoutMs =
                crossHopTimeoutMs <= 0 ? DEFAULT_CROSS_HOP_TIMEOUT_MS : crossHopTimeoutMs;
        this.resultTopK = resultTopK <= 0 ? DEFAULT_RESULT_TOP_K : resultTopK;
        int parallelism = Math.max(2, router.shardCount());
        this.executor =
                Executors.newFixedThreadPool(
                        parallelism,
                        r -> {
                            Thread t = new Thread(r, "graph-shard-coordinator");
                            t.setDaemon(true);
                            return t;
                        });
    }

    public Map<String, Double> traverse(Set<String> seedEntities, int maxHops, EdgeFilter filter) {
        if (seedEntities == null || seedEntities.isEmpty() || maxHops <= 0) return Map.of();
        EdgeFilter effective = filter == null ? EdgeFilter.allowAll() : filter;

        Map<ShardId, Set<String>> bySeedShard = groupByShard(seedEntities);

        if (bySeedShard.size() == 1) {
            Map.Entry<ShardId, Set<String>> only = bySeedShard.entrySet().iterator().next();
            return capTopK(
                    client.localTraverse(
                            only.getKey(), only.getValue(), maxHops, effective, phase1TimeoutMs));
        }

        Map<String, Double> merged = new ConcurrentHashMap<>();
        Set<String> visited = ConcurrentHashMap.newKeySet();

        Map<ShardId, Set<String>> currentLevel = new HashMap<>(bySeedShard);
        int remainingHops = maxHops;
        int phaseTimeout = phase1TimeoutMs;

        while (!currentLevel.isEmpty() && remainingHops > 0) {
            Map<ShardId, CompletableFuture<Map<String, Double>>> futures = new HashMap<>();
            for (Map.Entry<ShardId, Set<String>> e : currentLevel.entrySet()) {
                ShardId shard = e.getKey();
                Set<String> seeds = e.getValue();
                if (!client.isHealthy(shard)) {
                    log.warn("Shard {} unhealthy, skipping", shard);
                    continue;
                }
                final int timeout = phaseTimeout;
                CompletableFuture<Map<String, Double>> f =
                        CompletableFuture.supplyAsync(
                                        () ->
                                                client.localTraverse(
                                                        shard, seeds, 1, effective, timeout),
                                        executor)
                                .orTimeout(timeout, TimeUnit.MILLISECONDS)
                                .exceptionally(
                                        ex -> {
                                            if (ex instanceof TimeoutException) {
                                                log.warn(
                                                        "Shard {} traverse timed out after {}ms",
                                                        shard,
                                                        timeout);
                                            } else {
                                                log.warn(
                                                        "Shard {} traverse failed: {}",
                                                        shard,
                                                        ex.getMessage());
                                            }
                                            return Map.of();
                                        });
                futures.put(shard, f);
            }

            Map<String, Double> hopResults = new LinkedHashMap<>();
            for (Map.Entry<ShardId, CompletableFuture<Map<String, Double>>> entry :
                    futures.entrySet()) {
                try {
                    Map<String, Double> partial =
                            entry.getValue().get(phaseTimeout, TimeUnit.MILLISECONDS);
                    for (Map.Entry<String, Double> p : partial.entrySet()) {
                        hopResults.merge(p.getKey(), p.getValue(), Math::max);
                        merged.merge(p.getKey(), p.getValue(), Math::max);
                    }
                } catch (TimeoutException te) {
                    log.warn("Shard {} traverse outer timeout", entry.getKey());
                } catch (Exception ex) {
                    log.warn("Shard {} traverse outer error: {}", entry.getKey(), ex.getMessage());
                }
            }

            remainingHops--;
            if (remainingHops <= 0) break;

            Map<ShardId, Set<String>> nextLevel = new HashMap<>();
            for (String entityId : hopResults.keySet()) {
                if (!visited.add(entityId)) continue;
                ShardId targetShard = router.routeByEntityId(entityId);
                nextLevel.computeIfAbsent(targetShard, k -> new LinkedHashSet<>()).add(entityId);
            }
            currentLevel = nextLevel;
            phaseTimeout = crossHopTimeoutMs;
        }

        for (String seed : seedEntities) {
            merged.putIfAbsent(seed, 1.0);
        }
        return capTopK(merged);
    }

    private Map<ShardId, Set<String>> groupByShard(Set<String> seedEntities) {
        Map<ShardId, Set<String>> grouped = new HashMap<>();
        for (String seed : seedEntities) {
            ShardId sid = router.routeByEntityId(seed);
            grouped.computeIfAbsent(sid, k -> new HashSet<>()).add(seed);
        }
        return grouped;
    }

    private Map<String, Double> capTopK(Map<String, Double> merged) {
        if (merged.size() <= resultTopK) return merged;
        return merged.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue(Comparator.reverseOrder()))
                .limit(resultTopK)
                .collect(
                        LinkedHashMap::new,
                        (m, e) -> m.put(e.getKey(), e.getValue()),
                        LinkedHashMap::putAll);
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
