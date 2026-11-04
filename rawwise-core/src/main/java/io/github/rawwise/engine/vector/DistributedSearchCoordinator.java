package io.github.rawwise.engine.vector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DistributedSearchCoordinator implements AutoCloseable {

    private static final int MAX_RETRIES = 3;
    private static final long RETRY_BASE_DELAY_MS = 50;
    private static final int CB_FAILURE_THRESHOLD = 3;
    private static final long CB_HALF_OPEN_AFTER_MS = 10_000;

    private final ExecutorService searchExecutor;
    private final long searchTimeoutMs;
    private final ConcurrentHashMap<String, ShardCircuitBreaker> breakers =
            new ConcurrentHashMap<>();
    private final Map<String, ShardClient> shardClients;
    private final long defaultTimeoutMs;

    public DistributedSearchCoordinator(
            Map<String, ShardClient> shardClients, long defaultTimeoutMs) {
        this.shardClients = Map.copyOf(shardClients);
        this.defaultTimeoutMs = defaultTimeoutMs;
        this.searchTimeoutMs = defaultTimeoutMs;
        int parallelism = Math.max(2, shardClients.size());
        this.searchExecutor =
                Executors.newFixedThreadPool(
                        parallelism,
                        r -> {
                            Thread t = new Thread(r, "distributed-vector-search");
                            t.setDaemon(false);
                            return t;
                        });
    }

    public int shardCount() {
        return shardClients.size();
    }

    public List<VectorSearchResult> distributedSearch(
            String tableId, float[] queryEmbedding, int topK) {
        return distributedSearch(tableId, queryEmbedding, topK, defaultTimeoutMs);
    }

    public List<VectorSearchResult> distributedSearch(
            String tableId, float[] queryEmbedding, int topK, long timeoutMs) {
        return distributedSearch(tableId, shardClients, queryEmbedding, topK, timeoutMs);
    }

    public List<VectorSearchResult> distributedSearch(
            String tableId,
            float[] queryEmbedding,
            int topK,
            long timeoutMs,
            int nprobe,
            IvfRouter router) {
        if (router.shardCount() == 0 || nprobe >= shardClients.size()) {
            return distributedSearch(tableId, shardClients, queryEmbedding, topK, timeoutMs);
        }
        List<String> targetShards = router.route(queryEmbedding, nprobe);
        Map<String, ShardClient> filtered = new HashMap<>();
        for (String shardId : targetShards) {
            ShardClient client = shardClients.get(shardId);
            if (client != null) filtered.put(shardId, client);
        }
        if (filtered.isEmpty()) {
            return distributedSearch(tableId, shardClients, queryEmbedding, topK, timeoutMs);
        }
        return distributedSearch(tableId, filtered, queryEmbedding, topK, timeoutMs);
    }

    public List<VectorSearchResult> distributedSearch(
            String tableId,
            Map<String, ShardClient> clients,
            float[] queryEmbedding,
            int topK,
            long timeoutMs) {

        if (clients.isEmpty()) return List.of();

        Map<String, CompletableFuture<List<SearchCandidate>>> futures = new HashMap<>();
        List<String> skippedShards = new ArrayList<>();

        for (var entry : clients.entrySet()) {
            String shardId = entry.getKey();
            ShardClient client = entry.getValue();
            ShardCircuitBreaker cb =
                    breakers.computeIfAbsent(shardId, k -> new ShardCircuitBreaker());
            if (!cb.allowRequest()) {
                log.warn("Circuit breaker open for shard {}, skipping", shardId);
                skippedShards.add(shardId);
                continue;
            }

            if (!client.isAvailable(shardId)) {
                log.warn("Shard {} unavailable, skipping", shardId);
                cb.onFailure();
                skippedShards.add(shardId);
                continue;
            }

            CompletableFuture<List<SearchCandidate>> future =
                    CompletableFuture.supplyAsync(
                                    () ->
                                            searchWithRetry(
                                                    tableId,
                                                    client,
                                                    queryEmbedding,
                                                    topK,
                                                    shardId,
                                                    cb),
                                    searchExecutor)
                            .orTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                            .exceptionally(
                                    ex -> {
                                        if (ex instanceof java.util.concurrent.TimeoutException) {
                                            log.warn(
                                                    "Shard {} search timed out after {}ms",
                                                    shardId,
                                                    timeoutMs);
                                        } else {
                                            log.warn(
                                                    "Shard {} search failed: {}",
                                                    shardId,
                                                    ex.getMessage());
                                        }
                                        breakers.get(shardId).onFailure();
                                        return List.of();
                                    });

            futures.put(shardId, future);
        }

        if (!skippedShards.isEmpty()) {
            log.info("Skipped {} unavailable shards: {}", skippedShards.size(), skippedShards);
        }

        List<SearchCandidate> allCandidates = new ArrayList<>();
        for (var entry : futures.entrySet()) {
            try {
                List<SearchCandidate> candidates =
                        entry.getValue().get(timeoutMs, TimeUnit.MILLISECONDS);
                allCandidates.addAll(candidates);
            } catch (TimeoutException e) {
                log.warn("Shard {} search timed out after {}ms", entry.getKey(), timeoutMs);
                breakers.get(entry.getKey()).onFailure();
            } catch (Exception e) {
                log.warn("Shard {} search failed", entry.getKey(), e);
                breakers.get(entry.getKey()).onFailure();
            }
        }

        allCandidates.sort(Comparator.comparingDouble(SearchCandidate::approximateDistance));

        List<VectorSearchResult> results = new ArrayList<>();
        for (SearchCandidate c : allCandidates.stream().limit(topK).toList()) {
            results.add(new VectorSearchResult(c.entryKey(), "", c.approximateDistance()));
        }
        return results;
    }

    public List<VectorSearchResult> distributedSearch(
            Map<String, ShardInternalIndex> shardIndices,
            float[] queryEmbedding,
            int topK,
            IvfRouter router,
            int nprobe) {

        List<String> targetShards = router.route(queryEmbedding, topK, nprobe);
        if (targetShards.isEmpty()) return List.of();

        Map<String, CompletableFuture<List<SearchCandidate>>> futures = new HashMap<>();
        for (String shardId : targetShards) {
            ShardCircuitBreaker cb =
                    breakers.computeIfAbsent(shardId, k -> new ShardCircuitBreaker());
            if (!cb.allowRequest()) {
                log.warn("Circuit breaker open for shard {}, skipping", shardId);
                continue;
            }

            ShardInternalIndex idx = shardIndices.get(shardId);
            if (idx == null) {
                cb.onFailure();
                continue;
            }

            futures.put(
                    shardId,
                    CompletableFuture.supplyAsync(
                            () -> searchWithRetry(idx, queryEmbedding, topK, shardId, cb),
                            searchExecutor));
        }

        List<SearchCandidate> allCandidates = new ArrayList<>();
        for (var entry : futures.entrySet()) {
            try {
                List<SearchCandidate> candidates =
                        entry.getValue().get(searchTimeoutMs, TimeUnit.MILLISECONDS);
                allCandidates.addAll(candidates);
            } catch (TimeoutException e) {
                log.warn("Shard {} search timed out after {}ms", entry.getKey(), searchTimeoutMs);
                breakers.get(entry.getKey()).onFailure();
            } catch (Exception e) {
                log.warn("Shard {} search failed", entry.getKey(), e);
                breakers.get(entry.getKey()).onFailure();
            }
        }

        allCandidates.sort(Comparator.comparingDouble(SearchCandidate::approximateDistance));

        List<VectorSearchResult> results = new ArrayList<>();
        for (SearchCandidate c : allCandidates.stream().limit(topK).toList()) {
            results.add(new VectorSearchResult(c.entryKey(), "", c.approximateDistance()));
        }
        return results;
    }

    private List<SearchCandidate> searchWithRetry(
            ShardInternalIndex idx,
            float[] query,
            int topK,
            String shardId,
            ShardCircuitBreaker cb) {

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                List<SearchCandidate> result = idx.search(query, topK);
                cb.onSuccess();
                return result;
            } catch (Exception e) {
                if (attempt < MAX_RETRIES - 1) {
                    long delay = RETRY_BASE_DELAY_MS * (1L << attempt);
                    log.debug(
                            "Shard {} search attempt {}/{} failed, retrying in {}ms",
                            shardId,
                            attempt + 1,
                            MAX_RETRIES,
                            delay);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    log.warn("Shard {} search failed after {} retries", shardId, MAX_RETRIES);
                }
            }
        }
        cb.onFailure();
        return List.of();
    }

    private List<SearchCandidate> searchWithRetry(
            String tableId,
            ShardClient client,
            float[] query,
            int topK,
            String shardId,
            ShardCircuitBreaker cb) {

        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                List<SearchCandidate> result = client.search(tableId, shardId, query, topK);
                cb.onSuccess();
                return result;
            } catch (Exception e) {
                if (attempt < MAX_RETRIES - 1) {
                    long delay = RETRY_BASE_DELAY_MS * (1L << attempt);
                    log.debug(
                            "Shard {} search attempt {}/{} failed, retrying in {}ms",
                            shardId,
                            attempt + 1,
                            MAX_RETRIES,
                            delay);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    log.warn("Shard {} search failed after {} retries", shardId, MAX_RETRIES);
                }
            }
        }
        cb.onFailure();
        return List.of();
    }

    @Override
    public void close() {
        searchExecutor.shutdown();
        try {
            if (!searchExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                searchExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            searchExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static class ShardCircuitBreaker {
        private volatile State state = State.CLOSED;
        private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
        private final AtomicLong openedAt = new AtomicLong(0);

        enum State {
            CLOSED,
            OPEN,
            HALF_OPEN
        }

        synchronized boolean allowRequest() {
            switch (state) {
                case CLOSED -> {
                    return true;
                }
                case OPEN -> {
                    if (System.currentTimeMillis() - openedAt.get() > CB_HALF_OPEN_AFTER_MS) {
                        state = State.HALF_OPEN;
                        return true;
                    }
                    return false;
                }
                case HALF_OPEN -> {
                    return true;
                }
            }
            return true;
        }

        synchronized void onSuccess() {
            consecutiveFailures.set(0);
            state = State.CLOSED;
        }

        synchronized void onFailure() {
            int failures = consecutiveFailures.incrementAndGet();
            if (failures >= CB_FAILURE_THRESHOLD && state != State.OPEN) {
                state = State.OPEN;
                openedAt.set(System.currentTimeMillis());
                log.warn("Circuit breaker OPEN ({} consecutive failures)", failures);
            } else if (state == State.HALF_OPEN) {
                state = State.OPEN;
                openedAt.set(System.currentTimeMillis());
            }
        }
    }
}
