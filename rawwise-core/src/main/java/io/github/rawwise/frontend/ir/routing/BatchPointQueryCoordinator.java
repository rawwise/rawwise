package io.github.rawwise.frontend.ir.routing;

import io.github.rawwise.backend.routing.ShardResolver;
import io.github.rawwise.frontend.ir.*;
import java.util.*;
import java.util.concurrent.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BatchPointQueryCoordinator implements AutoCloseable {

    private final ExecutorService executor =
            new ThreadPoolExecutor(
                    4,
                    8,
                    60L,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(100),
                    new ThreadPoolExecutor.CallerRunsPolicy());
    private final FastPathExecutor fastPathExecutor;
    private final ShardResolver shardResolver;

    public BatchPointQueryCoordinator(
            FastPathExecutor fastPathExecutor, ShardResolver shardResolver) {
        this.fastPathExecutor = fastPathExecutor;
        this.shardResolver = shardResolver;
    }

    public BatchPointQueryCoordinator(FastPathExecutor fastPathExecutor) {
        this.fastPathExecutor = fastPathExecutor;
        this.shardResolver = null;
    }

    public BatchPointQueryResult executeCrossShard(
            TableMetaIR tableMeta, List<byte[]> allKeys, List<String> projection, long snapshotTs) {
        String tableId = tableMeta.tableIdentifier();
        List<ShardFailure> preFailures = new ArrayList<>();
        List<ShardBatch> batches = groupByShard(allKeys, tableId, preFailures);
        if (batches.isEmpty() && preFailures.isEmpty()) {
            if (!allKeys.isEmpty()) {
                preFailures.add(
                        new ShardFailure(
                                -1,
                                "NO_ROUTE",
                                "no shards routed for " + allKeys.size() + " keys"));
                return BatchPointQueryResult.partialFailure(List.of(), preFailures);
            }
            return BatchPointQueryResult.success(List.of());
        }

        List<CompletableFuture<ShardResult>> futures = new ArrayList<>();
        for (ShardBatch batch : batches) {
            futures.add(
                    CompletableFuture.supplyAsync(
                            () -> {
                                BatchPointQueryIR bpq =
                                        new BatchPointQueryIR(
                                                tableMeta,
                                                null,
                                                snapshotTs,
                                                null,
                                                batch.keys(),
                                                projection,
                                                batch.executorId(),
                                                batch.shardId(),
                                                batch.epoch());
                                FastPathResult result = fastPathExecutor.execute(bpq);
                                return new ShardResult(batch.shardId(), result);
                            },
                            executor));
        }

        List<byte[]> merged = new ArrayList<>();
        List<ShardFailure> failures = new ArrayList<>(preFailures);
        for (int i = 0; i < futures.size(); i++) {
            int shardId = batches.get(i).shardId();
            try {
                ShardResult shardResult = futures.get(i).get(5, TimeUnit.SECONDS);
                FastPathResult fpResult = shardResult.result();
                switch (fpResult) {
                    case FastPathResult.Hit h -> merged.add(h.data());
                    case FastPathResult.BatchHit bh -> {
                        for (var r : bh.results()) {
                            if (r.found()) merged.add(r.data());
                        }
                    }
                    case FastPathResult.Miss m -> {}
                    case FastPathResult.StaleEpoch se ->
                            failures.add(
                                    new ShardFailure(
                                            shardId,
                                            "STALE_EPOCH",
                                            "expected="
                                                    + se.expectedEpoch()
                                                    + " actual="
                                                    + se.actualEpoch()));
                    case FastPathResult.Unreachable u ->
                            failures.add(new ShardFailure(shardId, "UNREACHABLE", u.executorId()));
                    case FastPathResult.FallbackRequired fr ->
                            failures.add(
                                    new ShardFailure(shardId, "FALLBACK_REQUIRED", fr.reason()));
                    case FastPathResult.Updated u ->
                            failures.add(
                                    new ShardFailure(
                                            shardId,
                                            "UNEXPECTED_UPDATED",
                                            "rows=" + u.rowsAffected()));
                    case FastPathResult.Deleted d ->
                            failures.add(
                                    new ShardFailure(
                                            shardId,
                                            "UNEXPECTED_DELETED",
                                            "rows=" + d.rowsAffected()));
                    case FastPathResult.WriteConflict wc ->
                            failures.add(new ShardFailure(shardId, "WRITE_CONFLICT", wc.reason()));
                    case FastPathResult.NotFound nf ->
                            failures.add(new ShardFailure(shardId, "NOT_FOUND", "not found"));
                }
            } catch (TimeoutException e) {
                failures.add(new ShardFailure(shardId, "TIMEOUT", e.getMessage()));
            } catch (Exception e) {
                failures.add(new ShardFailure(shardId, "EXECUTION_ERROR", e.getMessage()));
            }
        }

        if (!failures.isEmpty()) {
            return BatchPointQueryResult.partialFailure(merged, failures);
        }
        return BatchPointQueryResult.success(merged);
    }

    private List<ShardBatch> groupByShard(
            List<byte[]> keys, String tableId, List<ShardFailure> failures) {
        if (shardResolver == null) {
            log.warn("ShardResolver not wired, using hash-based routing");
            return groupByShardLegacy(keys);
        }
        Map<Integer, List<byte[]>> shardGroups = new LinkedHashMap<>();
        for (byte[] key : keys) {
            int shardId = shardResolver.keyToShard(key);
            shardGroups.computeIfAbsent(shardId, k -> new ArrayList<>()).add(key);
        }

        List<ShardBatch> batches = new ArrayList<>();
        for (var entry : shardGroups.entrySet()) {
            int shardId = entry.getKey();
            String executorId = fastPathExecutor.lookupExecutor(shardId);
            if (executorId != null) {
                long epoch = fastPathExecutor.resolveShardEpoch(tableId, shardId);
                batches.add(new ShardBatch(shardId, executorId, entry.getValue(), epoch));
            } else {
                log.warn("No executor found for shardId={}", shardId);
                failures.add(
                        new ShardFailure(
                                shardId,
                                "NO_EXECUTOR",
                                "no executor registered for shard " + shardId));
            }
        }
        return batches;
    }

    private List<ShardBatch> groupByShardLegacy(List<byte[]> keys) {
        var executors = fastPathExecutor.getRegisteredExecutorIds();
        if (executors.isEmpty()) {
            return List.of();
        }
        List<String> executorList = new ArrayList<>(executors);
        Map<Integer, List<byte[]>> executorGroups = new LinkedHashMap<>();
        for (byte[] key : keys) {
            int hash = Arrays.hashCode(key);
            int idx = (hash == Integer.MIN_VALUE) ? 0 : (Math.abs(hash) % executorList.size());
            executorGroups.computeIfAbsent(idx, k -> new ArrayList<>()).add(key);
        }

        List<ShardBatch> batches = new ArrayList<>();
        for (int i = 0; i < executorList.size(); i++) {
            List<byte[]> group = executorGroups.get(i);
            if (group != null && !group.isEmpty()) {
                String executorId = executorList.get(i);
                batches.add(new ShardBatch(i, executorId, group, 0L));
            }
        }
        return batches;
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
