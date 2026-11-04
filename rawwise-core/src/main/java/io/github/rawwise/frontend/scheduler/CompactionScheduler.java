package io.github.rawwise.frontend.scheduler;

import io.github.rawwise.common.catalog.Catalog;
import io.github.rawwise.common.exception.CatalogException;
import io.github.rawwise.engine.compaction.CompactionManager;
import io.github.rawwise.engine.compaction.merger.LatestWinMergeFunction;
import io.github.rawwise.engine.compaction.merger.MergeFunction;
import io.github.rawwise.engine.compaction.strategy.CompactionStrategy;
import io.github.rawwise.engine.compaction.strategy.TieredStrategy;
import io.github.rawwise.model.record.TableId;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompactionScheduler implements AutoCloseable {

    private final ScheduledExecutorService executor;
    private final ExecutorService compactionPool;
    private final CompactionManager compactionManager;
    private final Catalog catalog;
    private final CompactionStrategy strategy;
    private final MergeFunction mergeFunction;
    private final long intervalMs;
    private final int parallelism;

    public CompactionScheduler(
            CompactionManager compactionManager,
            Catalog catalog,
            CompactionStrategy strategy,
            MergeFunction mergeFunction,
            long intervalMs) {
        this(
                compactionManager,
                catalog,
                strategy,
                mergeFunction,
                intervalMs,
                Math.min(4, Runtime.getRuntime().availableProcessors()));
    }

    public CompactionScheduler(
            CompactionManager compactionManager,
            Catalog catalog,
            CompactionStrategy strategy,
            MergeFunction mergeFunction,
            long intervalMs,
            int parallelism) {
        this.compactionManager = compactionManager;
        this.catalog = catalog;
        this.strategy = strategy;
        this.mergeFunction = mergeFunction;
        this.intervalMs = intervalMs;
        this.parallelism = parallelism;
        this.executor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "compaction-scheduler");
                            t.setDaemon(true);
                            return t;
                        });
        this.compactionPool =
                Executors.newFixedThreadPool(
                        parallelism,
                        r -> {
                            Thread t = new Thread(r, "compaction-worker");
                            t.setDaemon(true);
                            return t;
                        });
    }

    public static CompactionScheduler createDefault(
            CompactionManager compactionManager, Catalog catalog, long intervalMs) {
        TieredStrategy strategy = new TieredStrategy();
        LatestWinMergeFunction mergeFn = new LatestWinMergeFunction();
        return new CompactionScheduler(compactionManager, catalog, strategy, mergeFn, intervalMs);
    }

    public void start() {
        executor.scheduleWithFixedDelay(
                this::runCompactionCycle, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info(
                "CompactionScheduler started: interval={}ms, strategy={}, mergeFn={}",
                intervalMs,
                strategy.strategyType(),
                mergeFunction.getClass().getSimpleName());
    }

    private void runCompactionCycle() {
        try {
            List<String> tableIds;
            try {
                tableIds = catalog.listTables("default").stream().map(TableId::tableName).toList();
            } catch (CatalogException e) {
                log.warn("Failed to list tables from catalog", e);
                return;
            }
            if (tableIds.isEmpty()) {
                return;
            }
            List<CompletableFuture<Void>> futures =
                    tableIds.stream()
                            .map(
                                    tableId ->
                                            CompletableFuture.runAsync(
                                                    () -> {
                                                        try {
                                                            CompactionManager.CompactionResult
                                                                    result =
                                                                            compactionManager
                                                                                    .compact(
                                                                                            tableId,
                                                                                            strategy,
                                                                                            mergeFunction);
                                                            if (result.compactedGroupCount() > 0) {
                                                                log.info(
                                                                        "Compaction completed: table={}, groups={}, rows={}",
                                                                        tableId,
                                                                        result
                                                                                .compactedGroupCount(),
                                                                        result.rewrittenRows());
                                                            }
                                                        } catch (Exception e) {
                                                            log.warn(
                                                                    "Compaction failed for table {}",
                                                                    tableId,
                                                                    e);
                                                        }
                                                    },
                                                    compactionPool))
                            .toList();
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
            compactionManager.getGarbageCollector().runGcCycleAllTables();
        } catch (Exception e) {
            log.warn("Compaction cycle failed", e);
        }
    }

    @Override
    public void close() {
        executor.shutdown();
        compactionPool.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("CompactionScheduler did not terminate gracefully, forcing shutdown");
                executor.shutdownNow();
            }
            if (!compactionPool.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("CompactionPool did not terminate gracefully, forcing shutdown");
                compactionPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            compactionPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("CompactionScheduler stopped");
    }
}
