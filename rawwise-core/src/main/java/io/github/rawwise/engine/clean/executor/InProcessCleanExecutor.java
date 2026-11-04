package io.github.rawwise.engine.clean.executor;

import io.github.rawwise.engine.clean.CleanManager;
import io.github.rawwise.engine.clean.CleanResult;
import io.github.rawwise.engine.clean.strategy.OrphanFileStrategy;
import io.github.rawwise.engine.clean.strategy.SnapshotExpiryStrategy;
import io.github.rawwise.engine.clean.strategy.TTLExpiryStrategy;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InProcessCleanExecutor implements AutoCloseable {

    private static final long GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS = 30L;
    private static final long FORCED_SHUTDOWN_TIMEOUT_SECONDS = 10L;

    private final CleanManager cleanManager;
    private final ExecutorService executor;
    private volatile boolean closed = false;

    public InProcessCleanExecutor(CleanManager cleanManager) {
        this(cleanManager, createDefaultExecutor());
    }

    public InProcessCleanExecutor(CleanManager cleanManager, ExecutorService executor) {
        this.cleanManager = Objects.requireNonNull(cleanManager, "cleanManager must not be null");
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
    }

    private static ExecutorService createDefaultExecutor() {
        int cpus = Runtime.getRuntime().availableProcessors();
        return new ThreadPoolExecutor(
                cpus,
                cpus * 2,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(256),
                new CleanThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public CompletableFuture<CleanResult> submitOrphanClean(
            String tableId, OrphanFileStrategy strategy) {
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(strategy, "strategy must not be null");
        return submitClean(
                () -> cleanManager.cleanOrphanFiles(tableId, strategy),
                "Orphan clean failed for table " + tableId);
    }

    public CompletableFuture<CleanResult> submitTTLClean(
            String tableId, TTLExpiryStrategy strategy) {
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(strategy, "strategy must not be null");
        return submitClean(
                () -> cleanManager.cleanTTLExpired(tableId, strategy),
                "TTL clean failed for table " + tableId);
    }

    public CompletableFuture<CleanResult> submitSnapshotClean(
            String tableId, SnapshotExpiryStrategy strategy) {
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(strategy, "strategy must not be null");
        return submitClean(
                () -> cleanManager.cleanSnapshots(tableId, strategy),
                "Snapshot clean failed for table " + tableId);
    }

    private CompletableFuture<CleanResult> submitClean(
            CleanAction cleanAction, String errorMessage) {
        if (closed) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("InProcessCleanExecutor has been closed"));
        }
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return cleanAction.execute();
                    } catch (IOException e) {
                        throw new CleanExecutionException(errorMessage, e);
                    } catch (RuntimeException e) {
                        throw new CleanExecutionException(errorMessage, e);
                    }
                },
                executor);
    }

    @FunctionalInterface
    private interface CleanAction {
        CleanResult execute() throws IOException;
    }

    @Override
    public void close() {
        closed = true;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn(
                        "InProcessCleanExecutor did not terminate in {}s, forcing shutdown",
                        GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS);
                executor.shutdownNow();
                if (!executor.awaitTermination(FORCED_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    log.error("InProcessCleanExecutor did not terminate after forced shutdown");
                }
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static class CleanThreadFactory implements ThreadFactory {
        private final AtomicInteger counter = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "rawwise-clean-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    }
}
