package io.github.rawwise.axiom.pipeline.graph.distributed;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphCentroidRefreshScheduler implements AutoCloseable {

    public static final long DEFAULT_PERIOD_SECONDS = 30L;
    private static final long[] BACKOFF_SECONDS = {5L, 30L, 60L};

    private final GraphCentroidRouter router;
    private final long periodSeconds;
    private final ScheduledExecutorService executor;
    private final AtomicLong consecutiveFailures = new AtomicLong(0);
    private volatile ScheduledFuture<?> task;
    private volatile boolean running;

    public GraphCentroidRefreshScheduler(GraphCentroidRouter router) {
        this(router, DEFAULT_PERIOD_SECONDS);
    }

    public GraphCentroidRefreshScheduler(GraphCentroidRouter router, long periodSeconds) {
        this.router = Objects.requireNonNull(router, "router");
        this.periodSeconds = periodSeconds <= 0 ? DEFAULT_PERIOD_SECONDS : periodSeconds;
        this.executor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "graph-centroid-refresh");
                            t.setDaemon(true);
                            return t;
                        });
    }

    public synchronized void start() {
        if (running) return;
        running = true;
        scheduleNext(periodSeconds);
        log.info("GraphCentroidRefreshScheduler started: periodSeconds={}", periodSeconds);
    }

    public synchronized void stop() {
        if (!running) return;
        running = false;
        if (task != null) task.cancel(false);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("GraphCentroidRefreshScheduler stopped");
    }

    @Override
    public void close() {
        stop();
    }

    private void scheduleNext(long delaySeconds) {
        if (!running) return;
        task = executor.schedule(this::runOnce, delaySeconds, TimeUnit.SECONDS);
    }

    private void runOnce() {
        if (!running) return;
        long nextDelay = periodSeconds;
        try {
            router.refreshFromMetastore();
            consecutiveFailures.set(0);
        } catch (Exception e) {
            long failures = consecutiveFailures.incrementAndGet();
            int idx = (int) Math.min(failures - 1, BACKOFF_SECONDS.length - 1);
            nextDelay = BACKOFF_SECONDS[idx];
            log.warn(
                    "GraphCentroidRouter refresh failed (attempt {}): {} — next retry in {}s",
                    failures,
                    e.getMessage(),
                    nextDelay);
        }
        scheduleNext(nextDelay);
    }
}
