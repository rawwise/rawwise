package io.github.rawwise.engine.vector;

import io.github.rawwise.engine.storage.flush.FlushListener;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VectorFlushListener implements FlushListener, AutoCloseable {

    private static final int MAX_RETRIES = 5;
    private static final int MAX_QUEUE_SIZE = 10_000;

    private final VectorIndex vectorIndex;
    private final VectorMetrics metrics;
    private final ConcurrentLinkedQueue<FailedFlush> failedFlushQueue =
            new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService retryScheduler;

    public VectorFlushListener(VectorIndex vectorIndex) {
        this(vectorIndex, null);
    }

    public VectorFlushListener(VectorIndex vectorIndex, VectorMetrics metrics) {
        this.vectorIndex = vectorIndex;
        this.metrics = metrics;
        this.retryScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "vector-flush-retry");
                            t.setDaemon(true);
                            return t;
                        });
        this.retryScheduler.scheduleWithFixedDelay(
                this::retryFailedFlushes, 60, 60, TimeUnit.SECONDS);
    }

    @Override
    public void onFlushCommitted(
            String tableId, String instanceId, Collection<byte[]> flushedKeys) {
        if (flushedKeys == null || flushedKeys.isEmpty()) return;

        String namespace = tableId;
        List<String> entryKeys = new ArrayList<>(flushedKeys.size());
        for (byte[] keyBytes : flushedKeys) {
            entryKeys.add(new String(keyBytes, StandardCharsets.UTF_8));
        }

        try {
            vectorIndex.flushToCold(namespace, entryKeys);
            log.info(
                    "Vector flush completed for tableId={}, instanceId={}, keyCount={}",
                    tableId,
                    instanceId,
                    entryKeys.size());
        } catch (Exception e) {
            log.error(
                    "Vector flush failed for tableId={}, instanceId={}, keyCount={}, error={}",
                    tableId,
                    instanceId,
                    entryKeys.size(),
                    e.getMessage(),
                    e);
            if (metrics != null) {
                metrics.recordFlushFailure();
            }
            if (failedFlushQueue.size() < MAX_QUEUE_SIZE) {
                failedFlushQueue.add(
                        new FailedFlush(namespace, entryKeys, System.currentTimeMillis(), 0));
            } else {
                log.error(
                        "Vector flush retry queue full (size={}), discarding failed flush for namespace={}, keyCount={}",
                        MAX_QUEUE_SIZE,
                        namespace,
                        entryKeys.size());
            }
            if (metrics == null) throw new RuntimeException(e);
        }
    }

    private void retryFailedFlushes() {
        List<FailedFlush> batch = new ArrayList<>();
        FailedFlush ff;
        while ((ff = failedFlushQueue.poll()) != null) {
            batch.add(ff);
        }
        if (batch.isEmpty()) return;

        log.info("Retrying {} failed vector flushes", batch.size());
        int succeeded = 0;
        for (FailedFlush failed : batch) {
            try {
                vectorIndex.flushToCold(failed.namespace, failed.entryKeys);
                succeeded++;
                log.info(
                        "Vector flush retry succeeded for namespace={}, keyCount={}",
                        failed.namespace,
                        failed.entryKeys.size());
            } catch (Exception e) {
                int nextRetry = failed.retryCount() + 1;
                if (nextRetry < MAX_RETRIES) {
                    if (failedFlushQueue.size() < MAX_QUEUE_SIZE) {
                        failedFlushQueue.add(
                                new FailedFlush(
                                        failed.namespace,
                                        failed.entryKeys,
                                        failed.failedAtMs(),
                                        nextRetry));
                    } else {
                        log.error(
                                "Vector flush retry queue full (size={}), discarding retry for namespace={}, retryCount={}",
                                MAX_QUEUE_SIZE,
                                failed.namespace,
                                nextRetry);
                    }
                    log.debug(
                            "Vector flush retry failed for namespace={}, retryCount={}, will retry later",
                            failed.namespace,
                            nextRetry);
                } else {
                    log.error(
                            "Vector flush retry discarded for namespace={}, exceeded max retries ({})",
                            failed.namespace,
                            MAX_RETRIES);
                }
            }
        }
        log.info("Vector flush retry completed: {}/{} succeeded", succeeded, batch.size());
    }

    public int pendingRetryCount() {
        return failedFlushQueue.size();
    }

    @Override
    public void close() {
        retryScheduler.shutdown();
        try {
            if (!retryScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                retryScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            retryScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    record FailedFlush(String namespace, List<String> entryKeys, long failedAtMs, int retryCount) {}
}
