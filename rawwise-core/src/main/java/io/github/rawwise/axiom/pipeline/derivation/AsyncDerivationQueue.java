package io.github.rawwise.axiom.pipeline.derivation;

import io.github.rawwise.engine.storage.KnowledgeStore;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * Offloads the heavy derivation chain (Chunk → View → Embedding → JVector → Keyword, plus Cognitive
 * Stage) from the synchronous ingest path onto Java 21 virtual threads.
 *
 * <p>The Raw → LMDB write stays synchronous (strong consistency); only the post-write derivation is
 * enqueued here. Backpressure is enforced via a bounded semaphore: when all permits are taken
 * (queue full), the {@code Caller Runs Policy} semantics kick in and the task runs inline on the
 * caller (ingest) thread rather than being dropped.
 *
 * <p>Progress is tracked per table via a watermark persisted in the {@link Knowledge Store} (key =
 * {@code derivation:watermark:{table Id}}) so that consumers can observe how far async derivation
 * has lagged behind the synchronous write path.
 */
@Slf4j
public class AsyncDerivationQueue implements AutoCloseable {

    public static final int DEFAULT_QUEUE_SIZE = 10000;
    static final String WATERMARK_KEY_PREFIX = "derivation:watermark:";

    private final ExecutorService virtualThreadExecutor;
    private final Semaphore concurrencyPermits;
    private final KnowledgeStore knowledgeStore;
    private final DerivationLoadMonitor loadMonitor;
    private final int maxPending;
    private final ConcurrentHashMap<String, String> watermarkCache = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    public AsyncDerivationQueue(int queueSize, KnowledgeStore knowledgeStore) {
        this(queueSize, knowledgeStore, null, queueSize);
    }

    public AsyncDerivationQueue(KnowledgeStore knowledgeStore) {
        this(DEFAULT_QUEUE_SIZE, knowledgeStore);
    }

    public AsyncDerivationQueue(
            int queueSize,
            KnowledgeStore knowledgeStore,
            DerivationLoadMonitor loadMonitor,
            int maxPending) {
        int size = queueSize > 0 ? queueSize : DEFAULT_QUEUE_SIZE;
        this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.concurrencyPermits = new Semaphore(size);
        this.knowledgeStore = knowledgeStore;
        this.loadMonitor = loadMonitor != null ? loadMonitor : new DerivationLoadMonitor();
        this.maxPending = maxPending > 0 ? maxPending : size;
        log.info(
                "AsyncDerivationQueue initialized: queueSize={}, maxPending={}, knowledgeStore={}, loadMonitor={}",
                size,
                this.maxPending,
                knowledgeStore != null ? "present" : "null",
                this.loadMonitor != null ? "present" : "null");
    }

    /**
     * Bounded-concurrency constructor: {@code queueSize} caps the pending queue while {@code
     * concurrency} caps the number of derivation tasks running in parallel. Capping concurrency
     * keeps the burst of embedding / LLM calls issued during bulk ingest below the upstream
     * provider's rate limit (avoiding 429-driven circuit breaker trips).
     */
    public AsyncDerivationQueue(
            int queueSize,
            int concurrency,
            KnowledgeStore knowledgeStore,
            DerivationLoadMonitor loadMonitor) {
        int pending = queueSize > 0 ? queueSize : DEFAULT_QUEUE_SIZE;
        int permits = concurrency > 0 ? concurrency : DEFAULT_QUEUE_SIZE;
        this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.concurrencyPermits = new Semaphore(permits);
        this.knowledgeStore = knowledgeStore;
        this.loadMonitor = loadMonitor != null ? loadMonitor : new DerivationLoadMonitor();
        this.maxPending = pending;
        log.info(
                "AsyncDerivationQueue initialized: queueSize={}, concurrency={}, knowledgeStore={}, loadMonitor={}",
                pending,
                permits,
                knowledgeStore != null ? "present" : "null",
                this.loadMonitor != null ? "present" : "null");
    }

    /**
     * Submit a derivation task. When the queue is full (permits exhausted) the task is executed
     * synchronously on the caller thread (Caller Runs Policy backpressure). After {@link #close()}
     * the queue also runs tasks inline on the caller thread so that in-flight writes during
     * shutdown still get derived.
     */
    public void submit(Runnable derivationTask) {
        if (closed) {
            log.debug("AsyncDerivationQueue closed, running derivation in caller thread");
            runSafely(derivationTask);
            return;
        }
        if (!concurrencyPermits.tryAcquire()) {
            log.warn(
                    "AsyncDerivationQueue backpressure: permits exhausted, running derivation in caller thread");
            runSafely(derivationTask);
            return;
        }
        virtualThreadExecutor.submit(
                () -> {
                    try {
                        runSafely(derivationTask);
                    } finally {
                        concurrencyPermits.release();
                    }
                });
    }

    /**
     * Submit a DerivationTask with backpressure-aware degradation.
     *
     * <p>When the system is overloaded (as determined by {@link DerivationLoadMonitor}), or when
     * the pending queue is full, the task is degraded to its lightweight path ({@link
     * DerivationTask#runLightweight()}) and executed inline on the caller thread. Otherwise the
     * full derivation path ({@link DerivationTask#runFull()}) runs on a virtual thread.
     */
    public void submit(DerivationTask task) {
        if (closed) {
            log.debug(
                    "AsyncDerivationQueue closed, running derivation lightweight in caller thread");
            runDerivationSafely(task, true);
            return;
        }
        boolean overloaded = loadMonitor.isOverloaded();
        if (overloaded) {
            log.debug("AsyncDerivationQueue: system overloaded, degrading to lightweight");
            runDerivationSafely(task, true);
            return;
        }
        if (!concurrencyPermits.tryAcquire()) {
            log.warn(
                    "AsyncDerivationQueue backpressure: permits exhausted, degrading to lightweight");
            runDerivationSafely(task, true);
            return;
        }
        virtualThreadExecutor.submit(
                () -> {
                    try {
                        runDerivationSafely(task, false);
                    } finally {
                        concurrencyPermits.release();
                    }
                });
    }

    private void runDerivationSafely(DerivationTask task, boolean lightweight) {
        try {
            if (lightweight) {
                task.runLightweight();
            } else {
                task.runFull();
            }
        } catch (Throwable e) {
            log.error(
                    "Async derivation task FAILED (caught Throwable): type={} msg={}",
                    e.getClass().getName(),
                    e.getMessage(),
                    e);
        }
    }

    private static void runSafely(Runnable task) {
        try {
            task.run();
        } catch (Throwable e) {
            log.error(
                    "Async derivation task FAILED (caught Throwable): type={} msg={}",
                    e.getClass().getName(),
                    e.getMessage(),
                    e);
        }
    }

    /**
     * @return the last derived record key for the table, or {@code null} if no watermark has been
     *     recorded yet. Served from an in-memory cache first, then the Knowledge Store.
     */
    public String getWatermark(String tableId) {
        if (tableId == null) return null;
        String cached = watermarkCache.get(tableId);
        if (cached != null) return cached;
        if (knowledgeStore == null) return null;
        try {
            byte[] key = (WATERMARK_KEY_PREFIX + tableId).getBytes(StandardCharsets.UTF_8);
            Optional<byte[]> val = knowledgeStore.get(key);
            if (val.isPresent()) {
                String wm = new String(val.get(), StandardCharsets.UTF_8);
                watermarkCache.put(tableId, wm);
                return wm;
            }
        } catch (Exception e) {
            log.warn(
                    "AsyncDerivationQueue.getWatermark failed tableId={}: {}",
                    tableId,
                    e.getMessage());
        }
        return null;
    }

    /**
     * Record the watermark for a table once its derivation chain has completed. Updates the
     * in-memory cache immediately and persists to the Knowledge Store best-effort.
     */
    public void updateWatermark(String tableId, String recordKey) {
        if (tableId == null || recordKey == null) return;
        watermarkCache.put(tableId, recordKey);
        if (knowledgeStore == null) return;
        try {
            byte[] key = (WATERMARK_KEY_PREFIX + tableId).getBytes(StandardCharsets.UTF_8);
            knowledgeStore.put(key, recordKey.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.warn(
                    "AsyncDerivationQueue.updateWatermark persist failed tableId={}: {}",
                    tableId,
                    e.getMessage());
        }
    }

    public int availablePermits() {
        return concurrencyPermits.availablePermits();
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
        virtualThreadExecutor.shutdown();
        try {
            if (!virtualThreadExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                virtualThreadExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            virtualThreadExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("AsyncDerivationQueue closed");
    }
}
