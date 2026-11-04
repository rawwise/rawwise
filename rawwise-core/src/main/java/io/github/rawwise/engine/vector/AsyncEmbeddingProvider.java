package io.github.rawwise.engine.vector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AsyncEmbeddingProvider implements EmbeddingProvider, AutoCloseable {

    private final HttpEmbeddingProvider delegate;
    private final ExecutorService executor;
    private final int batchSize;
    private final Duration timeout;
    private final ConcurrentLinkedQueue<PendingEmbedding> pendingQueue =
            new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService retryScheduler;

    public record PendingEmbedding(
            String namespace,
            String entryKey,
            String text,
            long createdAtMs,
            java.util.function.BiConsumer<float[], Throwable> callback) {

        public PendingEmbedding(String namespace, String entryKey, String text, long createdAtMs) {
            this(namespace, entryKey, text, createdAtMs, null);
        }
    }

    public AsyncEmbeddingProvider(
            HttpEmbeddingProvider delegate, int threadPoolSize, int batchSize, long timeoutMs) {
        this.delegate = delegate;
        this.batchSize = batchSize;
        this.timeout = Duration.ofMillis(timeoutMs);
        this.executor =
                Executors.newFixedThreadPool(
                        threadPoolSize,
                        r -> {
                            Thread t = new Thread(r, "async-embedding");
                            t.setDaemon(false);
                            return t;
                        });
        this.retryScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "async-embedding-retry");
                            t.setDaemon(true);
                            return t;
                        });
        this.retryScheduler.scheduleWithFixedDelay(
                this::retryPendingEmbeddings, 30, 30, TimeUnit.SECONDS);
    }

    @Override
    public float[] embed(String text) {
        Future<float[]> future = executor.submit(() -> delegate.embed(text));
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new EmbeddingUnavailableException(
                    "Embedding timed out after " + timeout.toMillis() + "ms", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EmbeddingUnavailableException("Embedding interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) throw re;
            throw new EmbeddingUnavailableException("Embedding failed", cause);
        }
    }

    public void embedAsync(
            String namespace,
            String entryKey,
            String text,
            java.util.function.BiConsumer<String, float[]> onEmbeddingReady) {
        if (delegate.getCircuitBreakerState() == CircuitBreaker.State.OPEN) {
            pendingQueue.add(
                    new PendingEmbedding(namespace, entryKey, text, System.currentTimeMillis()));
            log.info(
                    "Circuit breaker OPEN, queued embedding for namespace={} entryKey={}",
                    namespace,
                    entryKey);
            return;
        }
        executor.submit(
                () -> {
                    try {
                        float[] embedding = delegate.embed(text);
                        if (onEmbeddingReady != null) {
                            onEmbeddingReady.accept(entryKey, embedding);
                        }
                    } catch (EmbeddingUnavailableException e) {
                        pendingQueue.add(
                                new PendingEmbedding(
                                        namespace, entryKey, text, System.currentTimeMillis()));
                        log.info(
                                "Embedding failed, queued for retry: namespace={} entryKey={}",
                                namespace,
                                entryKey);
                    } catch (Exception e) {
                        log.error(
                                "Unexpected embedding error for namespace={} entryKey={}",
                                namespace,
                                entryKey,
                                e);
                    }
                });
    }

    private void retryPendingEmbeddings() {
        List<PendingEmbedding> batch = new ArrayList<>();
        PendingEmbedding pe;
        while ((pe = pendingQueue.poll()) != null) {
            batch.add(pe);
        }
        if (batch.isEmpty()) return;

        log.info("Retrying {} pending embeddings", batch.size());
        int succeeded = 0;
        for (PendingEmbedding pending : batch) {
            try {
                float[] result = delegate.embed(pending.text());
                if (pending.callback() != null) {
                    pending.callback().accept(result, null);
                }
                log.info(
                        "Retry succeeded for namespace={} entryKey={}",
                        pending.namespace(),
                        pending.entryKey());
                succeeded++;
            } catch (Exception e) {
                if (pending.callback() != null) {
                    pending.callback().accept(null, e);
                }
                pendingQueue.add(pending);
                log.debug(
                        "Retry failed for namespace={} entryKey={}, will retry later",
                        pending.namespace(),
                        pending.entryKey());
            }
        }
        log.info(
                "Retry completed: {}/{} succeeded, {} re-queued",
                succeeded,
                batch.size(),
                batch.size() - succeeded);
    }

    public int pendingCount() {
        return pendingQueue.size();
    }

    public CompletableFuture<float[]> embedAsyncFuture(String text) {
        if (delegate.getCircuitBreakerState() == CircuitBreaker.State.OPEN) {
            return CompletableFuture.failedFuture(
                    new EmbeddingUnavailableException("Circuit breaker OPEN"));
        }
        return CompletableFuture.supplyAsync(() -> delegate.embed(text), executor)
                .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
                .exceptionally(
                        e -> {
                            if (e instanceof java.util.concurrent.TimeoutException) {
                                throw new EmbeddingUnavailableException(
                                        "Embedding timed out after " + timeout.toMillis() + "ms",
                                        e);
                            }
                            if (e instanceof java.util.concurrent.CompletionException
                                    && e.getCause() != null) {
                                Throwable cause = e.getCause();
                                if (cause instanceof RuntimeException re) throw re;
                                throw new EmbeddingUnavailableException("Embedding failed", cause);
                            }
                            if (e instanceof RuntimeException re) throw re;
                            throw new EmbeddingUnavailableException("Embedding failed", e);
                        });
    }

    public CompletableFuture<float[][]> embedBatchAsyncFuture(List<String> texts) {
        if (texts.isEmpty()) {
            return CompletableFuture.completedFuture(new float[0][]);
        }
        List<CompletableFuture<float[]>> futures = new ArrayList<>(texts.size());
        for (String text : texts) {
            futures.add(embedAsyncFuture(text));
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(
                        v -> {
                            float[][] result = new float[texts.size()][];
                            for (int i = 0; i < futures.size(); i++) {
                                result[i] = futures.get(i).join();
                            }
                            return result;
                        });
    }

    @Override
    public float[][] embedBatch(List<String> texts) {
        if (texts.size() <= batchSize) {
            return delegate.embedBatch(texts);
        }
        List<Future<float[][]>> futures = new ArrayList<>();
        int offset = 0;
        while (offset < texts.size()) {
            int end = Math.min(offset + batchSize, texts.size());
            List<String> batch = texts.subList(offset, end);
            futures.add(executor.submit(() -> delegate.embedBatch(batch)));
            offset = end;
        }

        float[][] result = new float[texts.size()][];
        offset = 0;
        for (Future<float[][]> future : futures) {
            try {
                float[][] batchResult = future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                System.arraycopy(batchResult, 0, result, offset, batchResult.length);
                offset += batchResult.length;
            } catch (TimeoutException e) {
                future.cancel(true);
                throw new EmbeddingUnavailableException("Batch embedding timed out", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new EmbeddingUnavailableException("Batch embedding interrupted", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException re) throw re;
                throw new EmbeddingUnavailableException("Batch embedding failed", cause);
            }
        }
        return result;
    }

    @Override
    public String modelIdentifier() {
        return delegate.modelIdentifier();
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
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        try {
            delegate.close();
        } catch (Exception e) {
            log.warn("Failed to close delegate provider", e);
        }
    }
}
