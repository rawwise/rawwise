package io.github.rawwise.axiom.pipeline.derivation;

import io.github.rawwise.engine.vector.EmbeddingProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EmbeddingBatcher implements AutoCloseable {

    public static final int DEFAULT_BATCH_SIZE = 32;
    public static final long DEFAULT_FLUSH_INTERVAL_MS = 100L;

    private final EmbeddingProvider provider;
    private final int batchSize;
    private final long flushIntervalMs;
    private final List<PendingEmbed> buffer;
    private final Lock batchLock;
    private final ScheduledExecutorService flushScheduler;

    public record PendingEmbed(String text, CompletableFuture<float[]> future) {}

    public EmbeddingBatcher(EmbeddingProvider provider, int batchSize, long flushIntervalMs) {
        this.provider = provider;
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
        this.flushIntervalMs = flushIntervalMs > 0 ? flushIntervalMs : DEFAULT_FLUSH_INTERVAL_MS;
        this.buffer = new ArrayList<>();
        this.batchLock = new ReentrantLock();
        this.flushScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "embedding-batcher-flush");
                            t.setDaemon(true);
                            return t;
                        });
        this.flushScheduler.scheduleAtFixedRate(
                this::flushIfReady,
                this.flushIntervalMs,
                this.flushIntervalMs,
                TimeUnit.MILLISECONDS);
        log.info(
                "EmbeddingBatcher initialized: batchSize={}, flushIntervalMs={}, provider={}",
                this.batchSize,
                this.flushIntervalMs,
                provider != null ? provider.modelIdentifier() : "null");
    }

    public CompletableFuture<float[]> embed(String text) {
        if (provider == null) {
            return CompletableFuture.completedFuture(new float[0]);
        }
        CompletableFuture<float[]> future = new CompletableFuture<>();
        boolean shouldFlush = false;
        batchLock.lock();
        try {
            buffer.add(new PendingEmbed(text, future));
            if (buffer.size() >= batchSize) {
                shouldFlush = true;
            }
        } finally {
            batchLock.unlock();
        }
        if (shouldFlush) {
            flushIfReady();
        }
        return future;
    }

    private void flushIfReady() {
        List<PendingEmbed> drained;
        batchLock.lock();
        try {
            if (buffer.isEmpty()) {
                return;
            }
            drained = new ArrayList<>(buffer);
            buffer.clear();
        } finally {
            batchLock.unlock();
        }
        try {
            List<String> texts = new ArrayList<>(drained.size());
            for (PendingEmbed pe : drained) {
                texts.add(pe.text());
            }
            float[][] embeddings = provider.embedBatch(texts);
            for (int i = 0; i < drained.size(); i++) {
                float[] emb =
                        (embeddings != null && i < embeddings.length && embeddings[i] != null)
                                ? embeddings[i]
                                : new float[0];
                drained.get(i).future().complete(emb);
            }
        } catch (Exception e) {
            log.warn(
                    "EmbeddingBatcher flush failed for {} pending embeds: {}",
                    drained.size(),
                    e.getMessage(),
                    e);
            for (PendingEmbed pe : drained) {
                pe.future().completeExceptionally(e);
            }
        }
    }

    @Override
    public void close() {
        flushScheduler.shutdown();
        try {
            if (!flushScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                flushScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            flushScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        flushIfReady();
        log.info("EmbeddingBatcher closed");
    }
}
