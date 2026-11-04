package io.github.rawwise.engine.vector.onnx;

import io.github.rawwise.engine.vector.EmbeddingProvider;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EmbeddingProviderSwitcher implements EmbeddingProvider {

    private static final int FAILURE_THRESHOLD = 5;
    private static final long RESET_TIMEOUT_MS = 30_000L;

    private final AtomicReference<EmbeddingProvider> current;
    private final EmbeddingProvider fallback;
    private volatile int dimension;

    private final AtomicInteger currentFailures = new AtomicInteger(0);
    private final AtomicLong currentOpenSince = new AtomicLong(0);

    private final AtomicReference<EmbeddingProvider> shadow = new AtomicReference<>(null);
    private final AtomicBoolean inTransition = new AtomicBoolean(false);
    private final AtomicLong transitionDualWriteCount = new AtomicLong(0);
    private final AtomicLong transitionShadowFailures = new AtomicLong(0);

    private volatile ReindexCoordinator reindexCoordinator;
    private volatile String pendingReindexJobId;
    private volatile int pendingNewDimension;

    public EmbeddingProviderSwitcher(EmbeddingProvider current, EmbeddingProvider fallback) {
        this(current, fallback, null);
    }

    public EmbeddingProviderSwitcher(
            EmbeddingProvider current,
            EmbeddingProvider fallback,
            ReindexCoordinator reindexCoordinator) {
        if (current == null || fallback == null) {
            throw new IllegalArgumentException("current and fallback providers must not be null");
        }
        int currentDim = resolveDimension(current);
        int fallbackDim = resolveDimension(fallback);
        if (currentDim > 0 && fallbackDim > 0 && currentDim != fallbackDim) {
            throw new IllegalStateException(
                    "EmbeddingProviderSwitcher dimension mismatch: current="
                            + currentDim
                            + " fallback="
                            + fallbackDim);
        }
        this.current = new AtomicReference<>(current);
        this.fallback = fallback;
        this.dimension = currentDim > 0 ? currentDim : fallbackDim;
        this.reindexCoordinator = reindexCoordinator;
    }

    public void setReindexCoordinator(ReindexCoordinator coordinator) {
        this.reindexCoordinator = coordinator;
        log.info(
                "EmbeddingProviderSwitcher: ReindexCoordinator wired (wasNull={})",
                coordinator == null);
    }

    @Override
    public float[] embed(String text) {
        EmbeddingProvider primary = current.get();
        EmbeddingProvider shadowProvider = inTransition.get() ? shadow.get() : null;

        if (shadowProvider != null) {
            try {
                shadowProvider.embed(text);
                transitionDualWriteCount.incrementAndGet();
            } catch (RuntimeException e) {
                transitionShadowFailures.incrementAndGet();
                log.warn("EmbeddingProviderSwitcher shadow dual-write failed: {}", e.getMessage());
            }
        }

        if (isCurrentOpen()) {
            return fallback.embed(text);
        }
        try {
            float[] vec = primary.embed(text);
            currentFailures.set(0);
            return vec;
        } catch (RuntimeException e) {
            int fails = currentFailures.incrementAndGet();
            log.warn("Primary EmbeddingProvider failed (failures={}): {}", fails, e.getMessage());
            if (fails >= FAILURE_THRESHOLD) {
                currentOpenSince.set(System.currentTimeMillis());
            }
            if (shadowProvider != null) {
                try {
                    return shadowProvider.embed(text);
                } catch (RuntimeException shadowEx) {
                    log.warn(
                            "Shadow provider also failed, using fallback: {}",
                            shadowEx.getMessage());
                }
            }
            return fallback.embed(text);
        }
    }

    @Override
    public float[][] embedBatch(List<String> texts) {
        float[][] result = new float[texts.size()][];
        for (int i = 0; i < texts.size(); i++) {
            result[i] = embed(texts.get(i));
        }
        return result;
    }

    public int dimension() {
        return dimension;
    }

    public void startTransition(EmbeddingProvider newProvider) {
        if (newProvider == null) {
            throw new IllegalArgumentException("newProvider must not be null");
        }
        int newDim = resolveDimension(newProvider);
        if (newDim > 0 && dimension > 0 && newDim != dimension) {
            throw new IllegalStateException(
                    "startTransition dimension mismatch: existing="
                            + dimension
                            + " new="
                            + newDim
                            + " — use startDimensionTransition for dimension changes");
        }
        if (!inTransition.compareAndSet(false, true)) {
            throw new IllegalStateException("Transition already in progress");
        }
        shadow.set(newProvider);
        transitionDualWriteCount.set(0);
        transitionShadowFailures.set(0);
        pendingNewDimension = 0;
        log.info(
                "EmbeddingProviderSwitcher.startTransition: dual-write begin, current={} shadow={}",
                current.get().modelIdentifier(),
                newProvider.modelIdentifier());
    }

    public String startDimensionTransition(
            EmbeddingProvider newProvider,
            int newDimension,
            String srcTableId,
            String destTableId) {
        if (newProvider == null) {
            throw new IllegalArgumentException("newProvider must not be null");
        }
        if (newDimension <= 0) {
            throw new IllegalArgumentException("newDimension must be positive: " + newDimension);
        }
        int providerDim = resolveDimension(newProvider);
        if (providerDim > 0 && providerDim != newDimension) {
            throw new IllegalStateException(
                    "startDimensionTransition: provider dimension="
                            + providerDim
                            + " does not match requested newDimension="
                            + newDimension);
        }
        if (!inTransition.compareAndSet(false, true)) {
            throw new IllegalStateException("Transition already in progress");
        }
        shadow.set(newProvider);
        transitionDualWriteCount.set(0);
        transitionShadowFailures.set(0);
        pendingNewDimension = newDimension;

        String jobId = null;
        if (reindexCoordinator != null) {
            jobId =
                    reindexCoordinator.submitReindex(
                            srcTableId, destTableId, dimension, newDimension);
            pendingReindexJobId = jobId;
            log.info(
                    "EmbeddingProviderSwitcher.startDimensionTransition: dim {}->{} reindexJobId={} dual-write begin",
                    dimension,
                    newDimension,
                    jobId);
        } else {
            log.warn(
                    "EmbeddingProviderSwitcher.startDimensionTransition: dim {}->{} but no ReindexCoordinator wired, "
                            + "reindex job NOT submitted",
                    dimension,
                    newDimension);
        }
        return jobId;
    }

    public void commitTransition() {
        if (!inTransition.compareAndSet(true, false)) {
            throw new IllegalStateException("No transition in progress");
        }
        EmbeddingProvider shadowProvider = shadow.getAndSet(null);
        if (shadowProvider == null) {
            throw new IllegalStateException("Transition was in progress but shadow was null");
        }
        EmbeddingProvider previous = current.getAndSet(shadowProvider);
        currentFailures.set(0);
        currentOpenSince.set(0);
        int oldDimension = this.dimension;
        if (pendingNewDimension > 0) {
            this.dimension = pendingNewDimension;
            log.info(
                    "EmbeddingProviderSwitcher.commitTransition: {} -> {} (dualWrites={} shadowFailures={}) "
                            + "dimension {}->{} reindexJobId={}",
                    previous.modelIdentifier(),
                    shadowProvider.modelIdentifier(),
                    transitionDualWriteCount.get(),
                    transitionShadowFailures.get(),
                    oldDimension,
                    pendingNewDimension,
                    pendingReindexJobId);
            pendingNewDimension = 0;
        } else {
            log.info(
                    "EmbeddingProviderSwitcher.commitTransition: {} -> {} (dualWrites={} shadowFailures={})",
                    previous.modelIdentifier(),
                    shadowProvider.modelIdentifier(),
                    transitionDualWriteCount.get(),
                    transitionShadowFailures.get());
        }
        pendingReindexJobId = null;
        if (previous instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                log.debug(
                        "EmbeddingProviderSwitcher.commitTransition: failed to close previous provider: {}",
                        e.getMessage());
            }
        }
    }

    public void abortTransition() {
        if (!inTransition.compareAndSet(true, false)) {
            throw new IllegalStateException("No transition in progress");
        }
        EmbeddingProvider abandoned = shadow.getAndSet(null);
        if (abandoned instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                log.debug(
                        "EmbeddingProviderSwitcher.abortTransition: failed to close abandoned shadow: {}",
                        e.getMessage());
            }
        }
        log.info(
                "EmbeddingProviderSwitcher.abortTransition: discarded shadow={} keeping current={} "
                        + "pendingNewDimension={} reindexJobId={}",
                abandoned == null ? "<null>" : abandoned.modelIdentifier(),
                current.get().modelIdentifier(),
                pendingNewDimension,
                pendingReindexJobId);
        pendingNewDimension = 0;
        pendingReindexJobId = null;
    }

    public boolean isInTransition() {
        return inTransition.get();
    }

    public EmbeddingProvider getShadow() {
        return shadow.get();
    }

    public long getTransitionDualWriteCount() {
        return transitionDualWriteCount.get();
    }

    public long getTransitionShadowFailures() {
        return transitionShadowFailures.get();
    }

    public String getPendingReindexJobId() {
        return pendingReindexJobId;
    }

    public ReindexCoordinator getReindexCoordinator() {
        return reindexCoordinator;
    }

    public void swapCurrent(EmbeddingProvider newProvider) {
        if (newProvider == null) {
            throw new IllegalArgumentException("newProvider must not be null");
        }
        int newDim = resolveDimension(newProvider);
        if (newDim > 0 && dimension > 0 && newDim != dimension) {
            throw new IllegalStateException(
                    "swapCurrent dimension mismatch: existing=" + dimension + " new=" + newDim);
        }
        EmbeddingProvider previous = current.getAndSet(newProvider);
        currentFailures.set(0);
        currentOpenSince.set(0);
        log.info(
                "EmbeddingProviderSwitcher.swapCurrent: {} -> {}",
                previous == null ? "<null>" : previous.modelIdentifier(),
                newProvider.modelIdentifier());
    }

    public EmbeddingProvider getCurrent() {
        return current.get();
    }

    public EmbeddingProvider getFallback() {
        return fallback;
    }

    @Override
    public String modelIdentifier() {
        EmbeddingProvider c = current.get();
        String id =
                "switcher("
                        + (c == null ? "?" : c.modelIdentifier())
                        + " | fallback="
                        + fallback.modelIdentifier();
        if (inTransition.get() && shadow.get() != null) {
            id += " | shadow=" + shadow.get().modelIdentifier();
        }
        return id + ")";
    }

    private boolean isCurrentOpen() {
        long openSince = currentOpenSince.get();
        if (openSince == 0) {
            return false;
        }
        if (System.currentTimeMillis() - openSince > RESET_TIMEOUT_MS) {
            currentOpenSince.set(0);
            currentFailures.set(0);
            return false;
        }
        return true;
    }

    private static int resolveDimension(EmbeddingProvider provider) {
        if (provider instanceof OnnxEmbeddingProvider p) return p.dimension();
        if (provider instanceof NoOpOnnxEmbeddingProvider p) return p.dimension();
        if (provider instanceof EmbeddingProviderSwitcher s) return s.dimension();
        return 0;
    }
}
