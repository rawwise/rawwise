package io.github.rawwise.engine.vector;

import io.github.rawwise.governance.metrics.RawWiseMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VectorMetrics implements VectorMetricsMBean {

    private final AtomicLong searchTotalNanos = new AtomicLong(0);
    private final AtomicLong cacheHitCount = new AtomicLong(0);
    private final AtomicLong cacheMissCount = new AtomicLong(0);

    private Counter searchCounter;
    private Counter searchErrorCounter;
    private Counter insertCounter;
    private Counter flushCounter;
    private Counter flushFailureCounter;
    private Counter mergeCounter;
    private Counter coldSearchCounter;
    private Timer searchTimer;
    private final AtomicLong flushFailureCountFallback = new AtomicLong();

    public VectorMetrics() {
        if (RawWiseMetrics.isInitialized()) {
            registerMicrometer();
        }
    }

    private void registerMicrometer() {
        searchCounter = RawWiseMetrics.counter("rawwise.vector.search.count");
        searchErrorCounter = RawWiseMetrics.counter("rawwise.vector.search.error.count");
        insertCounter = RawWiseMetrics.counter("rawwise.vector.insert.count");
        flushCounter = RawWiseMetrics.counter("rawwise.vector.flush.count");
        flushFailureCounter = RawWiseMetrics.counter("rawwise.vector.flush.failure.count");
        mergeCounter = RawWiseMetrics.counter("rawwise.vector.merge.count");
        coldSearchCounter = RawWiseMetrics.counter("rawwise.vector.cold.search.count");
        searchTimer = RawWiseMetrics.timer("rawwise.vector.search.latency");
    }

    public void recordSearch(long latencyNanos) {
        searchTotalNanos.addAndGet(latencyNanos);
        if (searchCounter != null) searchCounter.increment();
        if (searchTimer != null) searchTimer.record(java.time.Duration.ofNanos(latencyNanos));
    }

    public void recordSearchError() {
        if (searchErrorCounter != null) searchErrorCounter.increment();
    }

    public void recordInsert() {
        if (insertCounter != null) insertCounter.increment();
    }

    public void recordFlush(int entryCount) {
        if (flushCounter != null) flushCounter.increment();
    }

    public void recordFlushFailure() {
        flushFailureCountFallback.incrementAndGet();
        if (flushFailureCounter != null) flushFailureCounter.increment();
    }

    public void recordMerge() {
        if (mergeCounter != null) mergeCounter.increment();
    }

    public void recordColdSearch() {
        if (coldSearchCounter != null) coldSearchCounter.increment();
    }

    public void recordCacheHit() {
        cacheHitCount.incrementAndGet();
    }

    public void recordCacheMiss() {
        cacheMissCount.incrementAndGet();
    }

    @Override
    public long getSearchCount() {
        return searchCounter != null ? (long) searchCounter.count() : 0;
    }

    @Override
    public double getSearchLatencyAvgMs() {
        long count = getSearchCount();
        return count > 0 ? searchTotalNanos.get() / (double) count / 1_000_000.0 : 0;
    }

    @Override
    public long getSearchErrorCount() {
        return searchErrorCounter != null ? (long) searchErrorCounter.count() : 0;
    }

    @Override
    public long getInsertCount() {
        return insertCounter != null ? (long) insertCounter.count() : 0;
    }

    @Override
    public long getFlushCount() {
        return flushCounter != null ? (long) flushCounter.count() : 0;
    }

    @Override
    public long getFlushFailureCount() {
        return flushFailureCountFallback.get();
    }

    @Override
    public long getMergeCount() {
        return mergeCounter != null ? (long) mergeCounter.count() : 0;
    }

    @Override
    public long getColdSearchCount() {
        return coldSearchCounter != null ? (long) coldSearchCounter.count() : 0;
    }

    @Override
    public double getEmbeddingCacheHitRate() {
        long hits = cacheHitCount.get();
        long misses = cacheMissCount.get();
        long total = hits + misses;
        return total > 0 ? (double) hits / total : 0.0;
    }
}

interface VectorMetricsMBean {
    long getSearchCount();

    double getSearchLatencyAvgMs();

    long getSearchErrorCount();

    long getInsertCount();

    long getFlushCount();

    long getFlushFailureCount();

    long getMergeCount();

    long getColdSearchCount();

    double getEmbeddingCacheHitRate();
}
