package io.github.rawwise.engine.storage.metrics;

import io.github.rawwise.governance.metrics.RawWiseMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MicrometerReaderMetrics implements ReaderMetrics {

    private final Timer readTimer;
    private final Counter seekCounter;
    private final Counter cacheHitCounter;
    private final Counter cacheMissCounter;
    private final Counter errorCounter;
    private final Counter retryCounter;

    public MicrometerReaderMetrics() {
        this.readTimer = RawWiseMetrics.timer("rawwise.storage.read.latency");
        this.seekCounter = RawWiseMetrics.counter("rawwise.storage.read.seek.count");
        this.cacheHitCounter = RawWiseMetrics.counter("rawwise.storage.read.cache.hit");
        this.cacheMissCounter = RawWiseMetrics.counter("rawwise.storage.read.cache.miss");
        this.errorCounter = RawWiseMetrics.counter("rawwise.storage.read.error.count");
        this.retryCounter = RawWiseMetrics.counter("rawwise.storage.read.retry.count");
    }

    @Override
    public void recordRead(String operation, long bytes, long recordCount, long durationNanos) {
        readTimer.record(durationNanos, TimeUnit.NANOSECONDS);
        RawWiseMetrics.counter("rawwise.storage.read.bytes", "operation", operation);
        RawWiseMetrics.counter("rawwise.storage.read.records", "operation", operation);
    }

    @Override
    public void recordSeek(long position) {
        seekCounter.increment();
    }

    @Override
    public void recordCacheHit(boolean hit) {
        if (hit) {
            cacheHitCounter.increment();
        } else {
            cacheMissCounter.increment();
        }
    }

    @Override
    public void recordError(ReaderErrorType type, IOException error, String operation) {
        errorCounter.increment();
    }

    @Override
    public void recordRetry(String operation, int retryCount) {
        retryCounter.increment(retryCount);
    }
}
