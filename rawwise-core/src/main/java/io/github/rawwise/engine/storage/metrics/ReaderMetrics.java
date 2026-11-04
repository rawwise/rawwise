package io.github.rawwise.engine.storage.metrics;

import java.io.IOException;

public interface ReaderMetrics {
    void recordRead(String operation, long bytes, long recordCount, long durationNanos);

    void recordSeek(long position);

    void recordCacheHit(boolean hit);

    void recordError(ReaderErrorType type, IOException error, String operation);

    void recordRetry(String operation, int retryCount);
}
