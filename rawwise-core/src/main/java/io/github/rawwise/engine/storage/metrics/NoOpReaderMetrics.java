package io.github.rawwise.engine.storage.metrics;

import java.io.IOException;

public enum NoOpReaderMetrics implements ReaderMetrics {
    INSTANCE;

    @Override
    public void recordRead(String operation, long bytes, long recordCount, long durationNanos) {}

    @Override
    public void recordSeek(long position) {}

    @Override
    public void recordCacheHit(boolean hit) {}

    @Override
    public void recordError(ReaderErrorType type, IOException error, String operation) {}

    @Override
    public void recordRetry(String operation, int retryCount) {}
}
