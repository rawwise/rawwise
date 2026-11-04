package io.github.rawwise.engine.storage;

public record WritePathConfig(
        int maxPendingBatchSize,
        long maxPendingBatchBytes,
        int maxPendingInstances,
        double lmdbUsageThreshold) {
    public WritePathConfig {
        if (maxPendingBatchSize <= 0)
            throw new IllegalArgumentException(
                    "maxPendingBatchSize must be positive, got: " + maxPendingBatchSize);
        if (maxPendingBatchBytes <= 0)
            throw new IllegalArgumentException(
                    "maxPendingBatchBytes must be positive, got: " + maxPendingBatchBytes);
        if (maxPendingInstances <= 0)
            throw new IllegalArgumentException(
                    "maxPendingInstances must be positive, got: " + maxPendingInstances);
        if (lmdbUsageThreshold <= 0.0 || lmdbUsageThreshold >= 1.0)
            throw new IllegalArgumentException(
                    "lmdbUsageThreshold must be in (0.0, 1.0), got: " + lmdbUsageThreshold);
    }
}
