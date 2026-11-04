package io.github.rawwise.backend.server.job;

public record LeasedResult(
        String resultId,
        String splitPlanJson,
        long leaseDurationMs,
        long estimatedBytes,
        long completionTime)
        implements JobResultHandle {
    public boolean isExpired(long cutoffTime) {
        return completionTime < cutoffTime;
    }

    public boolean isLeaseValid(long leaseStartTimeMs) {
        return System.currentTimeMillis() < leaseStartTimeMs + leaseDurationMs;
    }
}
