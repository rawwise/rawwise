package io.github.rawwise.backend.server.job;

public record ExternalResult(
        String resultId,
        String storageUri,
        long totalRows,
        long estimatedBytes,
        long completionTime)
        implements JobResultHandle {

    public static final long UNKNOWN_ROW_COUNT = -1L;

    public static ExternalResult ofUnknownRowCount(
            String resultId, String storageUri, long estimatedBytes, long completionTime) {
        return new ExternalResult(
                resultId, storageUri, UNKNOWN_ROW_COUNT, estimatedBytes, completionTime);
    }

    public boolean hasKnownRowCount() {
        return totalRows != UNKNOWN_ROW_COUNT;
    }

    public boolean isExpired(long cutoffTime) {
        return completionTime < cutoffTime;
    }
}
