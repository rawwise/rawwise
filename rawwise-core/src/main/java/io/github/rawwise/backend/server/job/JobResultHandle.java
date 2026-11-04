package io.github.rawwise.backend.server.job;

public sealed interface JobResultHandle permits InlineResult, ExternalResult, LeasedResult {
    String resultId();

    long estimatedBytes();

    long completionTime();

    boolean isExpired(long cutoffTime);
}
