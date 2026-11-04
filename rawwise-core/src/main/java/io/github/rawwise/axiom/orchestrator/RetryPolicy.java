package io.github.rawwise.axiom.orchestrator;

public record RetryPolicy(int maxRetries, long initialDelayMs, double backoffMultiplier) {
    public static final RetryPolicy NO_RETRY = new RetryPolicy(0, 0, 1.0);
}
