package io.github.rawwise.execution;

public record RetryPolicy(
        int maxAttempts,
        long initialDelayMs,
        double backoffMultiplier,
        long maxDelayMs,
        RetryOn retryOn) {

    public static final RetryPolicy NO_RETRY = new RetryPolicy(1, 0, 1.0, 0, RetryOn.NEVER);

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1, got: " + maxAttempts);
        }
        if (initialDelayMs < 0) {
            throw new IllegalArgumentException(
                    "initialDelayMs must be >= 0, got: " + initialDelayMs);
        }
        if (maxDelayMs < 0) {
            throw new IllegalArgumentException("maxDelayMs must be >= 0, got: " + maxDelayMs);
        }
        if (backoffMultiplier < 1.0) {
            throw new IllegalArgumentException(
                    "backoffMultiplier must be >= 1.0, got: " + backoffMultiplier);
        }
        if (retryOn == null) {
            retryOn = RetryOn.ON_FAILURE;
        }
    }

    public enum RetryOn {
        NEVER,
        ON_FAILURE,
        ON_TIMEOUT,
        ON_FAILURE_OR_TIMEOUT
    }

    public long computeDelay(int attemptNumber) {
        if (attemptNumber <= 1) return 0;
        long delay = (long) (initialDelayMs * Math.pow(backoffMultiplier, attemptNumber - 2));
        return Math.min(delay, maxDelayMs);
    }
}
