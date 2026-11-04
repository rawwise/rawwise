package io.github.rawwise.engine.vector;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

final class CircuitBreaker {

    private final int failureThreshold;
    private final long resetTimeoutMs;
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicBoolean probeInProgress = new AtomicBoolean(false);
    private volatile long lastFailureTime = 0;

    public enum State {
        CLOSED,
        OPEN,
        HALF_OPEN
    }

    CircuitBreaker(int failureThreshold, long resetTimeoutMs) {
        this.failureThreshold = failureThreshold;
        this.resetTimeoutMs = resetTimeoutMs;
    }

    void beforeRequest() {
        State current = state.get();
        switch (current) {
            case CLOSED -> {
                return;
            }
            case OPEN -> {
                if (System.currentTimeMillis() - lastFailureTime > resetTimeoutMs) {
                    if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                        probeInProgress.set(true);
                        return;
                    }
                }
                throw new EmbeddingUnavailableException("Circuit breaker OPEN");
            }
            case HALF_OPEN -> {
                if (probeInProgress.compareAndSet(false, true)) {
                    return;
                }
                throw new EmbeddingUnavailableException(
                        "Circuit breaker HALF_OPEN, probe in progress");
            }
        }
    }

    void onSuccess() {
        State current = state.get();
        if (current == State.HALF_OPEN) {
            probeInProgress.set(false);
            if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
                failureCount.set(0);
            }
        } else if (current == State.CLOSED) {
            failureCount.updateAndGet(v -> Math.max(0, v - 1));
        }
    }

    void onFailure() {
        State current = state.get();
        if (current == State.HALF_OPEN) {
            probeInProgress.set(false);
            lastFailureTime = System.currentTimeMillis();
            state.compareAndSet(State.HALF_OPEN, State.OPEN);
        } else if (current == State.CLOSED) {
            if (failureCount.incrementAndGet() >= failureThreshold) {
                lastFailureTime = System.currentTimeMillis();
                state.compareAndSet(State.CLOSED, State.OPEN);
            }
        }
    }

    State getState() {
        return state.get();
    }

    int getFailureCount() {
        return failureCount.get();
    }
}
