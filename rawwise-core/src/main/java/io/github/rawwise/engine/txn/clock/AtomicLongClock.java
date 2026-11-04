package io.github.rawwise.engine.txn.clock;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Standalone LogicalClock implementation using AtomicLong. Suitable for single-node deployments and
 * unit testing. Thread-safe via AtomicLong CAS operations.
 */
public final class AtomicLongClock implements LogicalClock {
    private final AtomicLong counter;

    public AtomicLongClock(long initialValue) {
        if (initialValue < 0) {
            throw new IllegalArgumentException(
                    "initialValue must be non-negative: " + initialValue);
        }
        this.counter = new AtomicLong(initialValue);
    }

    @Override
    public long now() {
        return counter.get();
    }

    @Override
    public long next() {
        long val = counter.incrementAndGet();
        if (val <= 0) {
            throw new ClockOverflowException("LogicalClock overflowed past Long.MAX_VALUE");
        }
        return val;
    }

    @Override
    public long advanceToAtLeast(long minValue) {
        return counter.updateAndGet(current -> Math.max(current, minValue));
    }

    public static AtomicLongClock recover(long persistedValue) {
        return new AtomicLongClock(persistedValue);
    }

    /**
     * Thrown when the logical clock overflows past {@link Long#MAX_VALUE}. This indicates INV-009
     * monotonicity can no longer be guaranteed.
     */
    public static final class ClockOverflowException extends RuntimeException {
        public ClockOverflowException(String message) {
            super(message);
        }
    }
}
