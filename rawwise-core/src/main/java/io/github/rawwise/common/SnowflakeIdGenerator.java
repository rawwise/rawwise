package io.github.rawwise.common;

import io.github.rawwise.engine.storage.ingestion.SinkException;
import io.github.rawwise.engine.storage.ingestion.SinkFatalException;
import io.github.rawwise.engine.storage.ingestion.SinkRetryableException;
import java.time.Clock;
import java.util.Objects;

public class SnowflakeIdGenerator {

    private static final long EPOCH = 1700000000000L;
    private static final long MAX_CLOCK_BACKWARD_MS = 2000L;
    private static final int NEXT_MILLIS_TIMEOUT_MS = 10;
    private static final int SPIN_THRESHOLD = 10_000_000;
    private static final long WORKER_ID_MASK = 0x3FFL;
    private static final long SEQUENCE_MASK = 0xFFFL;

    private final long workerId;
    private final Clock clock;
    private long lastTimestamp = -1L;
    private long sequence = 0L;

    public SnowflakeIdGenerator(long workerId, Clock clock) {
        if (workerId < 0 || workerId > WORKER_ID_MASK) {
            throw new IllegalArgumentException("workerId must be in [0, 1023]");
        }
        this.workerId = workerId;
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    public synchronized long nextId() throws SinkException {
        long ts = clock.millis() - EPOCH;

        if (ts < lastTimestamp) {
            long offset = lastTimestamp - ts;
            if (offset > MAX_CLOCK_BACKWARD_MS) {
                throw new SinkFatalException(
                        "Clock moved backwards by " + offset + "ms, worker " + workerId);
            }
            try {
                Thread.sleep(offset + 1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SinkFatalException("Interrupted during clock catchup");
            }
            ts = clock.millis() - EPOCH;
            if (ts < lastTimestamp) {
                throw new SinkFatalException(
                        "Clock still behind after " + offset + "ms sleep, worker " + workerId);
            }
        }

        if (ts == lastTimestamp) {
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0) {
                ts = nextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }
        lastTimestamp = ts;
        return (ts << 22) | (workerId << 12) | sequence;
    }

    private long nextMillis(long lastTimestamp) throws SinkException {
        long deadline = clock.millis() + NEXT_MILLIS_TIMEOUT_MS;
        long ts = clock.millis() - EPOCH;
        int spins = 0;
        while (ts <= lastTimestamp) {
            if (clock.millis() > deadline) {
                throw new SinkRetryableException("Sequence overflow timeout, worker " + workerId);
            }
            if (++spins > SPIN_THRESHOLD) {
                Thread.yield();
                spins = 0;
            }
            Thread.onSpinWait();
            ts = clock.millis() - EPOCH;
        }
        return ts;
    }
}
