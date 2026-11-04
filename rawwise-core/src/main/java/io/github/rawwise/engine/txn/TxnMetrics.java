package io.github.rawwise.engine.txn;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe transaction lifecycle metrics for observability.
 *
 * <h3>Design</h3>
 *
 * <ul>
 *   <li>{@link LongAdder} for high-concurrency counters (begin, commit, rollback, etc.)
 *   <li>{@link AtomicLong} for the active count gauge
 *   <li>{@link ConcurrentHashMap} for per-dimension breakdowns (isolation level, reason, conflict
 *       type)
 *   <li>{@link #snapshot()} returns a point-in-time {@link Snapshot} for monitoring systems
 * </ul>
 */
public class TxnMetrics {

    // --- Counters ---
    private final LongAdder beginCount = new LongAdder();
    private final LongAdder commitCount = new LongAdder();
    private final LongAdder rollbackCount = new LongAdder();
    private final LongAdder conflictCount = new LongAdder();
    private final LongAdder timeoutCount = new LongAdder();

    // --- Per-dimension breakdowns ---
    private final ConcurrentHashMap<IsolationLevel, LongAdder> beginCountByLevel =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<IsolationLevel, LongAdder> commitCountByLevel =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> rollbackCountByReason =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ConflictReport.ConflictType, LongAdder> conflictCountByType =
            new ConcurrentHashMap<>();

    // --- Latency tracking ---
    private final LongAdder commitLatencySumMs = new LongAdder();
    private final LongAdder commitLatencyCount = new LongAdder();

    // --- Gauge ---
    private final AtomicLong activeCount = new AtomicLong();

    // ========================================================================
    // Record methods
    // ========================================================================

    /** Record the start of a new transaction. */
    public void recordBegin(IsolationLevel level) {
        beginCount.increment();
        beginCountByLevel.computeIfAbsent(level, k -> new LongAdder()).increment();
    }

    /** Record a successful commit with its latency in milliseconds. */
    public void recordCommit(IsolationLevel level, long latencyMs) {
        commitCount.increment();
        commitCountByLevel.computeIfAbsent(level, k -> new LongAdder()).increment();
        commitLatencySumMs.add(latencyMs);
        commitLatencyCount.increment();
    }

    /** Record a rollback with a human-readable reason. */
    public void recordRollback(IsolationLevel level, String reason) {
        rollbackCount.increment();
        if (reason != null) {
            rollbackCountByReason.computeIfAbsent(reason, k -> new LongAdder()).increment();
        }
    }

    /** Record a conflict detected during validation. */
    public void recordConflict(ConflictReport.ConflictType type) {
        conflictCount.increment();
        conflictCountByType.computeIfAbsent(type, k -> new LongAdder()).increment();
    }

    /** Record a transaction that was force-rolled-back due to timeout. */
    public void recordTimeout() {
        timeoutCount.increment();
    }

    /** Increment the active transaction gauge. */
    public void incrementActive() {
        activeCount.incrementAndGet();
    }

    /** Decrement the active transaction gauge. */
    public void decrementActive() {
        activeCount.decrementAndGet();
    }

    // ========================================================================
    // Getters
    // ========================================================================

    public long beginCount() {
        return beginCount.sum();
    }

    public long beginCount(IsolationLevel level) {
        LongAdder adder = beginCountByLevel.get(level);
        return adder != null ? adder.sum() : 0L;
    }

    public long commitCount() {
        return commitCount.sum();
    }

    public long commitCount(IsolationLevel level) {
        LongAdder adder = commitCountByLevel.get(level);
        return adder != null ? adder.sum() : 0L;
    }

    public long rollbackCount() {
        return rollbackCount.sum();
    }

    public long rollbackCount(String reason) {
        LongAdder adder = rollbackCountByReason.get(reason);
        return adder != null ? adder.sum() : 0L;
    }

    public long conflictCount() {
        return conflictCount.sum();
    }

    public long conflictCount(ConflictReport.ConflictType type) {
        LongAdder adder = conflictCountByType.get(type);
        return adder != null ? adder.sum() : 0L;
    }

    public long timeoutCount() {
        return timeoutCount.sum();
    }

    public long activeCount() {
        return activeCount.get();
    }

    /**
     * Average commit latency in milliseconds.
     *
     * @return average latency, or 0 if no commits have been recorded
     */
    public double commitLatencyAvgMs() {
        long count = commitLatencyCount.sum();
        if (count == 0) {
            return 0.0;
        }
        return (double) commitLatencySumMs.sum() / count;
    }

    // ========================================================================
    // Snapshot
    // ========================================================================

    /**
     * Point-in-time snapshot of all metrics for monitoring systems.
     *
     * <p>Snapshot values are computed at call time and may not be perfectly consistent with each
     * other — this is acceptable for metrics.
     */
    public Snapshot snapshot() {
        Map<IsolationLevel, Long> beginByLevel = new HashMap<>();
        for (Map.Entry<IsolationLevel, LongAdder> e : beginCountByLevel.entrySet()) {
            beginByLevel.put(e.getKey(), e.getValue().sum());
        }

        Map<IsolationLevel, Long> commitByLevel = new HashMap<>();
        for (Map.Entry<IsolationLevel, LongAdder> e : commitCountByLevel.entrySet()) {
            commitByLevel.put(e.getKey(), e.getValue().sum());
        }

        Map<String, Long> rollbackByReason = new HashMap<>();
        for (Map.Entry<String, LongAdder> e : rollbackCountByReason.entrySet()) {
            rollbackByReason.put(e.getKey(), e.getValue().sum());
        }

        Map<ConflictReport.ConflictType, Long> conflictByType = new HashMap<>();
        for (Map.Entry<ConflictReport.ConflictType, LongAdder> e : conflictCountByType.entrySet()) {
            conflictByType.put(e.getKey(), e.getValue().sum());
        }

        return new Snapshot(
                beginCount.sum(),
                Collections.unmodifiableMap(beginByLevel),
                commitCount.sum(),
                Collections.unmodifiableMap(commitByLevel),
                rollbackCount.sum(),
                Collections.unmodifiableMap(rollbackByReason),
                conflictCount.sum(),
                Collections.unmodifiableMap(conflictByType),
                timeoutCount.sum(),
                activeCount.get(),
                commitLatencyAvgMs());
    }

    /** Immutable point-in-time snapshot of all transaction metrics. */
    public record Snapshot(
            long beginCount,
            Map<IsolationLevel, Long> beginCountByLevel,
            long commitCount,
            Map<IsolationLevel, Long> commitCountByLevel,
            long rollbackCount,
            Map<String, Long> rollbackCountByReason,
            long conflictCount,
            Map<ConflictReport.ConflictType, Long> conflictCountByType,
            long timeoutCount,
            long activeCount,
            double commitLatencyAvgMs) {
        @Override
        public String toString() {
            return "TxnMetrics.Snapshot{"
                    + "beginCount="
                    + beginCount
                    + ", commitCount="
                    + commitCount
                    + ", rollbackCount="
                    + rollbackCount
                    + ", conflictCount="
                    + conflictCount
                    + ", timeoutCount="
                    + timeoutCount
                    + ", activeCount="
                    + activeCount
                    + ", commitLatencyAvgMs="
                    + commitLatencyAvgMs
                    + "}";
        }
    }
}
