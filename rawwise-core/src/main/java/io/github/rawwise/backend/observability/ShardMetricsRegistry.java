package io.github.rawwise.backend.observability;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

/**
 * Per-shard metrics registry.
 *
 * <p>Architecture: registers Micrometer/Prometheus metrics for each shard, supporting
 * query/write/compaction/replication dimensions.
 *
 * <p>Semantics:
 *
 * <ul>
 *   <li>each shardId maps to a {@link ShardMetrics} instance
 *   <li>metrics include: readCount, writeCount, compactionCount, replicationLagBytes, errorCount
 *   <li>thread safety: all counters use AtomicLong
 *   <li>production can integrate with Micrometer/Prometheus; MVP phase uses in-memory counters
 * </ul>
 */
@Slf4j
public class ShardMetricsRegistry {
    private final Map<Integer, ShardMetrics> metricsMap = new ConcurrentHashMap<>();

    /** Get or create shard metrics instance. */
    public ShardMetrics getOrCreate(int shardId) {
        return metricsMap.computeIfAbsent(
                shardId,
                k -> {
                    log.debug("ShardMetrics created for shardId={}", shardId);
                    return new ShardMetrics(shardId);
                });
    }

    /** Record read operation. */
    public void recordRead(int shardId) {
        getOrCreate(shardId).readCount.incrementAndGet();
    }

    /** Record read operation. */
    public void recordWrite(int shardId) {
        getOrCreate(shardId).writeCount.incrementAndGet();
    }

    /** Record compaction operation. */
    public void recordCompaction(int shardId) {
        getOrCreate(shardId).compactionCount.incrementAndGet();
    }

    /** Record error. */
    public void recordError(int shardId) {
        getOrCreate(shardId).errorCount.incrementAndGet();
    }

    /** Update replication lag (bytes). */
    public void updateReplicationLag(int shardId, long lagBytes) {
        getOrCreate(shardId).replicationLagBytes.set(lagBytes);
    }

    /** Returns a snapshot of all shard metrics. */
    public Map<Integer, ShardMetrics> snapshot() {
        return Map.copyOf(metricsMap);
    }

    /** Remove specified shard metrics. */
    public void removeShard(int shardId) {
        metricsMap.remove(shardId);
    }

    /** Clear all shard metrics. */
    public void clear() {
        metricsMap.clear();
    }

    /** Single shard metrics. */
    public static class ShardMetrics {
        private final int shardId;
        final AtomicLong readCount = new AtomicLong(0);
        final AtomicLong writeCount = new AtomicLong(0);
        final AtomicLong compactionCount = new AtomicLong(0);
        final AtomicLong errorCount = new AtomicLong(0);
        final AtomicLong replicationLagBytes = new AtomicLong(0);

        public ShardMetrics(int shardId) {
            this.shardId = shardId;
        }

        public int shardId() {
            return shardId;
        }

        public long readCount() {
            return readCount.get();
        }

        public long writeCount() {
            return writeCount.get();
        }

        public long compactionCount() {
            return compactionCount.get();
        }

        public long errorCount() {
            return errorCount.get();
        }

        public long replicationLagBytes() {
            return replicationLagBytes.get();
        }

        @Override
        public String toString() {
            return "ShardMetrics{shard="
                    + shardId
                    + ", reads="
                    + readCount
                    + ", writes="
                    + writeCount
                    + ", compactions="
                    + compactionCount
                    + ", errors="
                    + errorCount
                    + ", replLagBytes="
                    + replicationLagBytes
                    + "}";
        }
    }
}
