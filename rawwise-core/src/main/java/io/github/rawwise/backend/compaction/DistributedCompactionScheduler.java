package io.github.rawwise.backend.compaction;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * Distributed compaction mutex lock scheduler.
 *
 * <p>Architecture:
 *
 * <pre>
 * BE Node trigger Compaction:
 *     -> DistributedCompactionScheduler.tryAcquireCompactionLock(shardId, executorId)
 * -> CAS: if shard has no lock or lock is expired -> acquire succeeds
 * -> otherwise -> acquire fails (another node is compacting)
 * -> during compaction, call renewLock() every 60s to renew
 * -> after compaction completes, call releaseLock() to free
 *
 * Failure recovery:
 * -> background thread scans every 60s; locks not renewed for 5min are auto-released
 * -> other nodes can acquire the expired lock and take over compaction
 * </pre>
 *
 * <p>Semantics:
 *
 * <ul>
 *   <li>only one node holds the compaction lock per shard
 *   <li>lock renewal interval 60s; failure/expiry threshold 5min (300s without renewal means
 *       failure)
 *   <li>once lock is freed, other nodes can acquire it
 *   <li>MVP uses in-memory lock store; production replaces with Metastore CAS
 * </ul>
 */
@Slf4j
public class DistributedCompactionScheduler implements AutoCloseable {
    public static final long DEFAULT_RENEW_INTERVAL_SECONDS = 60L;
    public static final long DEFAULT_LOCK_EXPIRY_SECONDS = 300L;
    public static final long DEFAULT_SCAN_INTERVAL_SECONDS = 60L;

    private final Map<Integer, CompactionLock> locks = new ConcurrentHashMap<>();
    private final long renewIntervalSeconds;
    private final long lockExpirySeconds;
    private final ScheduledExecutorService expiryScanner;
    private final ScheduledFuture<?> scanTask;

    public DistributedCompactionScheduler() {
        this(
                DEFAULT_RENEW_INTERVAL_SECONDS,
                DEFAULT_LOCK_EXPIRY_SECONDS,
                DEFAULT_SCAN_INTERVAL_SECONDS);
    }

    public DistributedCompactionScheduler(
            long renewIntervalSeconds, long lockExpirySeconds, long scanIntervalSeconds) {
        this.renewIntervalSeconds =
                renewIntervalSeconds <= 0 ? DEFAULT_RENEW_INTERVAL_SECONDS : renewIntervalSeconds;
        this.lockExpirySeconds =
                lockExpirySeconds <= 0 ? DEFAULT_LOCK_EXPIRY_SECONDS : lockExpirySeconds;
        this.expiryScanner =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "compaction-lock-scanner");
                            t.setDaemon(true);
                            return t;
                        });
        this.scanTask =
                expiryScanner.scheduleAtFixedRate(
                        this::scanExpiredLocks,
                        scanIntervalSeconds,
                        scanIntervalSeconds,
                        TimeUnit.SECONDS);
        log.info(
                "DistributedCompactionScheduler started: renew={}s, expiry={}s, scan={}s",
                this.renewIntervalSeconds,
                this.lockExpirySeconds,
                scanIntervalSeconds);
    }

    /**
     * Acquire shard compaction lock (CAS semantics).
     *
     * @param shardId shard identifier
     * @param executorId executorId requesting the lock
     * @return true if acquire succeeds; false if lock is held by another executor and not expired
     */
    public boolean tryAcquireCompactionLock(int shardId, String executorId) {
        Objects.requireNonNull(executorId, "executorId");
        long now = System.currentTimeMillis();
        long expiryTime = now + lockExpirySeconds * 1000L;
        CompactionLock newLock = new CompactionLock(shardId, executorId, now, expiryTime);
        CompactionLock previous = locks.putIfAbsent(shardId, newLock);
        if (previous == null) {
            log.info("Compaction lock acquired: shardId={}, executorId={}", shardId, executorId);
            return true;
        }
        if (previous.isExpired(now)) {
            boolean replaced = locks.replace(shardId, previous, newLock);
            if (replaced) {
                log.info(
                        "Compaction lock acquired (expired takeover): shardId={}, executorId={}, prevExecutor={}",
                        shardId,
                        executorId,
                        previous.executorId);
                return true;
            }
            return false;
        }
        if (previous.executorId.equals(executorId)) {
            boolean replaced = locks.replace(shardId, previous, newLock);
            if (replaced) {
                log.debug(
                        "Compaction lock re-acquired by same executor: shardId={}, executorId={}",
                        shardId,
                        executorId);
                return true;
            }
            return false;
        }
        log.debug(
                "Compaction lock busy: shardId={}, heldBy={}, executorId={}",
                shardId,
                previous.executorId,
                executorId);
        return false;
    }

    /**
     * Renew compaction lock (called periodically during compaction execution).
     *
     * @param shardId shard identifier
     * @param executorId executorId holding the lock
     * @return true if renewal succeeds; false if lock was already taken over by another executor
     */
    public boolean renewLock(int shardId, String executorId) {
        Objects.requireNonNull(executorId, "executorId");
        long now = System.currentTimeMillis();
        long newExpiryTime = now + lockExpirySeconds * 1000L;
        CompactionLock current = locks.get(shardId);
        if (current == null) {
            log.warn("Compaction lock renew failed: shardId={} lock not found", shardId);
            return false;
        }
        if (!current.executorId.equals(executorId)) {
            log.warn(
                    "Compaction lock renew failed: shardId={} heldBy={} requester={}",
                    shardId,
                    current.executorId,
                    executorId);
            return false;
        }
        CompactionLock renewed =
                new CompactionLock(shardId, executorId, current.acquiredAt, newExpiryTime);
        boolean success = locks.replace(shardId, current, renewed);
        if (success) {
            log.debug(
                    "Compaction lock renewed: shardId={}, executorId={}, newExpiry={}ms",
                    shardId,
                    executorId,
                    newExpiryTime);
        }
        return success;
    }

    /**
     * Release compaction lock (called after compaction completes).
     *
     * @param shardId shard identifier
     * @param executorId executorId holding the lock
     */
    public void releaseLock(int shardId, String executorId) {
        Objects.requireNonNull(executorId, "executorId");
        CompactionLock current = locks.get(shardId);
        if (current != null && current.executorId.equals(executorId)) {
            locks.remove(shardId, current);
            log.info("Compaction lock released: shardId={}, executorId={}", shardId, executorId);
        }
    }

    /** Query shard current lock state. */
    public Optional<CompactionLock> getLockState(int shardId) {
        CompactionLock lock = locks.get(shardId);
        if (lock != null && lock.isExpired(System.currentTimeMillis())) {
            return Optional.empty();
        }
        return Optional.ofNullable(lock);
    }

    /** Returns all active locks. */
    public Map<Integer, CompactionLock> getActiveLocks() {
        long now = System.currentTimeMillis();
        Map<Integer, CompactionLock> active = new ConcurrentHashMap<>();
        for (Map.Entry<Integer, CompactionLock> e : locks.entrySet()) {
            if (!e.getValue().isExpired(now)) {
                active.put(e.getKey(), e.getValue());
            }
        }
        return active;
    }

    private void scanExpiredLocks() {
        long now = System.currentTimeMillis();
        int expired = 0;
        for (Map.Entry<Integer, CompactionLock> e : locks.entrySet()) {
            CompactionLock lock = e.getValue();
            if (lock.isExpired(now)) {
                if (locks.remove(e.getKey(), lock)) {
                    expired++;
                    log.warn(
                            "Compaction lock expired (auto-release): shardId={}, executorId={}, expiredAt={}",
                            e.getKey(),
                            lock.executorId,
                            lock.expiryTime);
                }
            }
        }
        if (expired > 0) {
            log.info("Compaction lock scan: expired {} locks", expired);
        }
    }

    @Override
    public void close() {
        if (scanTask != null) scanTask.cancel(false);
        expiryScanner.shutdown();
        try {
            if (!expiryScanner.awaitTermination(5, TimeUnit.SECONDS)) {
                expiryScanner.shutdownNow();
            }
        } catch (InterruptedException e) {
            expiryScanner.shutdownNow();
            Thread.currentThread().interrupt();
        }
        locks.clear();
        log.info("DistributedCompactionScheduler closed");
    }

    /**
     * Compaction lock state.
     *
     * @param shardId shard identifier
     * @param executorId executorId holding the lock
     * @param acquiredAt acquire timestamp (millis)
     * @param expiryTime expiry timestamp (millis)
     */
    public record CompactionLock(int shardId, String executorId, long acquiredAt, long expiryTime) {
        public boolean isExpired(long now) {
            return now > expiryTime;
        }
    }
}
