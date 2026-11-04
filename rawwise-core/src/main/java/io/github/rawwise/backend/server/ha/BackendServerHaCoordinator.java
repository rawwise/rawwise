package io.github.rawwise.backend.server.ha;

import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * BE Server Active-Standby leader election coordinator.
 *
 * <p>Implements Active-Standby leader election via Metastore CAS, RTO < 10s (requires Metastore
 * availability).
 *
 * <p>Leader election flow:
 *
 * <ol>
 *   <li>BE Server calls {@link #tryAcquireActive()} on startup
 *   <li>Metastore key {@code rawwise/cluster/active-server} not present or expired -> CAS acquires
 *       leadership, becomes Active
 *   <li>CAS fails -> becomes Standby, periodically checks Active failure
 *   <li>Active periodically renews ({@link #renewIntervalMs} interval, {@link #leaseTtlMs} TTL)
 *   <li>Active renewal fails (CAS failure) -> degrades to Standby
 *   <li>Standby detects Active lease expiry -> CAS acquires leadership, epoch+1
 * </ol>
 *
 * <p>Split-Brain protection: epoch is unique. Standby acquires leadership with epoch+1, BE Nodes
 * via {@code acceptedEpoch} reject old Active commands (enforced by {@code
 * TxnCoordinatorGrpcService}).
 *
 * <p>Thread model:
 *
 * <ul>
 *   <li>Active mode: single-thread ScheduledExecutor for periodic renewal
 *   <li>Standby mode: single-thread ScheduledExecutor for periodic leadership acquire
 *   <li>mode is mutually exclusive, controlled by {@link #isActive}
 * </ul>
 */
@Slf4j
public class BackendServerHaCoordinator {

    /** Metastore key: stores current Active BE Server lease info. */
    static final byte[] ACTIVE_SERVER_KEY =
            "rawwise/cluster/active-server".getBytes(StandardCharsets.UTF_8);

    private final Metastore metastore;
    private final String ownerId;
    private final String advertiseAddress;
    private final long leaseTtlMs;
    private final long renewIntervalMs;
    private final long standbyPollIntervalMs;

    private final AtomicBoolean isActive = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong currentEpoch = new AtomicLong(0);
    private final AtomicReference<HaLeaseInfo> currentLease = new AtomicReference<>(null);
    private final Object leaseLock = new Object();

    private ScheduledExecutorService scheduler;

    /**
     * Default constructor (TTL=10s, renewal interval=3s, Standby poll=3s).
     *
     * @param metastore Metastore client
     * @param ownerId BE Server instance ID (unique)
     * @param advertiseAddress external service address (host:port)
     */
    public BackendServerHaCoordinator(
            Metastore metastore, String ownerId, String advertiseAddress) {
        this(metastore, ownerId, advertiseAddress, 10_000L, 3_000L, 3_000L);
    }

    public BackendServerHaCoordinator(
            Metastore metastore,
            String ownerId,
            String advertiseAddress,
            long leaseTtlMs,
            long renewIntervalMs,
            long standbyPollIntervalMs) {
        this.metastore = java.util.Objects.requireNonNull(metastore, "metastore must not be null");
        this.ownerId = java.util.Objects.requireNonNull(ownerId, "ownerId must not be null");
        this.advertiseAddress =
                java.util.Objects.requireNonNull(
                        advertiseAddress, "advertiseAddress must not be null");
        if (leaseTtlMs <= 0) {
            throw new IllegalArgumentException("leaseTtlMs must be > 0, got: " + leaseTtlMs);
        }
        if (renewIntervalMs <= 0 || renewIntervalMs >= leaseTtlMs) {
            throw new IllegalArgumentException(
                    "renewIntervalMs must be > 0 and < leaseTtlMs, got: " + renewIntervalMs);
        }
        if (standbyPollIntervalMs <= 0) {
            throw new IllegalArgumentException(
                    "standbyPollIntervalMs must be > 0, got: " + standbyPollIntervalMs);
        }
        this.leaseTtlMs = leaseTtlMs;
        this.renewIntervalMs = renewIntervalMs;
        this.standbyPollIntervalMs = standbyPollIntervalMs;
    }

    /**
     * Acquire leadership. CAS semantics:
     *
     * <ul>
     *   <li>key not present -> NotExists CAS, epoch=1
     *   <li>key present but expired -> ValueEquals(oldSerialized) CAS, epoch=old epoch+1
     *   <li>key in/atnotdeprecation → return false
     * </ul>
     *
     * @return true if leadership acquired successfully
     */
    public boolean tryAcquireActive() {
        synchronized (leaseLock) {
            try {
                Optional<byte[]> existing = metastore.get(ACTIVE_SERVER_KEY);
                long now = System.currentTimeMillis();
                long newExpireAtMs = now + leaseTtlMs;

                if (existing.isEmpty()) {
                    HaLeaseInfo newLease =
                            new HaLeaseInfo(ownerId, advertiseAddress, 1L, newExpireAtMs, 0L);
                    boolean success =
                            metastore.compareAndSwap(
                                    List.of(new Precondition.NotExists(ACTIVE_SERVER_KEY)),
                                    List.of(
                                            new Mutation.Put(
                                                    ACTIVE_SERVER_KEY, newLease.serialize())));
                    if (success) {
                        onAcquiredActive(newLease);
                        return true;
                    }
                    log.info(
                            "tryAcquireActive: NotExists CAS failed (concurrent acquire), becoming Standby");
                    return false;
                }

                HaLeaseInfo current = HaLeaseInfo.deserialize(existing.get());
                if (!current.isExpired(now)) {
                    log.debug(
                            "tryAcquireActive: current lease not expired, owner={} epoch={} expireIn={}ms",
                            current.ownerId(),
                            current.epoch(),
                            current.expireAtMs() - now);
                    return false;
                }

                HaLeaseInfo takeover = current.takeOver(ownerId, advertiseAddress, newExpireAtMs);
                boolean success =
                        metastore.compareAndSwap(
                                List.of(
                                        new Precondition.ValueEquals(
                                                ACTIVE_SERVER_KEY, existing.get())),
                                List.of(new Mutation.Put(ACTIVE_SERVER_KEY, takeover.serialize())));
                if (success) {
                    onAcquiredActive(takeover);
                    return true;
                }
                log.info(
                        "tryAcquireActive: ValueEquals CAS failed (concurrent takeover), becoming Standby");
                return false;
            } catch (Exception e) {
                log.error("tryAcquireActive: failed: {}", e.getMessage(), e);
                return false;
            }
        }
    }

    private void onAcquiredActive(HaLeaseInfo lease) {
        isActive.set(true);
        currentEpoch.set(lease.epoch());
        currentLease.set(lease);
        log.info(
                "Acquired ACTIVE role — ownerId={} address={} epoch={} expireAtMs={}",
                lease.ownerId(),
                lease.address(),
                lease.epoch(),
                lease.expireAtMs());
    }

    /**
     * Active periodic renewal. CAS ValueEquals(currentSerialized) -> update expireAtMs. CAS failure
     * (preempted or concurrent modification) -> degrades to Standby.
     *
     * @return true if renewal succeeds, false if degraded to Standby
     */
    public boolean renewLease() {
        synchronized (leaseLock) {
            HaLeaseInfo current = currentLease.get();
            if (current == null) {
                log.warn("renewLease: no current lease, not Active");
                return false;
            }
            try {
                long now = System.currentTimeMillis();
                HaLeaseInfo renewed = current.renew(now + leaseTtlMs);
                boolean success =
                        metastore.compareAndSwap(
                                List.of(
                                        new Precondition.ValueEquals(
                                                ACTIVE_SERVER_KEY, current.serialize())),
                                List.of(new Mutation.Put(ACTIVE_SERVER_KEY, renewed.serialize())));
                if (success) {
                    currentLease.set(renewed);
                    log.debug(
                            "renewLease: success, new expireAtMs={}, epoch={}",
                            renewed.expireAtMs(),
                            renewed.epoch());
                    return true;
                }
                log.warn(
                        "renewLease: CAS failed, lease was preempted or concurrently modified — demoting to Standby");
                demoteToStandby();
                return false;
            } catch (Exception e) {
                log.error("renewLease: failed: {} — demoting to Standby", e.getMessage(), e);
                demoteToStandby();
                return false;
            }
        }
    }

    private void demoteToStandby() {
        isActive.set(false);
        currentEpoch.set(0);
        currentLease.set(null);
        log.info("Demoted to STANDBY (was Active, lost lease)");
    }

    /**
     * Start the coordinator. If already Active, starts renewal thread; otherwise starts Standby
     * thread.
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            log.warn("BackendServerHaCoordinator already running");
            return;
        }
        scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "be-server-ha-coordinator");
                            t.setDaemon(true);
                            return t;
                        });

        if (isActive.get()) {
            startActiveRenewal();
        } else {
            startStandbyPolling();
        }
        log.info(
                "BackendServerHaCoordinator started — ownerId={} address={} isActive={}",
                ownerId,
                advertiseAddress,
                isActive.get());
    }

    private void startActiveRenewal() {
        scheduler.scheduleAtFixedRate(
                () -> {
                    try {
                        if (!renewLease()) {
                            log.warn("Active renewal failed, switching to Standby polling");
                            scheduler.shutdown();
                            if (running.get()) {
                                scheduler =
                                        Executors.newSingleThreadScheduledExecutor(
                                                r -> {
                                                    Thread t =
                                                            new Thread(
                                                                    r, "be-server-ha-coordinator");
                                                    t.setDaemon(true);
                                                    return t;
                                                });
                                startStandbyPolling();
                            }
                        }
                    } catch (Exception e) {
                        log.error("Active renewal task exception: {}", e.getMessage(), e);
                    }
                },
                renewIntervalMs,
                renewIntervalMs,
                TimeUnit.MILLISECONDS);
        log.info("Active renewal started — interval={}ms ttl={}ms", renewIntervalMs, leaseTtlMs);
    }

    private void startStandbyPolling() {
        scheduler.scheduleAtFixedRate(
                () -> {
                    try {
                        if (tryAcquireActive()) {
                            log.info("Standby acquired Active, switching to renewal mode");
                            scheduler.shutdown();
                            if (running.get()) {
                                scheduler =
                                        Executors.newSingleThreadScheduledExecutor(
                                                r -> {
                                                    Thread t =
                                                            new Thread(
                                                                    r, "be-server-ha-coordinator");
                                                    t.setDaemon(true);
                                                    return t;
                                                });
                                startActiveRenewal();
                            }
                        }
                    } catch (Exception e) {
                        log.error("Standby polling task exception: {}", e.getMessage(), e);
                    }
                },
                standbyPollIntervalMs,
                standbyPollIntervalMs,
                TimeUnit.MILLISECONDS);
        log.info("Standby polling started — interval={}ms", standbyPollIntervalMs);
    }

    /**
     * Stop the coordinator. If Active, voluntarily releases lease (delete key or write expiry
     * time).
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
            try {
                if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                    log.warn("BackendServerHaCoordinator scheduler did not terminate in 2s");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (isActive.get()) {
            voluntarilyReleaseLease();
        }
        log.info("BackendServerHaCoordinator stopped — ownerId={}", ownerId);
    }

    private void voluntarilyReleaseLease() {
        synchronized (leaseLock) {
            HaLeaseInfo current = currentLease.get();
            if (current == null) {
                return;
            }
            try {
                boolean success =
                        metastore.compareAndSwap(
                                List.of(
                                        new Precondition.ValueEquals(
                                                ACTIVE_SERVER_KEY, current.serialize())),
                                List.of(new Mutation.Delete(ACTIVE_SERVER_KEY)));
                if (success) {
                    log.info(
                            "Voluntarily released Active lease — ownerId={} epoch={}",
                            current.ownerId(),
                            current.epoch());
                } else {
                    log.warn(
                            "Voluntary release CAS failed (lease already changed) — ownerId={}",
                            ownerId);
                }
            } catch (Exception e) {
                log.warn("Voluntary release failed: {}", e.getMessage());
            }
            isActive.set(false);
            currentEpoch.set(0);
            currentLease.set(null);
        }
    }

    /**
     * Read current Active BE Server address (from Metastore CAS key). Frontend uses this to
     * discover the Active BE Server.
     *
     * @return Active BE Server address (host:port), or empty if no Active or expired
     */
    public Optional<String> getActiveAddress() {
        try {
            Optional<byte[]> raw = metastore.get(ACTIVE_SERVER_KEY);
            if (raw.isEmpty()) {
                return Optional.empty();
            }
            HaLeaseInfo lease = HaLeaseInfo.deserialize(raw.get());
            if (lease.isExpired(System.currentTimeMillis())) {
                return Optional.empty();
            }
            return Optional.of(lease.address());
        } catch (Exception e) {
            log.warn("getActiveAddress: failed: {}", e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Read current Active BE Server epoch (from Metastore CAS key). BE Nodes sync acceptedEpoch
     * during leader election.
     *
     * @return current Active epoch, or 0 if no Active or expired
     */
    public long getActiveEpoch() {
        try {
            Optional<byte[]> raw = metastore.get(ACTIVE_SERVER_KEY);
            if (raw.isEmpty()) {
                return 0L;
            }
            HaLeaseInfo lease = HaLeaseInfo.deserialize(raw.get());
            if (lease.isExpired(System.currentTimeMillis())) {
                return 0L;
            }
            return lease.epoch();
        } catch (Exception e) {
            log.warn("getActiveEpoch: failed: {}", e.getMessage());
            return 0L;
        }
    }

    public boolean isActive() {
        return isActive.get();
    }

    public long getCurrentEpoch() {
        return currentEpoch.get();
    }

    public String getOwnerId() {
        return ownerId;
    }

    public String getAdvertiseAddress() {
        return advertiseAddress;
    }
}
