package io.github.rawwise.frontend.backend.ha;

import io.github.rawwise.backend.server.ha.HaLeaseInfo;
import io.github.rawwise.metastore.Metastore;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * Active BE Server address resolver for the Frontend.
 *
 * <p>Reads the current Active BE Server address from Metastore CAS key ({@code
 * rawwise/cluster/active-server}), for Frontend to use when creating Backend gRPC channel.
 *
 * <p>Resolution order:
 *
 * <ol>
 *   <li>HA enabled and Metastore has unexpired Active lease -> return lease.address()
 *   <li>Otherwise -> return empty (caller falls back to configured --backend-host/--backend-port)
 * </ol>
 *
 * <p>Failover awareness: periodically polls Metastore (at {@link #refreshIntervalMs} interval),
 * updates cache and logs when Active address changes. Current implementation only logs warnings,
 * actual gRPC channel switching requires caller to rebuild channel (future enhancement).
 */
@Slf4j
public class FrontendHaAddressResolver {

    private static final byte[] ACTIVE_SERVER_KEY =
            "rawwise/cluster/active-server".getBytes(StandardCharsets.UTF_8);

    private final Metastore metastore;
    private final long refreshIntervalMs;
    private final AtomicReference<Optional<String>> cachedAddress =
            new AtomicReference<>(Optional.empty());
    private ScheduledExecutorService scheduler;

    public FrontendHaAddressResolver(Metastore metastore) {
        this(metastore, 3_000L);
    }

    public FrontendHaAddressResolver(Metastore metastore, long refreshIntervalMs) {
        this.metastore = java.util.Objects.requireNonNull(metastore, "metastore must not be null");
        if (refreshIntervalMs <= 0) {
            throw new IllegalArgumentException(
                    "refreshIntervalMs must be > 0, got: " + refreshIntervalMs);
        }
        this.refreshIntervalMs = refreshIntervalMs;
    }

    /**
     * Read current Active BE Server address from Metastore.
     *
     * @return Active address (host:port), or empty if no Active or expired
     */
    public Optional<String> resolveActiveAddress() {
        try {
            Optional<byte[]> raw = metastore.get(ACTIVE_SERVER_KEY);
            if (raw.isEmpty()) {
                return Optional.empty();
            }
            HaLeaseInfo lease = HaLeaseInfo.deserialize(raw.get());
            if (lease.isExpired(System.currentTimeMillis())) {
                log.debug(
                        "resolveActiveAddress: lease expired, owner={} expireAtMs={}",
                        lease.ownerId(),
                        lease.expireAtMs());
                return Optional.empty();
            }
            return Optional.of(lease.address());
        } catch (Exception e) {
            log.warn("resolveActiveAddress: failed: {}", e.getMessage());
            return Optional.empty();
        }
    }

    /** Start periodic refresh thread. Logs when Active address changes. */
    public void startRefresh() {
        Optional<String> initial = resolveActiveAddress();
        cachedAddress.set(initial);
        log.info(
                "FrontendHaAddressResolver started — initial active address={}",
                initial.orElse("(none, will use fallback)"));

        scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "frontend-ha-resolver");
                            t.setDaemon(true);
                            return t;
                        });
        scheduler.scheduleAtFixedRate(
                this::refreshAndNotify,
                refreshIntervalMs,
                refreshIntervalMs,
                TimeUnit.MILLISECONDS);
    }

    private void refreshAndNotify() {
        try {
            Optional<String> latest = resolveActiveAddress();
            Optional<String> previous = cachedAddress.get();
            if (!latest.equals(previous)) {
                log.warn(
                        "Active BE Server address changed — previous={} latest={}",
                        previous.orElse("(none)"),
                        latest.orElse("(none)"));
                cachedAddress.set(latest);
            }
        } catch (Exception e) {
            log.debug("refreshAndNotify: failed: {}", e.getMessage());
        }
    }

    /**
     * Get cached Active address (last refresh result).
     *
     * @return cached Active address, or empty if no Active
     */
    public Optional<String> getCachedActiveAddress() {
        return cachedAddress.get();
    }

    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            try {
                if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                    log.warn("FrontendHaAddressResolver scheduler did not terminate in 2s");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("FrontendHaAddressResolver stopped");
    }
}
