package io.github.rawwise.backend.server.health;

import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.server.service.ClusterManagementServiceImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * BE Server failure detector.
 *
 * <p>Scans all registered BE Node lastHeartbeat timestamps every 10s. If an executor has not
 * reported heartbeat for 15s, it is deemed failed:
 *
 * <ol>
 *   <li>calls {@code routingCoordinator.onExecutorDown(executorId)} to trigger shard reassignment
 *   <li>removes the executor from the heartbeat table
 * </ol>
 *
 * <p>Deployment boundary: BE Server side component, created and started by {@code Main.java}.
 */
@Slf4j
public final class FailureDetector {

    private static final long DEFAULT_SCAN_INTERVAL_MS = 10_000L;
    private static final long DEFAULT_TIMEOUT_MS = 15_000L;

    private final ClusterManagementServiceImpl clusterService;
    private final RoutingCoordinator routingCoordinator;
    private final long scanIntervalMs;
    private final long timeoutMs;

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scanTask;

    public FailureDetector(
            ClusterManagementServiceImpl clusterService, RoutingCoordinator routingCoordinator) {
        this(clusterService, routingCoordinator, DEFAULT_SCAN_INTERVAL_MS, DEFAULT_TIMEOUT_MS);
    }

    public FailureDetector(
            ClusterManagementServiceImpl clusterService,
            RoutingCoordinator routingCoordinator,
            long scanIntervalMs,
            long timeoutMs) {
        this.clusterService = clusterService;
        this.routingCoordinator = routingCoordinator;
        this.scanIntervalMs = scanIntervalMs > 0 ? scanIntervalMs : DEFAULT_SCAN_INTERVAL_MS;
        this.timeoutMs = timeoutMs > 0 ? timeoutMs : DEFAULT_TIMEOUT_MS;
    }

    public void start() {
        if (scheduler != null) {
            log.warn("FailureDetector already started");
            return;
        }
        scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "failure-detector");
                            t.setDaemon(true);
                            return t;
                        });
        scanTask =
                scheduler.scheduleAtFixedRate(
                        this::scan, scanIntervalMs, scanIntervalMs, TimeUnit.MILLISECONDS);
        log.info(
                "FailureDetector started: scanInterval={}ms timeout={}ms",
                scanIntervalMs,
                timeoutMs);
    }

    void scan() {
        try {
            Set<String> executorIds = clusterService.getRegisteredExecutorIds();
            if (executorIds.isEmpty()) {
                return;
            }

            long now = System.currentTimeMillis();
            List<String> timedOut = new ArrayList<>();
            for (String executorId : executorIds) {
                Long lastHb = clusterService.getLastHeartbeatMs(executorId);
                if (lastHb == null) {
                    continue;
                }
                long elapsed = now - lastHb;
                if (elapsed > timeoutMs) {
                    timedOut.add(executorId);
                }
            }

            for (String executorId : timedOut) {
                log.warn(
                        "FailureDetector: executor {} timed out (elapsed={}ms > timeout={}ms), triggering shard reassignment",
                        executorId,
                        now - clusterService.getLastHeartbeatMs(executorId),
                        timeoutMs);
                try {
                    routingCoordinator.onExecutorDown(executorId);
                } catch (Exception e) {
                    log.error(
                            "FailureDetector: onExecutorDown failed for {}: {}",
                            executorId,
                            e.getMessage(),
                            e);
                }
                clusterService.removeHeartbeat(executorId);
            }
        } catch (Exception e) {
            log.error("FailureDetector scan error: {}", e.getMessage(), e);
        }
    }

    public void stop() {
        if (scanTask != null) {
            scanTask.cancel(false);
        }
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        log.info("FailureDetector stopped");
    }
}
