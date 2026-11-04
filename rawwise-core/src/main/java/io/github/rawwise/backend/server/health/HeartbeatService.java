package io.github.rawwise.backend.server.health;

import io.github.rawwise.server.ClusterManagementServiceGrpc;
import io.github.rawwise.server.HeartbeatRequest;
import io.github.rawwise.server.HeartbeatResponse;
import io.github.rawwise.server.Status;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * BE Node heartbeat reporting service.
 *
 * <p>After BE Node startup, this service sends {@code HeartbeatRequest} to BE Server every 5s. BE
 * Server {@code FailureDetector} scans lastHeartbeat; if no report for 15s, node is deemed failed.
 *
 * <p>Deployment boundary: BE Node side component, created and started by {@code NodeModeLauncher}.
 */
@Slf4j
public final class HeartbeatService {

    private static final long DEFAULT_INTERVAL_MS = 5000L;
    private static final long MAX_CONSECUTIVE_FAILURES = 3;

    private final ClusterManagementServiceGrpc.ClusterManagementServiceBlockingStub clusterStub;
    private final String executorId;
    private final String nodeId;
    private final int shardId;
    private final long intervalMs;

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> heartbeatTask;
    private volatile int consecutiveFailures = 0;

    public HeartbeatService(
            ClusterManagementServiceGrpc.ClusterManagementServiceBlockingStub clusterStub,
            String executorId,
            String nodeId,
            int shardId) {
        this(clusterStub, executorId, nodeId, shardId, DEFAULT_INTERVAL_MS);
    }

    public HeartbeatService(
            ClusterManagementServiceGrpc.ClusterManagementServiceBlockingStub clusterStub,
            String executorId,
            String nodeId,
            int shardId,
            long intervalMs) {
        this.clusterStub = clusterStub;
        this.executorId = executorId;
        this.nodeId = nodeId;
        this.shardId = shardId;
        this.intervalMs = intervalMs > 0 ? intervalMs : DEFAULT_INTERVAL_MS;
    }

    public void start() {
        if (scheduler != null) {
            log.warn("HeartbeatService already started for executor={}", executorId);
            return;
        }
        scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "heartbeat-" + executorId);
                            t.setDaemon(true);
                            return t;
                        });
        heartbeatTask =
                scheduler.scheduleAtFixedRate(
                        this::sendHeartbeat, 0, intervalMs, TimeUnit.MILLISECONDS);
        log.info("HeartbeatService started: executor={} interval={}ms", executorId, intervalMs);
    }

    private void sendHeartbeat() {
        try {
            HeartbeatRequest request =
                    HeartbeatRequest.newBuilder()
                            .setExecutorId(executorId)
                            .setNodeId(nodeId)
                            .setShardId(shardId)
                            .setTimestampMs(System.currentTimeMillis())
                            .build();
            HeartbeatResponse response =
                    clusterStub.withDeadlineAfter(3, TimeUnit.SECONDS).heartbeat(request);
            if (response.getStatus() != null && response.getStatus().getCode() == Status.Code.OK) {
                if (consecutiveFailures > 0) {
                    log.info(
                            "Heartbeat recovered after {} consecutive failures: executor={}",
                            consecutiveFailures,
                            executorId);
                }
                consecutiveFailures = 0;
            } else {
                consecutiveFailures++;
                log.warn(
                        "Heartbeat returned non-OK status: executor={} failures={}",
                        executorId,
                        consecutiveFailures);
            }
        } catch (Exception e) {
            consecutiveFailures++;
            if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
                log.error(
                        "Heartbeat failed {} consecutive times: executor={} error={}",
                        consecutiveFailures,
                        executorId,
                        e.getMessage());
            } else {
                log.debug(
                        "Heartbeat failed (attempt {}): executor={} error={}",
                        consecutiveFailures,
                        executorId,
                        e.getMessage());
            }
        }
    }

    public void stop() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
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
        log.info("HeartbeatService stopped: executor={}", executorId);
    }
}
