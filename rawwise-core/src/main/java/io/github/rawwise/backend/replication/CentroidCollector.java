package io.github.rawwise.backend.replication;

import io.github.rawwise.server.CentroidReportRequest;
import io.github.rawwise.server.CentroidReportResponse;
import io.github.rawwise.server.ClusterManagementServiceGrpc;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * BE Node centroid collector with periodic reporter.
 *
 * <p>Architecture:
 *
 * <pre>
 *   CentroidCollector (BE Node)
 * -> CentroidSource.computeCentroid() // compute local shard total centroid
 * -> ClusterManagementServiceBlockingStub.reportCentroid() // gRPC report BE Server
 * -> BE Server write Metastore // IvfRouter 30s refreshread
 * </pre>
 *
 * <p>Semantics:
 *
 * <ul>
 *   <li>default period is 60s (initial delay 60s to wait for index build)
 *   <li>centroid computation delegated to {@link CentroidSource} (called by implementation,
 *       decoupled from VectorIndex)
 *   <li>report failure does not block Node normal operation (best-effort, logs warning)
 *   <li>if vector count is 0, skip report (meaningless empty centroid)
 *   <li>vector count threshold triggers immediate report (optional; MVP phase uses periodic report
 *       only)
 * </ul>
 */
@Slf4j
public class CentroidCollector implements AutoCloseable {
    public static final long DEFAULT_PERIOD_SECONDS = 60L;
    public static final long DEFAULT_INITIAL_DELAY_SECONDS = 60L;

    private final CentroidSource centroidSource;
    private final ClusterManagementServiceGrpc.ClusterManagementServiceBlockingStub serverStub;
    private final String executorId;
    private final long periodSeconds;
    private final long initialDelaySeconds;
    private final ScheduledExecutorService executor;
    private volatile ScheduledFuture<?> task;
    private volatile boolean running;

    public CentroidCollector(
            CentroidSource centroidSource,
            ClusterManagementServiceGrpc.ClusterManagementServiceBlockingStub serverStub,
            String executorId) {
        this(
                centroidSource,
                serverStub,
                executorId,
                DEFAULT_PERIOD_SECONDS,
                DEFAULT_INITIAL_DELAY_SECONDS);
    }

    public CentroidCollector(
            CentroidSource centroidSource,
            ClusterManagementServiceGrpc.ClusterManagementServiceBlockingStub serverStub,
            String executorId,
            long periodSeconds,
            long initialDelaySeconds) {
        this.centroidSource = Objects.requireNonNull(centroidSource, "centroidSource");
        this.serverStub = Objects.requireNonNull(serverStub, "serverStub");
        this.executorId = Objects.requireNonNull(executorId, "executorId");
        this.periodSeconds = periodSeconds <= 0 ? DEFAULT_PERIOD_SECONDS : periodSeconds;
        this.initialDelaySeconds =
                initialDelaySeconds < 0 ? DEFAULT_INITIAL_DELAY_SECONDS : initialDelaySeconds;
        this.executor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "centroid-collector-" + executorId);
                            t.setDaemon(true);
                            return t;
                        });
    }

    public synchronized void start() {
        if (running) return;
        running = true;
        task =
                executor.scheduleAtFixedRate(
                        this::collectAndReport,
                        initialDelaySeconds,
                        periodSeconds,
                        TimeUnit.SECONDS);
        log.info(
                "CentroidCollector started: executorId={}, period={}s, initialDelay={}s",
                executorId,
                periodSeconds,
                initialDelaySeconds);
    }

    public synchronized void stop() {
        if (!running) return;
        running = false;
        if (task != null) task.cancel(false);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("CentroidCollector stopped: executorId={}", executorId);
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * Compute local shard centroid and report to BE Server.
     *
     * <p>Best-effort: on failure logs a warning, does not throw exception (does not block
     * scheduler).
     */
    public void collectAndReport() {
        try {
            CentroidData data = centroidSource.computeCentroid();
            if (data == null || data.vectorCount() == 0) {
                log.debug(
                        "CentroidCollector: skip report (no vectors) for executorId={}",
                        executorId);
                return;
            }
            CentroidReportRequest req =
                    CentroidReportRequest.newBuilder()
                            .setShardId(data.shardId())
                            .addAllCentroid(toFloatList(data.centroid()))
                            .setVectorCount(data.vectorCount())
                            .setTimestamp(System.currentTimeMillis())
                            .setExecutorId(executorId)
                            .build();
            CentroidReportResponse resp = serverStub.reportCentroid(req);
            if (resp.getAccepted()) {
                log.info(
                        "CentroidCollector: reported shard={} vectors={} executorId={}",
                        data.shardId(),
                        data.vectorCount(),
                        executorId);
            } else {
                log.warn("CentroidCollector: report rejected by server: {}", resp.getError());
            }
        } catch (Exception e) {
            log.warn(
                    "CentroidCollector: collectAndReport failed for executorId={}: {}",
                    executorId,
                    e.getMessage());
        }
    }

    private static List<Float> toFloatList(float[] arr) {
        List<Float> list = new java.util.ArrayList<>(arr.length);
        for (float v : arr) {
            list.add(v);
        }
        return list;
    }

    /** Centroid data source interface (implemented by BE Node, decoupled from VectorIndex). */
    public interface CentroidSource {
        /**
         * Compute local shard total centroid.
         *
         * @return centroid data, or null (no vectors or computation failed)
         */
        CentroidData computeCentroid();
    }

    /**
     * Centroid data.
     *
     * @param shardId shard identifier
     * @param centroid centroid vector (dimension mean)
     * @param vectorCount total vector count in shard
     */
    public record CentroidData(int shardId, float[] centroid, long vectorCount) {
        public CentroidData {
            Objects.requireNonNull(centroid, "centroid");
        }
    }
}
