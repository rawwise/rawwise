package io.github.rawwise.backend.server.service;

import com.google.protobuf.Duration;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import io.github.rawwise.backend.server.job.SparkJobScheduler;
import io.github.rawwise.governance.metrics.RawWiseMetrics;
import io.github.rawwise.server.AlertRule;
import io.github.rawwise.server.ComponentHealth;
import io.github.rawwise.server.HealthStatus;
import io.github.rawwise.server.MetricDataPoint;
import io.github.rawwise.server.MetricSubscription;
import io.github.rawwise.server.MonitorServiceGrpc;
import io.github.rawwise.server.PerformanceReport;
import io.github.rawwise.server.ReportRequest;
import io.github.rawwise.server.SlowQueryInfo;
import io.github.rawwise.server.SlowQueryReport;
import io.github.rawwise.server.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class MonitorServiceImpl extends MonitorServiceGrpc.MonitorServiceImplBase {

    private static final long SLOW_QUERY_THRESHOLD_MS = 5000;

    private final SparkJobScheduler jobScheduler;
    private final SparkSession spark;
    private final ScheduledExecutorService metricsScheduler;

    public MonitorServiceImpl(SparkJobScheduler jobScheduler, SparkSession spark) {
        this.jobScheduler = jobScheduler;
        this.spark = spark;
        this.metricsScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "monitor-metrics-publisher");
                            t.setDaemon(true);
                            return t;
                        });
    }

    @Override
    public void getHealthStatus(Empty request, StreamObserver<HealthStatus> responseObserver) {
        try {
            HealthStatus.Builder builder = HealthStatus.newBuilder();
            Timestamp now =
                    Timestamp.newBuilder()
                            .setSeconds(System.currentTimeMillis() / 1000)
                            .setNanos((int) ((System.currentTimeMillis() % 1000) * 1_000_000))
                            .build();
            builder.setCheckTime(now);

            boolean anyDown = false;
            boolean anyDegraded = false;

            ComponentHealth sparkHealth = checkSpark();
            builder.putComponents("spark", sparkHealth);
            if (sparkHealth.getStatus() == ComponentHealth.Status.DOWN) anyDown = true;
            if (sparkHealth.getStatus() == ComponentHealth.Status.UNKNOWN) anyDegraded = true;

            ComponentHealth jobSchedulerHealth = checkJobScheduler();
            builder.putComponents("job_scheduler", jobSchedulerHealth);
            if (jobSchedulerHealth.getStatus() == ComponentHealth.Status.DOWN) anyDown = true;

            ComponentHealth metricsHealth = checkMetrics();
            builder.putComponents("metrics", metricsHealth);
            if (metricsHealth.getStatus() == ComponentHealth.Status.UNKNOWN) anyDegraded = true;

            Runtime runtime = Runtime.getRuntime();
            long heapUsed = runtime.totalMemory() - runtime.freeMemory();
            long heapMax = runtime.maxMemory();
            double heapUsage = heapMax > 0 ? (double) heapUsed / heapMax : 0.0;
            if (heapUsage > 0.9) {
                anyDegraded = true;
                builder.addIssues(
                        "Heap usage above 90%: " + String.format("%.1f%%", heapUsage * 100));
            }

            if (anyDown) {
                builder.setStatus(HealthStatus.Status.UNHEALTHY);
            } else if (anyDegraded) {
                builder.setStatus(HealthStatus.Status.DEGRADED);
            } else {
                builder.setStatus(HealthStatus.Status.HEALTHY);
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Failed to get health status", e);
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("Health check failed: " + e.getMessage())
                            .asRuntimeException());
        }
    }

    @Override
    public void getSlowQueryReport(
            ReportRequest request, StreamObserver<SlowQueryReport> responseObserver) {
        try {
            long nowMs = System.currentTimeMillis();
            Timestamp now =
                    Timestamp.newBuilder()
                            .setSeconds(nowMs / 1000)
                            .setNanos((int) ((nowMs % 1000) * 1_000_000))
                            .build();

            SlowQueryReport.Builder reportBuilder =
                    SlowQueryReport.newBuilder().setGenerationTime(now);

            Map<String, Integer> byUser = new HashMap<>();
            Map<String, Integer> byTable = new HashMap<>();

            if (jobScheduler != null) {
                List<SparkJobScheduler.CompletedJobSummary> completed =
                        jobScheduler.getCompletedJobSummaries();
                long thresholdMs = SLOW_QUERY_THRESHOLD_MS;

                List<SparkJobScheduler.CompletedJobSummary> slowQueries =
                        completed.stream()
                                .filter(j -> j.durationMs() > thresholdMs)
                                .sorted((a, b) -> Long.compare(b.durationMs(), a.durationMs()))
                                .limit(request.getLimit() > 0 ? request.getLimit() : 50)
                                .toList();

                for (SparkJobScheduler.CompletedJobSummary job : slowQueries) {
                    SlowQueryInfo info =
                            SlowQueryInfo.newBuilder()
                                    .setQueryId(job.jobId())
                                    .setSql(job.sql())
                                    .setDuration(
                                            Duration.newBuilder()
                                                    .setSeconds(job.durationMs() / 1000)
                                                    .setNanos(
                                                            (int)
                                                                    ((job.durationMs() % 1000)
                                                                            * 1_000_000))
                                                    .build())
                                    .setExecutionTime(
                                            Timestamp.newBuilder()
                                                    .setSeconds(job.endTime() / 1000)
                                                    .setNanos(
                                                            (int)
                                                                    ((job.endTime() % 1000)
                                                                            * 1_000_000))
                                                    .build())
                                    .addIssues(
                                            "Execution time exceeded threshold ("
                                                    + thresholdMs
                                                    + "ms)")
                                    .build();
                    reportBuilder.addSlowQueries(info);

                    String table = extractTableName(job.sql());
                    if (table != null) {
                        byTable.merge(table, 1, Integer::sum);
                    }
                }

                reportBuilder.setTotalSlowQueries(slowQueries.size());
            }

            reportBuilder.putAllSlowQueriesByUser(byUser);
            reportBuilder.putAllSlowQueriesByTable(byTable);

            responseObserver.onNext(reportBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Failed to get slow query report", e);
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("Slow query report failed: " + e.getMessage())
                            .asRuntimeException());
        }
    }

    @Override
    public void subscribeMetrics(
            MetricSubscription request, StreamObserver<MetricDataPoint> responseObserver) {
        long sampleIntervalMs =
                request.getSampleInterval().getSeconds() * 1000
                        + request.getSampleInterval().getNanos() / 1_000_000;
        if (sampleIntervalMs <= 0) {
            sampleIntervalMs = 5000;
        }
        long durationMs =
                request.getDuration().getSeconds() * 1000
                        + request.getDuration().getNanos() / 1_000_000;
        if (durationMs <= 0) {
            durationMs = 60_000;
        }

        long finalSampleIntervalMs = sampleIntervalMs;
        long endTimeMs = System.currentTimeMillis() + durationMs;
        List<String> metricNames = request.getMetricNamesList();

        metricsScheduler.scheduleAtFixedRate(
                () -> {
                    try {
                        if (System.currentTimeMillis() > endTimeMs) {
                            responseObserver.onCompleted();
                            return;
                        }

                        MetricDataPoint.Builder pointBuilder = MetricDataPoint.newBuilder();
                        long nowMs = System.currentTimeMillis();
                        pointBuilder.setTimestamp(
                                Timestamp.newBuilder()
                                        .setSeconds(nowMs / 1000)
                                        .setNanos((int) ((nowMs % 1000) * 1_000_000))
                                        .build());

                        if (RawWiseMetrics.isInitialized()) {
                            MeterRegistry registry = RawWiseMetrics.registry();
                            for (String metricName : metricNames) {
                                registry.find(metricName)
                                        .timers()
                                        .forEach(
                                                timer -> {
                                                    pointBuilder.putValues(
                                                            metricName + ".count",
                                                            (double) timer.count());
                                                    pointBuilder.putValues(
                                                            metricName + ".mean",
                                                            timer.mean(
                                                                    java.util.concurrent.TimeUnit
                                                                            .MILLISECONDS));
                                                });
                                registry.find(metricName)
                                        .counters()
                                        .forEach(
                                                counter -> {
                                                    pointBuilder.putValues(
                                                            metricName, (double) counter.count());
                                                });
                                registry.find(metricName)
                                        .gauges()
                                        .forEach(
                                                gauge -> {
                                                    Double value = gauge.value();
                                                    if (value != null && Double.isFinite(value)) {
                                                        pointBuilder.putValues(metricName, value);
                                                    }
                                                });
                            }
                        }

                        pointBuilder.putLabels("source", "backend");
                        if (jobScheduler != null) {
                            pointBuilder.putLabels(
                                    "active_jobs",
                                    String.valueOf(jobScheduler.getActiveJobs().size()));
                            pointBuilder.putLabels(
                                    "queue_length", String.valueOf(jobScheduler.getQueueLength()));
                        }

                        responseObserver.onNext(pointBuilder.build());
                    } catch (Exception e) {
                        log.error("Error publishing metrics subscription data", e);
                    }
                },
                0,
                finalSampleIntervalMs,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void setAlertRule(AlertRule request, StreamObserver<Status> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("MonitorService.setAlertRule not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void getPerformanceReport(
            ReportRequest request, StreamObserver<PerformanceReport> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("MonitorService.getPerformanceReport not yet implemented")
                        .asRuntimeException());
    }

    public void shutdown() {
        metricsScheduler.shutdown();
        try {
            if (!metricsScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                metricsScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            metricsScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private ComponentHealth checkSpark() {
        ComponentHealth.Builder builder = ComponentHealth.newBuilder();
        long startMs = System.currentTimeMillis();
        try {
            if (spark == null || spark.sparkContext().isStopped()) {
                builder.setStatus(ComponentHealth.Status.DOWN)
                        .setMessage("SparkSession not available");
            } else {
                builder.setStatus(ComponentHealth.Status.UP).setMessage("SparkSession active");
            }
        } catch (Exception e) {
            builder.setStatus(ComponentHealth.Status.DOWN)
                    .setMessage("Spark check failed: " + e.getMessage());
        }
        long elapsedMs = System.currentTimeMillis() - startMs;
        builder.setResponseTime(
                Duration.newBuilder()
                        .setSeconds(elapsedMs / 1000)
                        .setNanos((int) ((elapsedMs % 1000) * 1_000_000))
                        .build());
        builder.setLastCheck(
                Timestamp.newBuilder()
                        .setSeconds(System.currentTimeMillis() / 1000)
                        .setNanos((int) ((System.currentTimeMillis() % 1000) * 1_000_000))
                        .build());
        return builder.build();
    }

    private ComponentHealth checkJobScheduler() {
        ComponentHealth.Builder builder = ComponentHealth.newBuilder();
        long startMs = System.currentTimeMillis();
        try {
            if (jobScheduler == null) {
                builder.setStatus(ComponentHealth.Status.DOWN)
                        .setMessage("JobScheduler not available");
            } else {
                int activeJobs = jobScheduler.getActiveJobs().size();
                int queueLength = jobScheduler.getQueueLength();
                builder.setStatus(ComponentHealth.Status.UP)
                        .setMessage(
                                "JobScheduler active, jobs="
                                        + activeJobs
                                        + ", queue="
                                        + queueLength);
            }
        } catch (Exception e) {
            builder.setStatus(ComponentHealth.Status.DOWN)
                    .setMessage("JobScheduler check failed: " + e.getMessage());
        }
        long elapsedMs = System.currentTimeMillis() - startMs;
        builder.setResponseTime(
                Duration.newBuilder()
                        .setSeconds(elapsedMs / 1000)
                        .setNanos((int) ((elapsedMs % 1000) * 1_000_000))
                        .build());
        builder.setLastCheck(
                Timestamp.newBuilder()
                        .setSeconds(System.currentTimeMillis() / 1000)
                        .setNanos((int) ((System.currentTimeMillis() % 1000) * 1_000_000))
                        .build());
        return builder.build();
    }

    private ComponentHealth checkMetrics() {
        ComponentHealth.Builder builder = ComponentHealth.newBuilder();
        long startMs = System.currentTimeMillis();
        try {
            if (RawWiseMetrics.isInitialized()) {
                builder.setStatus(ComponentHealth.Status.UP)
                        .setMessage("Micrometer registry initialized");
            } else {
                builder.setStatus(ComponentHealth.Status.UNKNOWN)
                        .setMessage("Micrometer registry not initialized");
            }
        } catch (Exception e) {
            builder.setStatus(ComponentHealth.Status.UNKNOWN)
                    .setMessage("Metrics check failed: " + e.getMessage());
        }
        long elapsedMs = System.currentTimeMillis() - startMs;
        builder.setResponseTime(
                Duration.newBuilder()
                        .setSeconds(elapsedMs / 1000)
                        .setNanos((int) ((elapsedMs % 1000) * 1_000_000))
                        .build());
        builder.setLastCheck(
                Timestamp.newBuilder()
                        .setSeconds(System.currentTimeMillis() / 1000)
                        .setNanos((int) ((System.currentTimeMillis() % 1000) * 1_000_000))
                        .build());
        return builder.build();
    }

    private String extractTableName(String sql) {
        if (sql == null) return null;
        String upper = sql.toUpperCase().trim();
        if (upper.startsWith("FROM ")) {
            String rest = sql.substring(5).trim();
            int spaceIdx = rest.indexOf(' ');
            return spaceIdx > 0 ? rest.substring(0, spaceIdx) : rest;
        }
        if (upper.startsWith("SELECT")
                || upper.startsWith("INSERT")
                || upper.startsWith("UPDATE")
                || upper.startsWith("DELETE")) {
            for (String keyword : new String[] {" FROM ", " INTO ", " UPDATE "}) {
                int idx = upper.indexOf(keyword);
                if (idx >= 0) {
                    String rest = sql.substring(idx + keyword.length()).trim();
                    int spaceIdx = rest.indexOf(' ');
                    return spaceIdx > 0 ? rest.substring(0, spaceIdx) : rest;
                }
            }
        }
        return null;
    }
}
