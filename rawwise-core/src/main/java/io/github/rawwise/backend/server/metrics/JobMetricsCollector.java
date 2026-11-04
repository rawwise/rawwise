package io.github.rawwise.backend.server.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.rawwise.backend.server.job.JobStatus;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.*;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

@Slf4j
public class JobMetricsCollector extends SparkListener {

    private final SparkSession spark;
    private final MetricRegistry metricRegistry;
    private final Map<String, JobMetrics> jobMetrics;
    private final Map<String, StageMetrics> stageMetrics;
    private final Map<String, TaskMetricsData> taskMetrics;
    private final Map<String, ExecutorMetrics> executorMetrics;

    private final ScheduledExecutorService scheduler;
    private final List<Consumer<JobMetrics>> metricsConsumers;

    // Cache related
    private final Cache<String, CachedMetrics> metricsCache;
    private final long cacheExpiryMs;

    // Statistics related
    private final AtomicLong totalJobsProcessed;
    private final AtomicLong totalTasksProcessed;
    private final AtomicLong totalBytesProcessed;

    private final boolean persistEnabled;

    public JobMetricsCollector(SparkSession spark) {
        this(spark, true);
    }

    public JobMetricsCollector(SparkSession spark, boolean persistEnabled) {
        this.spark = spark;
        this.metricRegistry = new MetricRegistry();
        this.jobMetrics = new ConcurrentHashMap<>();
        this.stageMetrics = new ConcurrentHashMap<>();
        this.taskMetrics = new ConcurrentHashMap<>();
        this.executorMetrics = new ConcurrentHashMap<>();

        this.scheduler = Executors.newScheduledThreadPool(2);
        this.metricsConsumers = new CopyOnWriteArrayList<>();

        this.cacheExpiryMs = TimeUnit.MINUTES.toMillis(5);
        this.metricsCache =
                Caffeine.newBuilder()
                        .maximumSize(1000)
                        .expireAfterWrite(cacheExpiryMs, TimeUnit.MILLISECONDS)
                        .build();

        this.totalJobsProcessed = new AtomicLong(0);
        this.totalTasksProcessed = new AtomicLong(0);
        this.totalBytesProcessed = new AtomicLong(0);

        this.persistEnabled = persistEnabled;

        registerSparkListener();
        initializeMetrics();
        startScheduledTasks();
    }

    public JobMetrics collect(String jobId) {
        // First check cache
        CachedMetrics cached = metricsCache.getIfPresent(jobId);
        if (cached != null && !cached.isExpired()) {
            return cached.metrics;
        }

        // Collect metrics from various data sources
        JobMetrics metrics = new JobMetrics();
        metrics.setJobId(jobId);

        // 1. Collect from Spark listener
        JobMetrics jobData = jobMetrics.get(jobId);
        if (jobData != null) {
            List<StageMetrics> stageMetricsList = collectStageMetrics(jobId);
            jobData.setStages(stageMetricsList);

            // Collect executor-level metrics
            List<ExecutorMetrics> executorMetricsList = collectExecutorMetrics(jobId);
            jobData.setExecutors(executorMetricsList);
        }

        // 2. Collect from Spark REST API (more detailed info)
        try {
            enrichWithSparkApiMetrics(jobId, metrics);
        } catch (Exception e) {
            // Ignore API errors, use existing data
            log.error("Failed to enrich metrics from Spark API: {}", e.getMessage());
        }

        // 3. Collect system resource metrics
        collectSystemMetrics(metrics);

        // 4. Calculate derived metrics
        calculateDerivedMetrics(metrics);

        // Cache result
        metricsCache.put(jobId, new CachedMetrics(metrics, System.currentTimeMillis()));

        // Notify consumers
        notifyConsumers(metrics);

        return metrics;
    }

    public void registerConsumer(Consumer<JobMetrics> consumer) {
        metricsConsumers.add(consumer);
    }

    public MetricsReport getMetricsReport() {
        MetricsReport report = new MetricsReport();
        report.setTimestamp(System.currentTimeMillis());
        report.setTotalJobsProcessed(totalJobsProcessed.get());
        report.setTotalTasksProcessed(totalTasksProcessed.get());
        report.setTotalBytesProcessed(totalBytesProcessed.get());

        // Job statistics
        Map<JobStatus, Integer> jobStats = new HashMap<>();
        for (JobMetrics job : jobMetrics.values()) {
            jobStats.merge(job.getStatus(), 1, Integer::sum);
        }
        report.setJobStatistics(jobStats);

        // Executor statistics
        report.setExecutorCount(executorMetrics.size());

        // Performance metrics
        report.setAverageJobDuration(calculateAverageJobDuration());
        report.setThroughputBytesPerSec(calculateCurrentThroughput());
        report.setTaskSuccessRate(calculateTaskSuccessRate());

        return report;
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        String jobId = String.valueOf(jobStart.jobId());

        JobMetrics jobData = new JobMetrics();
        jobData.setJobId(jobId);
        jobData.setSubmitTime(System.currentTimeMillis());
        jobData.setStartTime(jobStart.time());
        jobData.setStatus(JobStatus.RUNNING);
        // todo verify correctness
        jobData.setTotalTasks(jobStart.stageInfos().size());
        jobMetrics.put(jobId, jobData);
        totalJobsProcessed.incrementAndGet();

        // Record Stage information
        for (StageInfo stageInfo : JavaConverters.seqAsJavaList(jobStart.stageInfos())) {
            StageMetrics stageData = new StageMetrics();
            stageData.setStageId(stageInfo.stageId());
            stageData.setStageName(stageInfo.name());
            stageData.setJobId(jobId);
            stageData.setNumTasks(stageInfo.numTasks());
            stageData.setSubmitTime(System.currentTimeMillis());
            stageMetrics.put(String.valueOf(stageInfo.stageId()), stageData);
        }

        // Update metrics
        metricRegistry.counter("jobs.started").inc();
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        String jobId = String.valueOf(jobEnd.jobId());
        JobMetrics jobData = jobMetrics.get(jobId);

        if (jobData != null) {
            jobData.setEndTime(jobEnd.time());
            jobData.setDuration(jobEnd.time() - jobData.getStartTime());

            JobResult jobResult = jobEnd.jobResult();
            if (jobResult == null) {
                throw new IllegalArgumentException("Job result is null");
            } else if (jobResult instanceof org.apache.spark.scheduler.JobSucceeded$) {
                jobData.setStatus(JobStatus.SUCCEEDED);
                metricRegistry.counter("jobs.succeeded").inc();
            } else if (jobResult instanceof org.apache.spark.scheduler.JobFailed) {
                jobData.setStatus(JobStatus.FAILED);
                metricRegistry.counter("jobs.failed").inc();
            } else {
                throw new IllegalStateException(
                        "Unknown JobResult: " + jobResult.getClass().getName());
            }

            // Update histogram
            metricRegistry.histogram("job.duration").update(jobData.getDuration());
        }
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        StageInfo stageInfo = stageSubmitted.stageInfo();
        StageMetrics stageData = stageMetrics.get(String.valueOf(stageInfo.stageId()));
        if (stageData != null) {
            stageData.setSubmitTime((long) stageSubmitted.stageInfo().submissionTime().get());
        }
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        StageInfo stageInfo = stageCompleted.stageInfo();
        StageMetrics stageData = stageMetrics.get(String.valueOf(stageInfo.stageId()));

        if (stageData != null) {
            stageData.setCompletionTime((long) stageInfo.completionTime().get());
            stageData.setDuration(stageData.getCompletionTime() - stageData.getSubmitTime());
            // stageData.completedTasks = stageInfo.numTasks() -
            // stageInfo.numFailedTasks() -
            // stageInfo.numActiveTasks();
            // stageData.failedTasks = stageInfo.numFailedTasks();

            // Collect Stage metrics
            stageData.setInputBytes(stageInfo.taskMetrics().inputMetrics().bytesRead());
            stageData.setOutputBytes(stageInfo.taskMetrics().outputMetrics().bytesWritten());
            stageData.setShuffleReadBytes(
                    stageInfo.taskMetrics().shuffleReadMetrics().totalBytesRead());
            stageData.setShuffleWriteBytes(
                    stageInfo.taskMetrics().shuffleWriteMetrics().bytesWritten());

            // Update histogram
            metricRegistry.histogram("stage.duration").update(stageData.getDuration());

            // Update global statistics
            totalBytesProcessed.addAndGet(stageData.getInputBytes() + stageData.getOutputBytes());
        }
    }

    @Override
    public void onTaskStart(SparkListenerTaskStart taskStart) {
        String taskId = String.format("%d-%d", taskStart.stageId(), taskStart.taskInfo().taskId());
        TaskMetricsData taskData = new TaskMetricsData();
        taskData.setTaskId(taskId);
        taskData.setStageId(taskStart.stageId());
        taskData.setExecutorId(taskStart.taskInfo().executorId());
        taskData.setStatus("RUNNING");
        taskData.setSubmitTime(taskStart.taskInfo().launchTime());

        taskMetrics.put(taskId, taskData);
        totalTasksProcessed.incrementAndGet();

        metricRegistry.counter("tasks.started").inc();
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        String taskId = String.format("%d-%d", taskEnd.stageId(), taskEnd.taskInfo().taskId());
        TaskMetricsData taskData = taskMetrics.get(taskId);

        if (taskData != null) {
            taskData.setCompletedTime(taskEnd.taskInfo().finishTime());
            taskData.setDuration(taskData.getCompletedTime() - taskData.getSubmitTime());
            taskData.setStatus(taskEnd.taskInfo().status());

            // Collect Task metrics
            TaskMetrics metrics = taskEnd.taskMetrics();
            if (metrics != null) {
                taskData.setGcTime(metrics.jvmGCTime());
                taskData.setResultSerializationTime(metrics.resultSerializationTime());
                taskData.setBytesRead(metrics.inputMetrics().bytesRead());
                taskData.setBytesWritten(metrics.outputMetrics().bytesWritten());
                taskData.setRecordsRead(metrics.inputMetrics().recordsRead());
                taskData.setRecordsWritten(metrics.outputMetrics().recordsWritten());
                taskData.setShuffleReadBytes(metrics.shuffleReadMetrics().totalBytesRead());
                taskData.setShuffleWriteBytes(metrics.shuffleWriteMetrics().bytesWritten());
                taskData.setMemoryBytesSpilled(metrics.memoryBytesSpilled());
                taskData.setDiskBytesSpilled(metrics.diskBytesSpilled());
                taskData.setPeakExecutionMemory(metrics.peakExecutionMemory());
            }

            if ("SUCCESS".equals(taskData.getStatus())) {
                metricRegistry.counter("tasks.succeeded").inc();
                metricRegistry.histogram("task.duration").update(taskData.getDuration());
                metricRegistry.meter("bytes.processed.rate").mark(taskData.getBytesRead());
            } else {
                metricRegistry.counter("tasks.failed").inc();
            }
        }
    }

    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
        String executorId = executorAdded.executorId();
        ExecutorMetrics executorData = new ExecutorMetrics();
        executorData.setId(executorId);
        executorData.setHost(executorAdded.executorInfo().executorHost());
        // todo total memory
        // executorData.setTotalMemoryMb();
        executorData.setTotalCores(executorAdded.executorInfo().totalCores());
        executorData.setRegisterTime(System.currentTimeMillis());
        executorData.setLastHeartbeat(System.currentTimeMillis());
        executorMetrics.put(executorId, executorData);
        metricRegistry.counter("executors.added").inc();
    }

    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
        String executorId = executorRemoved.executorId();
        executorMetrics.remove(executorId);
        metricRegistry.counter("executors.removed").inc();
    }

    @Override
    public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate metricsUpdate) {
        String executorId = metricsUpdate.execId();
        ExecutorMetrics executorData = executorMetrics.get(executorId);

        if (executorData != null) {

            // executorData.setUsedMemoryMb();metricsUpdate.executorUpdates().usedOnHeapStorageMemory() / (1024 * 1024);
            executorData.setLastHeartbeat(System.currentTimeMillis());

            // Update other metrics, need to extract more info from metricsUpdate
        }
    }

    private long calculateAverageJobDuration() {
        long totalDuration = 0;
        int count = 0;

        for (JobMetrics job : jobMetrics.values()) {
            if (job.getDuration() > 0) {
                totalDuration += job.getDuration();
                count++;
            }
        }

        return count > 0 ? totalDuration / count : 0;
    }

    private double calculateCurrentThroughput() {
        // Calculate throughput for the last 5 minutes
        long fiveMinutesAgo = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5);

        long bytesProcessed = 0;
        for (JobMetrics job : jobMetrics.values()) {
            if (job.getEndTime() > fiveMinutesAgo) {
                // Need to calculate bytes based on actual situation here
                // bytesProcessed += job.bytesProcessed;
            }
        }

        return bytesProcessed / (5 * 60.0); // bytes per second
    }

    private double calculateTaskSuccessRate() {
        long totalTasks = totalTasksProcessed.get();
        long failedTasks =
                taskMetrics.values().stream()
                        .filter(task -> "FAILED".equals(task.getStatus()))
                        .count();

        return totalTasks > 0 ? 1.0 - (double) failedTasks / totalTasks : 1.0;
    }

    private List<StageMetrics> collectStageMetrics(String jobId) {
        List<StageMetrics> result = new ArrayList<>();

        // Find all Stages belonging to this Job
        stageMetrics.values().stream()
                .filter(stage -> jobId.equals(stage.getJobId()))
                .forEach(
                        stageData -> {
                            // Collect Task-level detailed information
                            List<TaskMetricsData> taskMetricsDataList =
                                    collectTaskMetrics(stageData.getStageId());
                            stageData.setTaskDetails(taskMetricsDataList);
                            result.add(stageData);
                        });

        return result;
    }

    private List<TaskMetricsData> collectTaskMetrics(int stageId) {
        List<TaskMetricsData> result = new ArrayList<>();

        taskMetrics.values().stream()
                .filter(task -> task.getStageId() == stageId)
                .forEach(taskData -> result.add(TaskMetricsData.copyOf(taskData)));

        return result;
    }

    private List<ExecutorMetrics> collectExecutorMetrics(String jobId) {
        List<ExecutorMetrics> result = new ArrayList<>();

        // Find Executors running this Job
        Set<String> executorIds = new HashSet<>();
        taskMetrics.values().stream()
                .filter(task -> jobId.equals(task.getJobId()))
                .map(TaskMetricsData::getExecutorId)
                .forEach(executorIds::add);

        executorIds.forEach(
                executorId -> {
                    ExecutorMetrics executorData = executorMetrics.get(executorId);
                    if (executorData != null) {
                        result.add(ExecutorMetrics.copyOf(executorData));
                    }
                });

        return result;
    }

    private void enrichWithSparkApiMetrics(String jobId, JobMetrics metrics) {
        try {
            // Build Spark REST API URL
            String sparkUrl = spark.sparkContext().conf().get("spark.master", "local");
            if (sparkUrl.contains("spark://")) {
                String[] parts = sparkUrl.split(":");
                String host = parts[1].substring(2); // remove"//"
                int port = Integer.parseInt(parts[2]);

                // Use Spark REST API to get Job info
                // Note: actual implementation requires HTTP client to call REST API
                // Simplified as pseudocode here
                /*
                SparkRestClient client = new SparkRestClient(host, port);
                SparkJobInfo sparkJobInfo = client.getJobInfo(jobId);

                if (sparkJobInfo != null) {
                metrics.setExecutorRunTime(sparkJobInfo.getExecutorRunTime());
                metrics.setExecutorCpuTime(sparkJobInfo.getExecutorCpuTime());
                metrics.setResultSerializationTime(sparkJobInfo.getResultSerializationTime());
                metrics.setJvmGcTime(sparkJobInfo.getJvmGcTime());
                metrics.setResultSize(sparkJobInfo.getResultSize());
                metrics.setDiskBytesSpilled(sparkJobInfo.getDiskBytesSpilled());
                metrics.setMemoryBytesSpilled(sparkJobInfo.getMemoryBytesSpilled());
                metrics.setPeakExecutionMemory(sparkJobInfo.getPeakExecutionMemory());
                metrics.setRecordsRead(sparkJobInfo.getRecordsRead());
                metrics.setBytesRead(sparkJobInfo.getBytesRead());
                metrics.setRecordsWritten(sparkJobInfo.getRecordsWritten());
                metrics.setBytesWritten(sparkJobInfo.getBytesWritten());
                metrics.setShuffleBytesRead(sparkJobInfo.getShuffleBytesRead());
                metrics.setShuffleBytesWritten(sparkJobInfo.getShuffleBytesWritten());
                metrics.setShuffleRecordsRead(sparkJobInfo.getShuffleRecordsRead());
                metrics.setShuffleRecordsWritten(sparkJobInfo.getShuffleRecordsWritten());
                }
                */
            }
        } catch (Exception e) {
            // Log error but do not interrupt
            log.error("Error enriching metrics from Spark API: {}", e.getMessage());
        }
    }

    private void collectSystemMetrics(JobMetrics metrics) {
        Runtime runtime = Runtime.getRuntime();

        SystemMetrics systemMetrics = new SystemMetrics();
        systemMetrics.setTotalMemory(runtime.totalMemory());
        systemMetrics.setFreeMemory(runtime.freeMemory());
        systemMetrics.setMaxMemory(runtime.maxMemory());
        systemMetrics.setAvailableProcessors(runtime.availableProcessors());

        // JVM Metrics
        systemMetrics.setHeapUsed(
                ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
        systemMetrics.setHeapMax(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax());
        systemMetrics.setNonHeapUsed(
                ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage().getUsed());

        // GC Metrics
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        long gcCount = 0;
        long gcTime = 0;
        for (GarbageCollectorMXBean gcBean : gcBeans) {
            gcCount += gcBean.getCollectionCount();
            gcTime += gcBean.getCollectionTime();
        }
        systemMetrics.setGcCount(gcCount);
        systemMetrics.setGcTime(gcTime);

        // Thread Metrics
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        systemMetrics.setThreadCount(threadBean.getThreadCount());
        systemMetrics.setDaemonThreadCount(threadBean.getDaemonThreadCount());
        systemMetrics.setPeakThreadCount(threadBean.getPeakThreadCount());

        metrics.setSystemMetrics(systemMetrics);
    }

    private void calculateDerivedMetrics(JobMetrics metrics) {
        long totalTime = metrics.getDuration();
        long executorRunTime = metrics.getExecutorRunTime();

        if (totalTime > 0) {
            // Calculate scheduling delay
            long schedulingDelay = totalTime - executorRunTime;
            metrics.setSchedulingDelay(schedulingDelay);

            // Calculate CPU utilization
            double cpuUtilization = (double) metrics.getExecutorCpuTime() / (totalTime);
            metrics.setCpuUtilization(Math.min(cpuUtilization, 1.0));

            // Calculate memory utilization
            if (metrics.getSystemMetrics() != null) {
                double memoryUtilization =
                        (double) metrics.getSystemMetrics().getHeapUsed()
                                / metrics.getSystemMetrics().getHeapMax();
                metrics.setMemoryUtilization(memoryUtilization);
            }
        }

        // Calculate throughput
        long bytesProcessed = metrics.getBytesRead() + metrics.getShuffleBytesRead();
        if (totalTime > 0) {
            double throughput = (double) bytesProcessed / totalTime * 1000; // bytes/ms to bytes/s
            metrics.setThroughputBytesPerSec(throughput);
        }

        // Calculate efficiency metrics
        if (metrics.getTotalTasks() > 0) {
            double taskSuccessRate = (double) metrics.getCompletedTasks() / metrics.getTotalTasks();
            metrics.setTaskSuccessRate(taskSuccessRate);

            double avgTaskTime = (double) metrics.getExecutorRunTime() / metrics.getTotalTasks();
            metrics.setAvgTaskTime(avgTaskTime);
        }
    }

    private void notifyConsumers(JobMetrics metrics) {
        metricsConsumers.forEach(
                consumer -> {
                    try {
                        consumer.accept(metrics);
                    } catch (Exception e) {
                        log.error("Error notifying metrics consumer: {}", e.getMessage());
                    }
                });
    }

    private void registerSparkListener() {
        spark.sparkContext().addSparkListener(this);
    }

    private void initializeMetrics() {
        // Register Job-level metrics
        metricRegistry.register("jobs.total", (Gauge<Long>) totalJobsProcessed::get);
        metricRegistry.register(
                "jobs.active",
                (Gauge<Integer>)
                        () ->
                                (int)
                                        jobMetrics.values().stream()
                                                .filter(job -> job.getStatus() == JobStatus.RUNNING)
                                                .count());

        // Register Task-level metrics
        metricRegistry.register("tasks.total", (Gauge<Long>) totalTasksProcessed::get);
        metricRegistry.register(
                "tasks.active",
                (Gauge<Integer>)
                        () ->
                                (int)
                                        taskMetrics.values().stream()
                                                .filter(task -> "RUNNING".equals(task.getStatus()))
                                                .count());

        // Register data processing metrics
        metricRegistry.register("bytes.processed.total", (Gauge<Long>) totalBytesProcessed::get);
        metricRegistry.register("bytes.processed.rate", new Meter());

        // Register executor metrics
        metricRegistry.register("executors.active", (Gauge<Integer>) executorMetrics::size);

        // Register histogram for latency analysis
        metricRegistry.histogram("job.duration");
        metricRegistry.histogram("task.duration");
        metricRegistry.histogram("stage.duration");
    }

    private void startScheduledTasks() {
        scheduler.scheduleAtFixedRate(this::cleanupExpiredMetrics, 5, 5, TimeUnit.MINUTES);
        scheduler.scheduleAtFixedRate(this::collectAggregateMetrics, 30, 30, TimeUnit.SECONDS);
        if (persistEnabled) {
            scheduler.scheduleAtFixedRate(this::persistMetrics, 1, 1, TimeUnit.MINUTES);
        }
    }

    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void cleanupExpiredMetrics() {
        long cutoffTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);

        // Clean up expired Job metrics
        jobMetrics
                .entrySet()
                .removeIf(
                        entry ->
                                entry.getValue().getEndTime() < cutoffTime
                                        && entry.getValue().getStatus() != JobStatus.RUNNING);

        // Clean up expired Stage metrics
        stageMetrics
                .entrySet()
                .removeIf(entry -> entry.getValue().getCompletionTime() < cutoffTime);

        // Clean up expired Task metrics
        taskMetrics.entrySet().removeIf(entry -> entry.getValue().getCompletedTime() < cutoffTime);

        // Clean up expired Executor metrics
        executorMetrics
                .entrySet()
                .removeIf(
                        entry ->
                                System.currentTimeMillis() - entry.getValue().getLastHeartbeat()
                                        > TimeUnit.MINUTES.toMillis(5));
    }

    private void collectAggregateMetrics() {
        // Calculate aggregate metrics for all active Jobs
        long totalActiveTasks = 0;
        long totalActiveBytes = 0;
        int activeJobs = 0;

        for (JobMetrics job : jobMetrics.values()) {
            if (job.getStatus() == JobStatus.RUNNING) {
                activeJobs++;
                totalActiveTasks += job.getTotalTasks();
                // todo more aggregate calculations can be added here
            }
        }

        // Update metrics
        metricRegistry.counter("jobs.active.count").inc(activeJobs);
        metricRegistry.histogram("jobs.active.tasks").update(totalActiveTasks);

        // Log aggregate metrics to log or monitoring system
        if (activeJobs > 0) {
            log.info("Active jobs: {}, Active tasks: {}", activeJobs, totalActiveTasks);
        }
    }

    private void persistMetrics() {
        try {
            String timestamp = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date());
            java.nio.file.Path metricsDir = Paths.get(System.getProperty("user.dir"), "metrics");
            if (!Files.exists(metricsDir)) {
                Files.createDirectories(metricsDir);
            }

            java.nio.file.Path filePath =
                    metricsDir.resolve(String.format("metrics-%s.json", timestamp));

            Map<String, Object> metricsSnapshot = new HashMap<>();
            metricsSnapshot.put("timestamp", System.currentTimeMillis());
            metricsSnapshot.put("totalJobs", totalJobsProcessed.get());
            metricsSnapshot.put("totalTasks", totalTasksProcessed.get());
            metricsSnapshot.put("totalBytes", totalBytesProcessed.get());
            metricsSnapshot.put("activeJobs", jobMetrics.size());
            metricsSnapshot.put("activeExecutors", executorMetrics.size());

            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(metricsSnapshot);

            Files.write(
                    filePath, json.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);

        } catch (Exception e) {
            log.error("Failed to persist metrics: {}", e.getMessage());
        }
    }

    private static class CachedMetrics {
        JobMetrics metrics;
        long timestamp;

        CachedMetrics(JobMetrics metrics, long timestamp) {
            this.metrics = metrics;
            this.timestamp = timestamp;
        }

        boolean isExpired() {
            return System.currentTimeMillis() - timestamp > TimeUnit.MINUTES.toMillis(5);
        }
    }
}
