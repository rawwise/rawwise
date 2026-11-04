package io.github.rawwise.backend.server.job;

import com.google.common.base.Throwables;
import io.github.rawwise.backend.server.load.BackpressureManager;
import io.github.rawwise.backend.server.metrics.JobMetricsCollector;
import io.github.rawwise.backend.server.session.SessionStore;
import io.github.rawwise.frontend.ir.IRType;
import io.github.rawwise.frontend.ir.QueryIR;
import io.github.rawwise.frontend.ir.adaptor.IRAdaptor;
import io.github.rawwise.frontend.ir.adaptor.IRAdaptorRegistry;
import io.github.rawwise.server.ColumnSchema;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class SparkJobScheduler implements JobScheduler {

    private static final int MAX_COMPLETED_JOBS = 1000;
    private static final int MAX_COLLECT_ROWS = 10000;
    private static final long INLINE_THRESHOLD_BYTES = 4 * 1024 * 1024;
    private static final long MAX_RESULT_BYTES = 2L * 1024 * 1024 * 1024;
    private static final long DEFAULT_RESULT_TTL_HOURS = 24;
    private static final long DEFAULT_CLEANUP_INTERVAL_SECONDS = 30;

    private final long resultTtlMs;
    private final long cleanupIntervalMs;

    private final SparkSession spark;
    private final ExecutorService queryExecutor;
    private final ConcurrentHashMap<String, ManagedJob> activeJobs;
    private final ConcurrentHashMap<String, JobResultHandle> completedJobs;
    private final ConcurrentHashMap<String, JobStatus> completedJobStatuses;
    private final ConcurrentHashMap<String, CompletedJobSummary> completedJobSummaries;
    private final ConcurrentLinkedDeque<String> completionOrder;
    private final AtomicLong totalResultBytes;
    private final ScheduledExecutorService scheduler;
    private final JobMetricsCollector metricsCollector;
    private final AtomicLong jobIdCounter;
    private final SessionStore sessionStore;
    private final AtomicInteger pendingCount;
    private final AtomicInteger runningCount;
    private final Set<String> everSubmittedJobIds;
    private final ConcurrentHashMap<String, String> jobTenants;
    private final AtomicInteger queueLength;
    private final BackpressureManager backpressureManager;
    private final JobEventLog eventLog;
    private final ConcurrentHashMap<String, JobCategory> jobCategories = new ConcurrentHashMap<>();
    private final boolean fairSchedulerEnabled;

    public SparkJobScheduler(
            SparkSession spark,
            SessionStore sessionStore,
            AtomicInteger queueLength,
            BackpressureManager backpressureManager,
            JobEventLog eventLog) {
        this.spark = spark;
        this.sessionStore = sessionStore;
        this.queueLength = queueLength;
        this.backpressureManager = backpressureManager;
        this.eventLog = eventLog != null ? eventLog : new JobEventLog();
        this.fairSchedulerEnabled =
                Boolean.parseBoolean(
                        System.getProperty("rawwise.backend.spark.fair-scheduler.enabled", "true"));
        int maxThreads = Runtime.getRuntime().availableProcessors() * 2;
        this.queryExecutor =
                new ThreadPoolExecutor(
                        maxThreads,
                        maxThreads,
                        60L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(100),
                        new ThreadPoolExecutor.AbortPolicy());
        this.resultTtlMs =
                TimeUnit.HOURS.toMillis(
                        Long.parseLong(
                                System.getProperty(
                                        "rawwise.job.result-ttl-hours",
                                        String.valueOf(DEFAULT_RESULT_TTL_HOURS))));
        this.cleanupIntervalMs =
                TimeUnit.SECONDS.toMillis(
                        Long.parseLong(
                                System.getProperty(
                                        "rawwise.job.cleanup-interval-seconds",
                                        String.valueOf(DEFAULT_CLEANUP_INTERVAL_SECONDS))));
        this.activeJobs = new ConcurrentHashMap<>();
        this.completedJobs = new ConcurrentHashMap<>();
        this.completedJobStatuses = new ConcurrentHashMap<>();
        this.completedJobSummaries = new ConcurrentHashMap<>();
        this.completionOrder = new ConcurrentLinkedDeque<>();
        this.totalResultBytes = new AtomicLong(0);
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.metricsCollector = new JobMetricsCollector(spark);
        this.jobIdCounter = new AtomicLong(0);
        this.pendingCount = new AtomicInteger(0);
        this.runningCount = new AtomicInteger(0);
        this.everSubmittedJobIds = ConcurrentHashMap.newKeySet();
        this.jobTenants = new ConcurrentHashMap<>();
    }

    public void start() {
        scheduler.scheduleAtFixedRate(
                () -> {
                    monitorActiveJobs();
                    cleanupOldJobs();
                },
                cleanupIntervalMs,
                cleanupIntervalMs,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public String submitJob(
            String sessionId, String sql, JobType type, Map<String, String> options) {
        String jobId = generateJobId();
        JobCategory category = toJobCategory(type);
        if (backpressureManager != null) {
            if (!backpressureManager.tryAcquireBoolean(category)) {
                eventLog.emit(
                        JobEvent.of(
                                jobId,
                                JobEvent.JobEventType.ADMISSION_REJECTED,
                                Map.of("category", category.name())));
                throw new RejectedExecutionException("Admission denied for category " + category);
            }
            eventLog.emit(
                    JobEvent.of(
                            jobId,
                            JobEvent.JobEventType.ADMISSION_GRANTED,
                            Map.of(
                                    "category",
                                    category.name(),
                                    "queueLength",
                                    String.valueOf(queueLength.get()))));
        }
        ManagedJob job = new ManagedJob(jobId, sessionId, sql, type, options);
        activeJobs.put(jobId, job);
        everSubmittedJobIds.add(jobId);
        jobCategories.put(jobId, category);
        eventLog.emit(
                JobEvent.of(
                        jobId,
                        JobEvent.JobEventType.SUBMITTED,
                        Map.of("type", type.name(), "category", category.name())));
        String tenantId = options.get("tenantId");
        if (tenantId != null) {
            jobTenants.put(jobId, tenantId);
        }
        pendingCount.incrementAndGet();
        queueLength.incrementAndGet();
        log.info("Submitting job {} (type={}, options={})", jobId, type, options);
        log.debug("Submitting job {} (sql={})", jobId, sql);

        try {
            job.setExecutionFuture(
                    queryExecutor.submit(() -> executeQueryJob(job, sessionId, type)));
            log.info("Job {} submitted to executor", jobId);
        } catch (RejectedExecutionException e) {
            JobCategory cat = jobCategories.remove(jobId);
            if (cat != null && backpressureManager != null) {
                backpressureManager.release(cat);
            }
            if (job.transitionTo(JobStatus.FAILED)) {
                job.setError(
                        "Job rejected: thread pool exhausted. Consider increasing capacity or reducing load.");
                job.setEndTime(System.currentTimeMillis());
                pendingCount.decrementAndGet();
                queueLength.decrementAndGet();
                moveToCompleted(job);
            }
            log.warn("Job {} rejected due to thread pool exhaustion", jobId);
        }
        return jobId;
    }

    @Override
    public String submitIR(QueryIR ir, long timeoutMs, String tenantId) {
        String jobId = generateJobId();
        JobCategory category = toJobCategory(ir.type());
        if (backpressureManager != null) {
            if (!backpressureManager.tryAcquireBoolean(category)) {
                eventLog.emit(
                        JobEvent.of(
                                jobId,
                                JobEvent.JobEventType.ADMISSION_REJECTED,
                                Map.of("category", category.name())));
                throw new RejectedExecutionException("Admission denied for category " + category);
            }
            eventLog.emit(
                    JobEvent.of(
                            jobId,
                            JobEvent.JobEventType.ADMISSION_GRANTED,
                            Map.of(
                                    "category",
                                    category.name(),
                                    "queueLength",
                                    String.valueOf(queueLength.get()))));
        }
        String syntheticSessionId = "ir_" + jobId;
        String description =
                "IR:" + ir.type().name() + ":" + String.join(",", ir.tableIdentifiers());

        Map<String, String> options = new HashMap<>();
        options.put("timeout", String.valueOf(timeoutMs));
        options.put("collect", "true");
        if (tenantId != null) {
            options.put("tenantId", tenantId);
        }

        JobType jobType = irTypeToJobType(ir.type());

        ManagedJob job = new ManagedJob(jobId, syntheticSessionId, description, jobType, options);
        activeJobs.put(jobId, job);
        everSubmittedJobIds.add(jobId);
        jobCategories.put(jobId, category);
        eventLog.emit(
                JobEvent.of(
                        jobId,
                        JobEvent.JobEventType.SUBMITTED,
                        Map.of("type", ir.type().name(), "category", category.name())));
        if (tenantId != null) {
            jobTenants.put(jobId, tenantId);
        }
        pendingCount.incrementAndGet();
        queueLength.incrementAndGet();
        log.info(
                "Submitting IR job {} (type={}, tables={})",
                jobId,
                ir.type(),
                ir.tableIdentifiers());

        try {
            job.setExecutionFuture(queryExecutor.submit(() -> executeIRJob(job, ir)));
            log.info("IR job {} submitted to executor", jobId);
        } catch (RejectedExecutionException e) {
            JobCategory cat = jobCategories.remove(jobId);
            if (cat != null && backpressureManager != null) {
                backpressureManager.release(cat);
            }
            if (job.transitionTo(JobStatus.FAILED)) {
                job.setError(
                        "Job rejected: thread pool exhausted. Consider increasing capacity or reducing load.");
                job.setEndTime(System.currentTimeMillis());
                pendingCount.decrementAndGet();
                queueLength.decrementAndGet();
                moveToCompleted(job);
            }
            log.warn("IR job {} rejected due to thread pool exhaustion", jobId);
        }
        return jobId;
    }

    private void executeQueryJob(ManagedJob job, String sessionId, JobType type) {
        boolean wasRunning = false;
        try {
            if (!job.transitionTo(JobStatus.RUNNING)) return;
            wasRunning = true;
            eventLog.emit(JobEvent.of(job.getId(), JobEvent.JobEventType.STARTED));
            pendingCount.decrementAndGet();
            runningCount.incrementAndGet();
            queueLength.decrementAndGet();
            job.setStartTime(System.currentTimeMillis());

            spark.sparkContext().setJobGroup(job.getId(), job.getSql(), true);
            JobCategory poolCategory = jobCategories.get(job.getId());
            if (fairSchedulerEnabled && poolCategory != null) {
                spark.sparkContext().setLocalProperty("spark.scheduler.pool", poolCategory.name());
            }

            sessionStore
                    .getSession(sessionId)
                    .ifPresent(
                            session -> {
                                session.markRunning();
                                sessionStore.updateSession(session);
                            });

            if (job.isCancelRequested()) {
                if (job.transitionTo(JobStatus.CANCELLED)) {
                    job.setEndTime(System.currentTimeMillis());
                    sessionStore
                            .getSession(sessionId)
                            .ifPresent(
                                    session -> {
                                        session.markCancelled();
                                        sessionStore.updateSession(session);
                                    });
                    return;
                }
            }

            Dataset<Row> result = spark.sql(job.getSql());
            log.info("Job {} spark.sql() returned, checking collect option", job.getId());

            if (job.isCancelRequested()) {
                if (job.transitionTo(JobStatus.CANCELLED)) {
                    job.setEndTime(System.currentTimeMillis());
                    sessionStore
                            .getSession(sessionId)
                            .ifPresent(
                                    session -> {
                                        session.markCancelled();
                                        sessionStore.updateSession(session);
                                    });
                    return;
                }
            }

            if (job.getOptions().getOrDefault("collect", "false").equals("true")
                    || type == JobType.WRITE) {
                log.info("Job {} materializing results", job.getId());
                JobResultHandle handle = materializeResult(job, result);
                job.setResultHandle(handle);
                job.setResult(new ListResult(job.toResult(), List.of()));
            } else if (job.getOptions().containsKey("outputPath")) {
                String outputPath = job.getOptions().get("outputPath");
                result.write()
                        .mode(SaveMode.Append)
                        .format(job.getOptions().getOrDefault("format", "parquet"))
                        .save(outputPath);
                long now = System.currentTimeMillis();
                JobResultHandle handle =
                        ExternalResult.ofUnknownRowCount(job.getId(), outputPath, 0L, now);
                job.setResultHandle(handle);
                job.setResult(new StringResult(job.toResult(), outputPath));
            }

            if (!job.transitionTo(JobStatus.SUCCEEDED)) {
                if (job.getStatus() == JobStatus.CANCELLING) {
                    job.transitionTo(JobStatus.CANCELLED);
                    job.setEndTime(System.currentTimeMillis());
                    sessionStore
                            .getSession(sessionId)
                            .ifPresent(
                                    session -> {
                                        session.markCancelled();
                                        sessionStore.updateSession(session);
                                    });
                }
                return;
            }
            job.setEndTime(System.currentTimeMillis());
            eventLog.emit(
                    JobEvent.of(
                            job.getId(),
                            JobEvent.JobEventType.SUCCEEDED,
                            Map.of(
                                    "duration",
                                    String.valueOf(job.getEndTime() - job.getStartTime()))));
            log.info("Job {} completed successfully", job.getId());

            sessionStore
                    .getSession(sessionId)
                    .ifPresent(
                            session -> {
                                session.markCompleted(
                                        job.getResultHandle() != null
                                                ? job.getResultHandle()
                                                : job.getResult());
                                sessionStore.updateSession(session);
                            });

        } catch (Throwable t) {
            log.error("Job {} failed with throwable: {}", job.getId(), t.getClass().getName(), t);
            if (job.getStatus() == JobStatus.CANCELLING) {
                job.transitionTo(JobStatus.CANCELLED);
                job.setError(Throwables.getStackTraceAsString(t));
                job.setEndTime(System.currentTimeMillis());
                sessionStore
                        .getSession(sessionId)
                        .ifPresent(
                                session -> {
                                    session.markCancelled();
                                    sessionStore.updateSession(session);
                                });
            } else if (job.getStatus().canTransitionTo(JobStatus.FAILED)) {
                job.transitionTo(JobStatus.FAILED);
                job.setError(Throwables.getStackTraceAsString(t));
                job.setEndTime(System.currentTimeMillis());
                eventLog.emit(
                        JobEvent.of(
                                job.getId(),
                                JobEvent.JobEventType.FAILED,
                                Map.of(
                                        "error",
                                        t.getMessage() != null
                                                ? t.getMessage()
                                                : t.getClass().getName())));
                sessionStore
                        .getSession(sessionId)
                        .ifPresent(
                                session -> {
                                    session.markFailed(t.getMessage());
                                    sessionStore.updateSession(session);
                                });
            }
        } finally {
            if (fairSchedulerEnabled) {
                spark.sparkContext().setLocalProperty("spark.scheduler.pool", null);
            }
            if (wasRunning) {
                runningCount.decrementAndGet();
            }
            JobCategory cat = jobCategories.remove(job.getId());
            if (cat != null && backpressureManager != null) {
                backpressureManager.release(cat);
            }
            moveToCompleted(job);
        }
    }

    private void executeIRJob(ManagedJob job, QueryIR ir) {
        boolean wasRunning = false;
        try {
            if (!job.transitionTo(JobStatus.RUNNING)) return;
            wasRunning = true;
            eventLog.emit(JobEvent.of(job.getId(), JobEvent.JobEventType.STARTED));
            pendingCount.decrementAndGet();
            runningCount.incrementAndGet();
            queueLength.decrementAndGet();
            job.setStartTime(System.currentTimeMillis());

            spark.sparkContext().setJobGroup(job.getId(), job.getSql(), true);
            JobCategory poolCategory = jobCategories.get(job.getId());
            if (fairSchedulerEnabled && poolCategory != null) {
                spark.sparkContext().setLocalProperty("spark.scheduler.pool", poolCategory.name());
            }
            log.info("IR job {} starting execution", job.getId());

            if (job.isCancelRequested()) {
                if (job.transitionTo(JobStatus.CANCELLED)) {
                    job.setEndTime(System.currentTimeMillis());
                    return;
                }
            }

            IRAdaptor<LogicalPlan> adaptor = IRAdaptorRegistry.create("spark");
            LogicalPlan plan = adaptor.adapt(ir);
            log.info("IR job {} LogicalPlan built, executing", job.getId());

            Dataset<Row> df = executeLogicalPlan(spark, plan);

            if (job.isCancelRequested()) {
                if (job.transitionTo(JobStatus.CANCELLED)) {
                    job.setEndTime(System.currentTimeMillis());
                    return;
                }
            }

            JobResultHandle handle = materializeResult(job, df);
            job.setResultHandle(handle);
            job.setResult(new ListResult(job.toResult(), List.of()));

            if (!job.transitionTo(JobStatus.SUCCEEDED)) {
                if (job.getStatus() == JobStatus.CANCELLING) {
                    job.transitionTo(JobStatus.CANCELLED);
                    job.setEndTime(System.currentTimeMillis());
                }
                return;
            }
            job.setEndTime(System.currentTimeMillis());
            eventLog.emit(
                    JobEvent.of(
                            job.getId(),
                            JobEvent.JobEventType.SUCCEEDED,
                            Map.of(
                                    "duration",
                                    String.valueOf(job.getEndTime() - job.getStartTime()))));
            log.info("IR job {} completed successfully", job.getId());

        } catch (Throwable t) {
            log.error(
                    "IR job {} failed with throwable: {}", job.getId(), t.getClass().getName(), t);
            if (job.getStatus() == JobStatus.CANCELLING) {
                job.transitionTo(JobStatus.CANCELLED);
                job.setError(Throwables.getStackTraceAsString(t));
                job.setEndTime(System.currentTimeMillis());
            } else if (job.getStatus().canTransitionTo(JobStatus.FAILED)) {
                job.transitionTo(JobStatus.FAILED);
                job.setError(Throwables.getStackTraceAsString(t));
                job.setEndTime(System.currentTimeMillis());
                eventLog.emit(
                        JobEvent.of(
                                job.getId(),
                                JobEvent.JobEventType.FAILED,
                                Map.of(
                                        "error",
                                        t.getMessage() != null
                                                ? t.getMessage()
                                                : t.getClass().getName())));
            }
        } finally {
            if (fairSchedulerEnabled) {
                spark.sparkContext().setLocalProperty("spark.scheduler.pool", null);
            }
            if (wasRunning) {
                runningCount.decrementAndGet();
            }
            JobCategory cat = jobCategories.remove(job.getId());
            if (cat != null && backpressureManager != null) {
                backpressureManager.release(cat);
            }
            moveToCompleted(job);
        }
    }

    private JobResultHandle materializeResult(ManagedJob job, Dataset<Row> result) {
        long now = System.currentTimeMillis();
        log.info("Job {} query plan: {}", job.getId(), result.queryExecution().executedPlan());

        List<ColumnSchema> columns = extractColumns(result.schema());

        Dataset<Row> limitedResult = result.limit(MAX_COLLECT_ROWS + 1);
        List<Row> rows = limitedResult.collectAsList();
        boolean truncated = rows.size() > MAX_COLLECT_ROWS;
        if (truncated) {
            rows = rows.subList(0, MAX_COLLECT_ROWS);
            log.warn("Job {} result truncated to {} rows", job.getId(), MAX_COLLECT_ROWS);
        }

        byte[] json = serializeRows(rows);

        if (json.length < INLINE_THRESHOLD_BYTES) {
            log.info(
                    "Job {} collected {} rows (inline, {} bytes)",
                    job.getId(),
                    rows.size(),
                    json.length);
            return new InlineResult(job.getId(), json, rows.size(), now, columns);
        } else {
            String warehousePath =
                    io.github.rawwise.governance.config.WarehousePathResolver.resolve();
            String uri = warehousePath + "/results/" + job.getId() + "/data.parquet";
            result.write().mode(SaveMode.ErrorIfExists).parquet(uri);
            log.info(
                    "Job {} wrote external result to {} ({} rows, {} bytes)",
                    job.getId(),
                    uri,
                    rows.size(),
                    json.length);
            return ExternalResult.ofUnknownRowCount(job.getId(), uri, json.length, now);
        }
    }

    private static List<ColumnSchema> extractColumns(StructType schema) {
        if (schema == null) return List.of();
        List<ColumnSchema> columns = new ArrayList<>();
        for (StructField field : schema.fields()) {
            columns.add(
                    ColumnSchema.newBuilder()
                            .setName(field.name())
                            .setType(field.dataType().typeName())
                            .setNullable(field.nullable())
                            .build());
        }
        return columns;
    }

    private static byte[] serializeRows(List<Row> rows) {
        if (rows.isEmpty()) {
            return "[]".getBytes(StandardCharsets.UTF_8);
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(rows.get(i).json());
        }
        sb.append("]");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static JobType irTypeToJobType(IRType type) {
        return switch (type) {
            case INSERT, BATCH_INSERT, UPDATE_RANGE, DELETE_RANGE -> JobType.WRITE;
            case CREATE_TABLE, DROP_TABLE, ALTER_TABLE, TRUNCATE, REFRESH -> JobType.DDL;
            default -> JobType.READ;
        };
    }

    private JobCategory toJobCategory(JobType jobType) {
        return switch (jobType) {
            case WRITE -> JobCategory.BATCH_INGESTION;
            case READ, DDL -> JobCategory.INTERACTIVE_QUERY;
        };
    }

    private JobCategory toJobCategory(IRType irType) {
        return switch (irType) {
            case SCAN -> JobCategory.TRAINING_SCAN;
            case INSERT, BATCH_INSERT, UPDATE_RANGE, DELETE_RANGE -> JobCategory.BATCH_INGESTION;
            case CREATE_TABLE, DROP_TABLE, ALTER_TABLE, TRUNCATE, REFRESH ->
                    JobCategory.INTERACTIVE_QUERY;
            default -> JobCategory.INTERACTIVE_QUERY;
        };
    }

    @Override
    public Optional<ManagedJob> getJob(String jobId) {
        return Optional.ofNullable(activeJobs.get(jobId));
    }

    @Override
    public JobStatus getJobStatus(String jobId) {
        ManagedJob job = activeJobs.get(jobId);
        if (job != null) {
            return job.getStatus();
        }
        JobStatus status = completedJobStatuses.get(jobId);
        if (status != null) {
            return status;
        }
        if (everSubmittedJobIds.contains(jobId)) {
            return JobStatus.RUNNING;
        }
        return null;
    }

    @Override
    public boolean cancelJob(String jobId) {
        ManagedJob job = activeJobs.get(jobId);
        if (job == null) return false;

        JobStatus currentStatus = job.getStatus();

        if (currentStatus == JobStatus.PENDING && job.transitionTo(JobStatus.CANCELLED)) {
            job.setEndTime(System.currentTimeMillis());
            pendingCount.decrementAndGet();
            queueLength.decrementAndGet();
            JobCategory cat = jobCategories.remove(jobId);
            if (cat != null && backpressureManager != null) {
                backpressureManager.release(cat);
            }
            eventLog.emit(JobEvent.of(jobId, JobEvent.JobEventType.CANCELLED));
            moveToCompleted(job);
            return true;
        }

        if (currentStatus == JobStatus.CANCELLING && job.transitionTo(JobStatus.CANCELLED)) {
            job.setEndTime(System.currentTimeMillis());
            moveToCompleted(job);
            return true;
        }

        if (job.transitionTo(JobStatus.CANCELLING)) {
            job.requestCancel();
            job.setCancellingTime(System.currentTimeMillis());
            Future<?> future = job.getExecutionFuture();
            if (future != null) future.cancel(true);
            spark.sparkContext().cancelJobGroup(jobId);
            return true;
        }
        return false;
    }

    @Override
    public JobResultHandle getJobResult(String jobId) {
        return completedJobs.get(jobId);
    }

    public Set<String> getEverSubmittedJobIds() {
        return everSubmittedJobIds;
    }

    @Override
    public String getJobTenantId(String jobId) {
        return jobTenants.get(jobId);
    }

    @Override
    public List<ManagedJob> getActiveJobs() {
        return new ArrayList<>(activeJobs.values());
    }

    @Override
    public int getQueueLength() {
        return queueLength.get();
    }

    public int getEffectiveQueueLength() {
        return pendingCount.get() + runningCount.get();
    }

    @Override
    public JobEventLog getEventLog() {
        return eventLog;
    }

    @Override
    public void shutdown() {
        scheduler.shutdown();
        queryExecutor.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            if (!queryExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                queryExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            queryExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void monitorActiveJobs() {
        activeJobs
                .values()
                .forEach(
                        job -> {
                            JobStatus status = job.getStatus();
                            if (status == JobStatus.RUNNING) {
                                job.updateMetrics(metricsCollector.collect(job.getId()));
                                if (job.isTimeout()) {
                                    if (job.transitionTo(JobStatus.TIMED_OUT)) {
                                        eventLog.emit(
                                                JobEvent.of(
                                                        job.getId(),
                                                        JobEvent.JobEventType.TIMED_OUT));
                                        spark.sparkContext().cancelJobGroup(job.getId());
                                        sessionStore
                                                .getSession(job.getSessionId())
                                                .ifPresent(
                                                        session -> {
                                                            session.markTimeout();
                                                            sessionStore.updateSession(session);
                                                        });
                                        job.setEndTime(System.currentTimeMillis());
                                        moveToCompleted(job);
                                    }
                                }
                            } else if (status == JobStatus.PENDING) {
                                if (job.isPendingTimeout()) {
                                    if (job.transitionTo(JobStatus.TIMED_OUT)) {
                                        eventLog.emit(
                                                JobEvent.of(
                                                        job.getId(),
                                                        JobEvent.JobEventType.TIMED_OUT));
                                        pendingCount.decrementAndGet();
                                        queueLength.decrementAndGet();
                                        sessionStore
                                                .getSession(job.getSessionId())
                                                .ifPresent(
                                                        session -> {
                                                            session.markTimeout();
                                                            sessionStore.updateSession(session);
                                                        });
                                        job.setEndTime(System.currentTimeMillis());
                                        moveToCompleted(job);
                                    }
                                }
                            } else if (status == JobStatus.CANCELLING) {
                                if (job.isCancellingTimeout()) {
                                    if (job.transitionTo(JobStatus.TIMED_OUT)) {
                                        eventLog.emit(
                                                JobEvent.of(
                                                        job.getId(),
                                                        JobEvent.JobEventType.TIMED_OUT));
                                        spark.sparkContext().cancelJobGroup(job.getId());
                                        sessionStore
                                                .getSession(job.getSessionId())
                                                .ifPresent(
                                                        session -> {
                                                            session.markTimeout();
                                                            sessionStore.updateSession(session);
                                                        });
                                        job.setEndTime(System.currentTimeMillis());
                                        moveToCompleted(job);
                                    }
                                }
                            }
                        });
    }

    private void cleanupOldJobs() {
        long cutoffTime = System.currentTimeMillis() - resultTtlMs;
        List<String> expiredKeys = new ArrayList<>();
        for (Map.Entry<String, JobResultHandle> entry : completedJobs.entrySet()) {
            if (entry.getValue().isExpired(cutoffTime)) {
                expiredKeys.add(entry.getKey());
            }
        }
        for (String key : expiredKeys) {
            JobResultHandle removed = completedJobs.remove(key);
            if (removed != null) {
                completionOrder.remove(key);
                completedJobStatuses.remove(key);
                completedJobSummaries.remove(key);
                jobTenants.remove(key);
                totalResultBytes.addAndGet(-removed.estimatedBytes());
            }
        }
        while (totalResultBytes.get() > MAX_RESULT_BYTES && !completionOrder.isEmpty()) {
            String eldest = completionOrder.pollFirst();
            if (eldest != null) {
                JobResultHandle removed = completedJobs.remove(eldest);
                completedJobStatuses.remove(eldest);
                completedJobSummaries.remove(eldest);
                jobTenants.remove(eldest);
                if (removed != null) {
                    totalResultBytes.addAndGet(-removed.estimatedBytes());
                }
            } else {
                break;
            }
        }
    }

    private void moveToCompleted(ManagedJob job) {
        if (activeJobs.remove(job.getId()) == null) return;

        completedJobStatuses.put(job.getId(), job.getStatus());
        completedJobSummaries.put(
                job.getId(),
                new CompletedJobSummary(
                        job.getId(),
                        job.getSql(),
                        job.getStartTime(),
                        job.getEndTime(),
                        job.getStatus(),
                        job.getJobType().name()));

        JobResultHandle handle = job.getResultHandle();
        if (handle != null) {
            completedJobs.put(job.getId(), handle);
            completionOrder.addLast(job.getId());
            totalResultBytes.addAndGet(handle.estimatedBytes());
        } else {
            completionOrder.addLast(job.getId());
        }

        while (completedJobs.size() > MAX_COMPLETED_JOBS
                || totalResultBytes.get() > MAX_RESULT_BYTES) {
            String eldest = completionOrder.pollFirst();
            if (eldest != null) {
                JobResultHandle removed = completedJobs.remove(eldest);
                completedJobStatuses.remove(eldest);
                jobTenants.remove(eldest);
                if (removed != null) {
                    totalResultBytes.addAndGet(-removed.estimatedBytes());
                }
            } else {
                break;
            }
        }
    }

    private String generateJobId() {
        return "job_" + System.currentTimeMillis() + "_" + jobIdCounter.incrementAndGet();
    }

    @SuppressWarnings("unchecked")
    private static Dataset<Row> executeLogicalPlan(SparkSession spark, LogicalPlan plan) {
        try {
            Class<?> classicDataset =
                    Class.forName(
                            "org.apache.spark.sql.classic.Dataset",
                            true,
                            Dataset.class.getClassLoader());
            Class<?> classicSparkSession =
                    Class.forName(
                            "org.apache.spark.sql.classic.SparkSession",
                            true,
                            Dataset.class.getClassLoader());
            java.lang.reflect.Method ofRows =
                    classicDataset.getMethod("ofRows", classicSparkSession, LogicalPlan.class);
            return (Dataset<Row>) ofRows.invoke(null, spark, plan);
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute LogicalPlan via Dataset.ofRows", e);
        }
    }

    public List<CompletedJobSummary> getCompletedJobSummaries() {
        return new ArrayList<>(completedJobSummaries.values());
    }

    public record CompletedJobSummary(
            String jobId,
            String sql,
            long startTime,
            long endTime,
            JobStatus status,
            String jobType) {
        public long durationMs() {
            return endTime - startTime;
        }
    }
}
