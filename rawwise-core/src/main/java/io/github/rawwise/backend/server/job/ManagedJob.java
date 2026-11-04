package io.github.rawwise.backend.server.job;

import io.github.rawwise.backend.server.metrics.JobMetrics;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;

public class ManagedJob {
    @Getter private final String id;
    @Getter private final String sessionId;
    @Getter private final String sql;
    @Getter private final JobType jobType;
    @Getter private final Map<String, String> options;
    private final AtomicReference<JobStatus> status = new AtomicReference<>(JobStatus.PENDING);
    @Getter private long startTime;
    @Getter private long endTime;
    @Getter private String error;
    private ManagedJobResult result;
    private JobResultHandle resultHandle;
    @Getter private JobMetrics metrics;
    private volatile boolean cancelRequested;
    private volatile Future<?> executionFuture;
    private volatile long cancellingTime;

    private final long timeoutMs;
    private final long submitTime;

    public ManagedJob(
            String id, String sessionId, String sql, JobType type, Map<String, String> options) {
        this.id = id;
        this.sessionId = sessionId;
        this.sql = sql;
        this.jobType = type;
        this.options = options;
        this.submitTime = System.currentTimeMillis();
        this.timeoutMs = parseTimeout(options);
        this.metrics = new JobMetrics();
    }

    private static long parseTimeout(Map<String, String> options) {
        try {
            return Long.parseLong(options.getOrDefault("timeout", "3600000"));
        } catch (NumberFormatException e) {
            return 3600000L;
        }
    }

    public JobStatus getStatus() {
        return status.get();
    }

    public boolean transitionTo(JobStatus target) {
        JobStatus current = status.get();
        while (true) {
            if (!current.canTransitionTo(target)) {
                return false;
            }
            if (status.compareAndSet(current, target)) {
                return true;
            }
            Thread.onSpinWait();
            current = status.get();
        }
    }

    public void requestCancel() {
        this.cancelRequested = true;
    }

    public boolean isCancelRequested() {
        return cancelRequested;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public void setError(String error) {
        this.error = error;
    }

    public void setResult(ManagedJobResult result) {
        this.result = result;
    }

    public void setResultHandle(JobResultHandle resultHandle) {
        this.resultHandle = resultHandle;
    }

    public JobResultHandle getResultHandle() {
        return resultHandle;
    }

    public void setExecutionFuture(Future<?> executionFuture) {
        this.executionFuture = executionFuture;
    }

    public void setCancellingTime(long cancellingTime) {
        this.cancellingTime = cancellingTime;
    }

    public Future<?> getExecutionFuture() {
        return executionFuture;
    }

    public boolean isCancellable() {
        return getStatus() == JobStatus.PENDING || getStatus() == JobStatus.RUNNING;
    }

    public boolean isTimeout() {
        return getStatus() == JobStatus.RUNNING
                && System.currentTimeMillis() - startTime > timeoutMs;
    }

    public boolean isPendingTimeout() {
        return getStatus() == JobStatus.PENDING
                && System.currentTimeMillis() - submitTime > timeoutMs;
    }

    public boolean isCancellingTimeout() {
        return getStatus() == JobStatus.CANCELLING
                && cancellingTime > 0
                && System.currentTimeMillis() - cancellingTime > 30_000;
    }

    public ManagedJobResult toResult() {
        return new ManagedJobResult(id, sql, status.get(), startTime, endTime, error);
    }

    public void updateMetrics(JobMetrics newMetrics) {
        this.metrics = newMetrics;
    }

    public ManagedJobResult getResult() {
        return result;
    }
}
