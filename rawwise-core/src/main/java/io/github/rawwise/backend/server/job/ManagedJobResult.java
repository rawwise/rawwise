package io.github.rawwise.backend.server.job;

public class ManagedJobResult {
    protected final String jobId;
    protected final String sql;
    protected final JobStatus status;
    protected final long startTime;
    protected final long endTime;
    protected final String error;

    public ManagedJobResult(
            String jobId,
            String sql,
            JobStatus status,
            long startTime,
            long endTime,
            String error) {
        this.jobId = jobId;
        this.sql = sql;
        this.status = status;
        this.startTime = startTime;
        this.endTime = endTime;
        this.error = error;
    }

    public JobStatus getStatus() {
        return status;
    }

    public long getCompletionTime() {
        return endTime;
    }
}
