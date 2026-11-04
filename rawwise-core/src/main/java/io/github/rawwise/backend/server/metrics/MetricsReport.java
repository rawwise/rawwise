package io.github.rawwise.backend.server.metrics;

import io.github.rawwise.backend.server.job.JobStatus;
import java.util.Map;
import lombok.Data;

@Data
public class MetricsReport {
    private long timestamp;
    private long totalJobsProcessed;
    private long totalTasksProcessed;
    private long totalBytesProcessed;
    private Map<JobStatus, Integer> jobStatistics;
    private int executorCount;
    private long averageJobDuration;
    private double throughputBytesPerSec;
    private double taskSuccessRate;
}
