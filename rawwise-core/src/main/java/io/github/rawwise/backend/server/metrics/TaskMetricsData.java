package io.github.rawwise.backend.server.metrics;

import lombok.Data;

@Data
public class TaskMetricsData {
    private String jobId;
    private String taskId;
    private String executorId;
    private int stageId;
    private String status;
    private long duration;
    private long gcTime;
    private long resultSerializationTime;
    private long bytesRead;
    private long bytesWritten;
    private long recordsRead;
    private long recordsWritten;
    private long shuffleReadBytes;
    private long shuffleWriteBytes;
    private long memoryBytesSpilled;
    private long diskBytesSpilled;
    private long peakExecutionMemory;
    private long submitTime;
    private long completedTime;

    public static TaskMetricsData copyOf(TaskMetricsData original) {
        if (original == null) {
            throw new IllegalArgumentException("Original TaskMetrics must not be null");
        }
        TaskMetricsData copy = new TaskMetricsData();
        copy.setJobId(original.getJobId());
        copy.setTaskId(original.getTaskId());
        copy.setExecutorId(original.getExecutorId());
        copy.setStageId(original.getStageId());
        copy.setStatus(original.getStatus());
        copy.setDuration(original.getDuration());
        copy.setGcTime(original.getGcTime());
        copy.setResultSerializationTime(original.getResultSerializationTime());
        copy.setBytesRead(original.getBytesRead());
        copy.setBytesWritten(original.getBytesWritten());
        copy.setRecordsRead(original.getRecordsRead());
        copy.setRecordsWritten(original.getRecordsWritten());
        copy.setShuffleReadBytes(original.getShuffleReadBytes());
        copy.setShuffleWriteBytes(original.getShuffleWriteBytes());
        copy.setMemoryBytesSpilled(original.getMemoryBytesSpilled());
        copy.setDiskBytesSpilled(original.getDiskBytesSpilled());
        copy.setPeakExecutionMemory(original.getPeakExecutionMemory());
        copy.setSubmitTime(original.getSubmitTime());
        copy.setCompletedTime(original.getCompletedTime());
        return copy;
    }
}
