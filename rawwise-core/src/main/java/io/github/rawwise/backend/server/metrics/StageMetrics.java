package io.github.rawwise.backend.server.metrics;

import java.util.List;
import java.util.stream.Collectors;
import lombok.Data;

@Data
public class StageMetrics {
    private int stageId;
    private String jobId;
    private String stageName;
    private int numTasks;
    private int completedTasks;
    private int failedTasks;
    private long inputBytes;
    private long outputBytes;
    private long shuffleReadBytes;
    private long shuffleWriteBytes;
    private long duration;
    private long submitTime;
    private long completionTime;
    private List<TaskMetricsData> taskDetails;

    public static StageMetrics copyOf(StageMetrics original) {
        if (original == null) {
            throw new IllegalArgumentException("Original StageMetrics must not be null");
        }

        StageMetrics copy = new StageMetrics();
        copy.setStageId(original.getStageId());
        copy.setJobId(original.getJobId());
        copy.setStageName(original.getStageName());
        copy.setNumTasks(original.getNumTasks());
        copy.setCompletedTasks(original.getCompletedTasks());
        copy.setFailedTasks(original.getFailedTasks());
        copy.setInputBytes(original.getInputBytes());
        copy.setOutputBytes(original.getOutputBytes());
        copy.setShuffleReadBytes(original.getShuffleReadBytes());
        copy.setShuffleWriteBytes(original.getShuffleWriteBytes());
        copy.setDuration(original.getDuration());
        copy.setSubmitTime(original.getSubmitTime());
        copy.setCompletionTime(original.getCompletionTime());

        if (original.getTaskDetails() != null) {
            List<TaskMetricsData> copiedTasks =
                    original.getTaskDetails().stream()
                            .map(TaskMetricsData::copyOf)
                            .collect(Collectors.toList());
            copy.setTaskDetails(copiedTasks);
        } else {
            copy.setTaskDetails(null);
        }

        return copy;
    }
}
