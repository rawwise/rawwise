package io.github.rawwise.backend.server.metrics;

import lombok.Data;

@Data
public class ExecutorMetrics {
    private String id;
    private String host;
    private int port;
    private long totalMemoryMb;
    private long usedMemoryMb;
    private int totalCores;
    private int usedCores;
    private int totalTasks;
    private int completedTasks;
    private int failedTasks;
    private long totalDurationMs;
    private long gcCount;
    private long gcTimeMs;
    private long bytesReceived;
    private long bytesSent;
    private long registerTime;
    private long lastHeartbeat;

    public static ExecutorMetrics copyOf(ExecutorMetrics original) {
        if (original == null) {
            throw new IllegalArgumentException("Original ExecutorMetrics must not be null");
        }

        ExecutorMetrics copy = new ExecutorMetrics();
        copy.setId(original.getId());
        copy.setHost(original.getHost());
        copy.setPort(original.getPort());
        copy.setTotalMemoryMb(original.getTotalMemoryMb());
        copy.setUsedMemoryMb(original.getUsedMemoryMb());
        copy.setTotalCores(original.getTotalCores());
        copy.setUsedCores(original.getUsedCores());
        copy.setTotalTasks(original.getTotalTasks());
        copy.setCompletedTasks(original.getCompletedTasks());
        copy.setFailedTasks(original.getFailedTasks());
        copy.setTotalDurationMs(original.getTotalDurationMs());
        copy.setGcCount(original.getGcCount());
        copy.setGcTimeMs(original.getGcTimeMs());
        copy.setBytesReceived(original.getBytesReceived());
        copy.setBytesSent(original.getBytesSent());
        copy.setRegisterTime(original.getRegisterTime());
        copy.setLastHeartbeat(original.getLastHeartbeat());

        return copy;
    }
}
