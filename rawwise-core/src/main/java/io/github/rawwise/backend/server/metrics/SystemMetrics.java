package io.github.rawwise.backend.server.metrics;

import lombok.Data;

@Data
public class SystemMetrics {
    private long totalMemory;
    private long freeMemory;
    private long maxMemory;
    private int availableProcessors;
    private long heapUsed;
    private long heapMax;
    private long nonHeapUsed;
    private long gcCount;
    private long gcTime;
    private int threadCount;
    private int daemonThreadCount;
    private int peakThreadCount;

    public static SystemMetrics copyOf(SystemMetrics original) {
        if (original == null) {
            throw new IllegalArgumentException("Original SystemMetrics must not be null");
        }
        SystemMetrics copy = new SystemMetrics();
        copy.setTotalMemory(original.getTotalMemory());
        copy.setFreeMemory(original.getFreeMemory());
        copy.setMaxMemory(original.getMaxMemory());
        copy.setAvailableProcessors(original.getAvailableProcessors());
        copy.setHeapUsed(original.getHeapUsed());
        copy.setHeapMax(original.getHeapMax());
        copy.setNonHeapUsed(original.getNonHeapUsed());
        copy.setGcCount(original.getGcCount());
        copy.setGcTime(original.getGcTime());
        copy.setThreadCount(original.getThreadCount());
        copy.setDaemonThreadCount(original.getDaemonThreadCount());
        copy.setPeakThreadCount(original.getPeakThreadCount());
        return copy;
    }
}
