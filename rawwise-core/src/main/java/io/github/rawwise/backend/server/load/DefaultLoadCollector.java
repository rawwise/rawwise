package io.github.rawwise.backend.server.load;

import java.lang.management.ManagementFactory;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class DefaultLoadCollector implements LoadCollector {
    private final com.sun.management.OperatingSystemMXBean comSunOsBean;
    private final AtomicInteger queueLengthSupplier;
    private final SparkSession spark;

    public DefaultLoadCollector(AtomicInteger queueLengthSupplier) {
        this(queueLengthSupplier, null);
    }

    public DefaultLoadCollector(AtomicInteger queueLengthSupplier, SparkSession spark) {
        java.lang.management.OperatingSystemMXBean osBean =
                ManagementFactory.getOperatingSystemMXBean();
        this.comSunOsBean =
                osBean instanceof com.sun.management.OperatingSystemMXBean sunBean ? sunBean : null;
        this.queueLengthSupplier = queueLengthSupplier;
        this.spark = spark;
    }

    @Override
    public BackendLoadInfo collect() {
        return BackendLoadInfo.builder()
                .queueLength(queueLengthSupplier.get())
                .cpuUsage(getCpuUsage())
                .memoryUsage(getMemoryUsage())
                .diskUsage(getDiskUsage())
                .networkUsage(getNetworkUsage())
                .sparkActiveTasks(getSparkActiveTasks())
                .sparkPendingTasks(getSparkPendingTasks())
                .build();
    }

    private double getCpuUsage() {
        if (comSunOsBean == null) {
            return 0.0;
        }
        double load = comSunOsBean.getCpuLoad();
        return load < 0.0 ? 0.0 : load;
    }

    private double getMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        return (double) usedMemory / totalMemory;
    }

    private double getDiskUsage() {
        try {
            FileStore store = Files.getFileStore(FileSystems.getDefault().getPath("/"));
            long total = store.getTotalSpace();
            long used = total - store.getUsableSpace();
            if (total > 0) {
                return (double) used / total;
            }
        } catch (Exception e) {
            log.warn("Failed to get disk usage via FileStore", e);
        }
        return 0.0;
    }

    private double getNetworkUsage() {
        return 0.0;
    }

    private int getSparkActiveTasks() {
        if (spark == null) {
            return 0;
        }
        try {
            return spark.sparkContext().statusTracker().getActiveJobIds().length;
        } catch (Exception e) {
            log.warn("Failed to get Spark active tasks", e);
            return 0;
        }
    }

    private int getSparkPendingTasks() {
        if (spark == null) {
            return 0;
        }
        try {
            int activeJobCount = spark.sparkContext().statusTracker().getActiveJobIds().length;
            int activeStageCount = spark.sparkContext().statusTracker().getActiveStageIds().length;
            return Math.max(0, activeStageCount - activeJobCount);
        } catch (Exception e) {
            log.warn("Failed to get Spark pending tasks", e);
            return 0;
        }
    }
}
