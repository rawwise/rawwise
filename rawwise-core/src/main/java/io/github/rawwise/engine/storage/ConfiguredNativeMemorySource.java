package io.github.rawwise.engine.storage;

import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConfiguredNativeMemorySource implements NativeMemorySource {
    private final long configuredLimitBytes;
    private final long mmapBudgetBytes;

    ConfiguredNativeMemorySource(RawWiseConfig config) {
        long configured = config.get(RawWiseOptions.STORAGE_NATIVE_MEMORY_LIMIT_MB);
        if (configured > 0) {
            this.configuredLimitBytes = configured * 1024L * 1024L;
        } else {
            long physical = getPhysicalMemoryBytes();
            if (physical > 0) {
                this.configuredLimitBytes = physical;
            } else {
                this.configuredLimitBytes = Runtime.getRuntime().maxMemory();
                log.warn(
                        "No native memory limit configured and physical memory detection failed. "
                                + "Falling back to JVM heap size {}MB as native memory budget. "
                                + "Set STORAGE_NATIVE_MEMORY_LIMIT_MB explicitly for production.",
                        configuredLimitBytes / 1024 / 1024);
            }
        }
        this.mmapBudgetBytes =
                NativeMemorySource.computeSafetyMargin(config, this.configuredLimitBytes);
    }

    @Override
    public long getMemoryLimitBytes() {
        return configuredLimitBytes;
    }

    @Override
    public long getMemoryUsageBytes() {
        long rss = getProcessRssBytes();
        if (rss >= 0) return rss;
        return -1;
    }

    @Override
    public long mmapBudgetBytes() {
        return mmapBudgetBytes;
    }

    @Override
    public long computeAvailableBytes() {
        long rss = getProcessRssBytes();
        if (rss < 0) return Long.MIN_VALUE;
        long jvmHeap = Runtime.getRuntime().maxMemory();
        long available = configuredLimitBytes - rss - jvmHeap;
        return Math.max(0, available);
    }

    private long getProcessRssBytes() {
        long rss = getProcessRssViaReflection();
        if (rss > 0) return rss;

        rss = getProcessRssViaOsCommand();
        if (rss > 0) return rss;

        rss = getProcessRssViaProcFilesystem();
        if (rss > 0) return rss;

        return estimateRssFromJvmMemory();
    }

    private long getProcessRssViaReflection() {
        try {
            com.sun.management.OperatingSystemMXBean osBean =
                    (com.sun.management.OperatingSystemMXBean)
                            ManagementFactory.getOperatingSystemMXBean();
            Method method = osBean.getClass().getMethod("getProcessRssBytes");
            Object result = method.invoke(osBean);
            if (result instanceof Long rss && rss > 0) return rss;
        } catch (Exception e) {
            log.debug("Reflection-based RSS detection unavailable: {}", e.getMessage());
        }
        return -1;
    }

    private long getProcessRssViaOsCommand() {
        try {
            long pid = ProcessHandle.current().pid();
            ProcessBuilder pb = new ProcessBuilder("ps", "-o", "rss=", "-p", String.valueOf(pid));
            pb.redirectErrorStream(true);
            Process process = pb.start();
            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line = reader.readLine();
                process.waitFor();
                if (line != null) {
                    long rssKb = Long.parseLong(line.trim());
                    return rssKb * 1024L;
                }
            }
        } catch (Exception e) {
            log.debug("OS command RSS detection failed: {}", e.getMessage());
        }
        return -1;
    }

    private long getProcessRssViaProcFilesystem() {
        try {
            java.nio.file.Path statusPath = java.nio.file.Path.of("/proc/self/status");
            if (!java.nio.file.Files.exists(statusPath)) return -1;
            String content = java.nio.file.Files.readString(statusPath);
            for (String line : content.split("\n")) {
                if (line.startsWith("VmRSS:")) {
                    String[] parts = line.trim().split("\\s+");
                    if (parts.length >= 2) {
                        long rssKb = Long.parseLong(parts[1]);
                        return rssKb * 1024L;
                    }
                }
            }
        } catch (Exception e) {
            log.debug("/proc/self/status RSS detection failed: {}", e.getMessage());
        }
        return -1;
    }

    private long estimateRssFromJvmMemory() {
        long heapUsed = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long heapMax = Runtime.getRuntime().maxMemory();
        long nonHeapEstimate = (long) (heapMax * 0.3);
        return heapUsed + nonHeapEstimate;
    }

    private long getPhysicalMemoryBytes() {
        try {
            com.sun.management.OperatingSystemMXBean osBean =
                    (com.sun.management.OperatingSystemMXBean)
                            ManagementFactory.getOperatingSystemMXBean();
            return osBean.getTotalPhysicalMemorySize();
        } catch (Exception e) {
            return -1;
        }
    }
}
