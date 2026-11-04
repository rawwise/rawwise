package io.github.rawwise.engine.storage;

import io.github.rawwise.backend.ContainerMemoryAdvisor;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ContainerNativeMemorySource implements NativeMemorySource {
    private final ContainerMemoryAdvisor advisor;
    private final RawWiseConfig config;
    private final long mmapBudgetBytes;

    ContainerNativeMemorySource(RawWiseConfig config) {
        this(config, ContainerMemoryAdvisor.getInstance());
    }

    ContainerNativeMemorySource(RawWiseConfig config, ContainerMemoryAdvisor advisor) {
        this.config = config;
        this.advisor = advisor;
        this.mmapBudgetBytes = computeMmapBudget(config, advisor);
    }

    @Override
    public long getMemoryLimitBytes() {
        ContainerMemoryAdvisor.MemorySnapshot snapshot = advisor.getMemorySnapshot();
        return snapshot.limitBytes();
    }

    @Override
    public long getMemoryUsageBytes() {
        ContainerMemoryAdvisor.MemorySnapshot snapshot = advisor.getMemorySnapshot();
        return snapshot.usageBytes();
    }

    @Override
    public long mmapBudgetBytes() {
        return mmapBudgetBytes;
    }

    @Override
    public long computeAvailableBytes() {
        ContainerMemoryAdvisor.MemorySnapshot snapshot = advisor.getMemorySnapshot();
        long limit = snapshot.limitBytes();
        long usage = snapshot.usageBytes();
        if (limit <= 0 || usage < 0) return Long.MIN_VALUE;
        if (limit == Long.MAX_VALUE) return Long.MIN_VALUE;
        long safetyMargin = NativeMemorySource.computeSafetyMargin(config, limit);
        return limit - usage - safetyMargin;
    }

    private static long computeMmapBudget(RawWiseConfig config, ContainerMemoryAdvisor advisor) {
        ContainerMemoryAdvisor.MemorySnapshot snapshot = advisor.getMemorySnapshot();
        long cgroupLimit = snapshot.limitBytes();
        if (cgroupLimit <= 0) {
            log.warn("Cannot determine cgroup memory limit. Falling back to configured limit.");
            return fallbackMmapBudget(config);
        }
        if (cgroupLimit == Long.MAX_VALUE) {
            log.warn("cgroup memory limit is 'max' (unlimited). Falling back to configured limit.");
            return fallbackMmapBudget(config);
        }
        long jvmHeap = Runtime.getRuntime().maxMemory();
        long safetyMargin = NativeMemorySource.computeSafetyMargin(config, cgroupLimit);
        long budget = Math.max(0, cgroupLimit - jvmHeap - safetyMargin);
        if (budget == 0) {
            log.warn(
                    "Container mmap budget is 0 (cgroupLimit={}MB, jvmHeap={}MB, safetyMargin={}MB). "
                            + "Consider reducing JVM heap or safety margin.",
                    cgroupLimit / 1024 / 1024,
                    jvmHeap / 1024 / 1024,
                    safetyMargin / 1024 / 1024);
        }
        log.info(
                "Container mmap budget: cgroupLimit={}MB, jvmHeap={}MB, safetyMargin={}MB, mmapBudget={}MB",
                cgroupLimit / 1024 / 1024,
                jvmHeap / 1024 / 1024,
                safetyMargin / 1024 / 1024,
                budget / 1024 / 1024);
        return budget;
    }

    private static long fallbackMmapBudget(RawWiseConfig config) {
        long configured = config.get(RawWiseOptions.STORAGE_NATIVE_MEMORY_LIMIT_MB);
        if (configured > 0) {
            return configured * 1024L * 1024L;
        }
        return Runtime.getRuntime().maxMemory();
    }
}
