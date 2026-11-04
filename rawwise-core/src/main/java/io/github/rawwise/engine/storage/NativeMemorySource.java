package io.github.rawwise.engine.storage;

import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import java.nio.file.Files;
import java.nio.file.Path;

public interface NativeMemorySource {
    long getMemoryLimitBytes();

    long getMemoryUsageBytes();

    long mmapBudgetBytes();

    long computeAvailableBytes();

    static long computeSafetyMargin(RawWiseConfig config, long baseLimit) {
        double safetyRatio = config.get(RawWiseOptions.CONTAINER_MEMORY_SAFETY_MARGIN_RATIO);
        long minSafetyBytes = config.get(RawWiseOptions.CONTAINER_MEMORY_MIN_SAFETY_MARGIN_BYTES);
        return Math.max((long) (baseLimit * safetyRatio), minSafetyBytes);
    }

    static NativeMemorySource create(RawWiseConfig config) {
        if (isContainerEnvironment()) {
            return new ContainerNativeMemorySource(config);
        }
        return new ConfiguredNativeMemorySource(config);
    }

    private static boolean isContainerEnvironment() {
        if (!Files.exists(Path.of("/proc"))) {
            return false;
        }
        return Files.exists(Path.of("/sys/fs/cgroup/memory/memory.limit_in_bytes"))
                || Files.exists(Path.of("/sys/fs/cgroup/memory.max"));
    }
}
