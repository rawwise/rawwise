package io.github.rawwise.engine.storage;

import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ContainerMemoryGuard {
    private final NativeMemorySource nativeMemorySource;
    private final long warningThresholdBytes;
    private final long criticalThresholdBytes;

    ContainerMemoryGuard(NativeMemorySource nativeMemorySource, RawWiseConfig config) {
        this.nativeMemorySource = nativeMemorySource;
        long limitBytes = nativeMemorySource.getMemoryLimitBytes();
        double warningRatio = config.get(RawWiseOptions.CONTAINER_MEMORY_WARNING_THRESHOLD_RATIO);
        double criticalRatio = config.get(RawWiseOptions.CONTAINER_MEMORY_CRITICAL_THRESHOLD_RATIO);
        long minWarningBytes = config.get(RawWiseOptions.CONTAINER_MEMORY_MIN_WARNING_BYTES);
        long minCriticalBytes = config.get(RawWiseOptions.CONTAINER_MEMORY_MIN_CRITICAL_BYTES);
        if (limitBytes <= 0 || limitBytes == Long.MAX_VALUE) {
            this.warningThresholdBytes = minWarningBytes;
            this.criticalThresholdBytes = minCriticalBytes;
            log.warn(
                    "Memory limit is invalid ({}), using min thresholds: warning={}MB, critical={}MB",
                    limitBytes,
                    minWarningBytes / 1024 / 1024,
                    minCriticalBytes / 1024 / 1024);
        } else {
            this.warningThresholdBytes =
                    Math.max((long) (limitBytes * warningRatio), minWarningBytes);
            this.criticalThresholdBytes =
                    Math.max((long) (limitBytes * criticalRatio), minCriticalBytes);
        }
        log.info(
                "ContainerMemoryGuard: limit={}MB, warning={}MB, critical={}MB",
                (limitBytes > 0 && limitBytes != Long.MAX_VALUE ? limitBytes / 1024 / 1024 : "N/A"),
                warningThresholdBytes / 1024 / 1024,
                criticalThresholdBytes / 1024 / 1024);
    }

    public MemoryPressureLevel getMemoryPressureLevel() {
        long available = nativeMemorySource.computeAvailableBytes();
        if (available == Long.MIN_VALUE) return MemoryPressureLevel.UNKNOWN;
        if (available == Long.MAX_VALUE) return MemoryPressureLevel.UNKNOWN;
        if (available <= 0) return MemoryPressureLevel.CRITICAL;
        if (available < criticalThresholdBytes) return MemoryPressureLevel.CRITICAL;
        if (available < warningThresholdBytes) return MemoryPressureLevel.HIGH;
        return MemoryPressureLevel.LOW;
    }

    public boolean isNativeMemoryLow() {
        MemoryPressureLevel level = getMemoryPressureLevel();
        return level == MemoryPressureLevel.HIGH || level == MemoryPressureLevel.CRITICAL;
    }

    public enum MemoryPressureLevel {
        LOW,
        HIGH,
        CRITICAL,
        UNKNOWN
    }
}
