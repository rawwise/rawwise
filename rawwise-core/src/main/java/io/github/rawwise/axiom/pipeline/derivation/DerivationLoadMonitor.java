package io.github.rawwise.axiom.pipeline.derivation;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import lombok.extern.slf4j.Slf4j;

/**
 * Backend Node local CPU/memory load monitor for the derivation pipeline.
 *
 * <p>Queries the JVM's {@link Operating System MXBean} (the {@code com.sun} variant, which exposes
 * {@link Operating System MXBean#get System Cpu Load()}) and {@link Memory MXBean} to decide
 * whether the node is too busy to run the full derivation chain (Embedding + JVector). When
 * overloaded, callers should switch to the lightweight derivation path via {@link Derivation
 * Task#run Lightweight()}.
 *
 * <p>This component lives entirely in the Backend Node process and must not depend on any Backend
 * Server components (no Backpressure Manager, no Job Category).
 */
@Slf4j
public class DerivationLoadMonitor {

    public static final double DEFAULT_CPU_THRESHOLD = 0.85;
    public static final double DEFAULT_MEMORY_THRESHOLD = 0.85;

    private final OperatingSystemMXBean osBean;
    private final MemoryMXBean memoryBean;
    private final double cpuThreshold;
    private final double memoryThreshold;

    public DerivationLoadMonitor() {
        this(DEFAULT_CPU_THRESHOLD, DEFAULT_MEMORY_THRESHOLD);
    }

    public DerivationLoadMonitor(double cpuThreshold, double memoryThreshold) {
        this.cpuThreshold = cpuThreshold;
        this.memoryThreshold = memoryThreshold;
        this.osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        this.memoryBean = ManagementFactory.getMemoryMXBean();
        log.info(
                "DerivationLoadMonitor initialized: cpuThreshold={}, memoryThreshold={}",
                cpuThreshold,
                memoryThreshold);
    }

    /**
     * Returns {@code true} when CPU <em>or</em> heap memory usage exceeds the configured threshold.
     * A negative CPU load value (returned by {@link Operating System MXBean#get System Cpu Load()}
     * when unavailable) is treated as "not overloaded".
     *
     * @return {@code true} if the node is overloaded, {@code false} otherwise
     */
    public boolean isOverloaded() {
        double cpuLoad = getCpuLoad();
        double memoryUsage = getMemoryUsage();
        boolean cpuOverloaded = cpuLoad >= 0 && cpuLoad > cpuThreshold;
        boolean memOverloaded = memoryUsage > memoryThreshold;
        return cpuOverloaded || memOverloaded;
    }

    /**
     * Captures a point-in-time snapshot of the node's CPU load, heap memory usage, and the
     * resulting overloaded flag.
     *
     * @return a {@link Derivation Load Snapshot} with current readings
     */
    public DerivationLoadSnapshot snapshot() {
        double cpuLoad = getCpuLoad();
        double memoryUsage = getMemoryUsage();
        boolean overloaded =
                (cpuLoad >= 0 && cpuLoad > cpuThreshold) || memoryUsage > memoryThreshold;
        return new DerivationLoadSnapshot(cpuLoad, memoryUsage, overloaded);
    }

    private double getCpuLoad() {
        try {
            return osBean.getSystemCpuLoad();
        } catch (Exception e) {
            log.warn("DerivationLoadMonitor.getCpuLoad failed: {}", e.getMessage(), e);
            return -1;
        }
    }

    private double getMemoryUsage() {
        try {
            MemoryUsage heap = memoryBean.getHeapMemoryUsage();
            long max = heap.getMax();
            if (max <= 0) {
                return 0.0;
            }
            return (double) heap.getUsed() / max;
        } catch (Exception e) {
            log.warn("DerivationLoadMonitor.getMemoryUsage failed: {}", e.getMessage(), e);
            return 0.0;
        }
    }

    public record DerivationLoadSnapshot(double cpuLoad, double memoryUsage, boolean overloaded) {}
}
