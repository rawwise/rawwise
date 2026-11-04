package io.github.rawwise.engine.storage;

import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MemoryBudget {
    private final AtomicLong totalUsedMemory = new AtomicLong(0);
    private final long mmapBudgetBytes;
    private final long instanceMappedBytes;
    private final double usageThreshold;

    public MemoryBudget(long mmapBudgetBytes, long instanceMappedBytes, double usageThreshold) {
        if (mmapBudgetBytes < 0) {
            throw new IllegalArgumentException(
                    "mmapBudgetBytes must not be negative, got: " + mmapBudgetBytes);
        }
        if (instanceMappedBytes <= 0) {
            throw new IllegalArgumentException(
                    "instanceMappedBytes must be positive, got: " + instanceMappedBytes);
        }
        if (usageThreshold <= 0.0 || usageThreshold > 1.0) {
            throw new IllegalArgumentException(
                    "usageThreshold must be in (0.0, 1.0], got: " + usageThreshold);
        }
        this.mmapBudgetBytes = mmapBudgetBytes;
        this.instanceMappedBytes = instanceMappedBytes;
        this.usageThreshold = usageThreshold;
    }

    public MemoryBudget(long mmapBudgetBytes, long instanceMappedBytes) {
        this(mmapBudgetBytes, instanceMappedBytes, 0.9);
    }

    public boolean canAllocate(long bytes) {
        if (mmapBudgetBytes == 0) return false;
        return totalUsedMemory.get() + bytes < mmapBudgetBytes * usageThreshold;
    }

    public boolean canAllocateOneInstance() {
        return canAllocate(instanceMappedBytes);
    }

    public boolean tryAllocate(long bytes) {
        while (true) {
            long current = totalUsedMemory.get();
            if (current + bytes >= mmapBudgetBytes * usageThreshold) return false;
            if (totalUsedMemory.compareAndSet(current, current + bytes)) return true;
        }
    }

    void allocate(long bytes) {
        totalUsedMemory.addAndGet(bytes);
    }

    public void release(long bytes) {
        long newValue = totalUsedMemory.addAndGet(-bytes);
        if (newValue < 0) {
            log.warn(
                    "MemoryBudget underflow: totalUsedMemory={} after releasing {} bytes. Resetting to 0.",
                    newValue + bytes,
                    bytes);
            totalUsedMemory.set(0);
        }
    }

    public long usedBytes() {
        return totalUsedMemory.get();
    }

    public long limitBytes() {
        return mmapBudgetBytes;
    }

    public long instanceMappedBytes() {
        return instanceMappedBytes;
    }
}
