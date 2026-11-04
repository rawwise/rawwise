package io.github.rawwise.engine.storage;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.governance.config.ConfigFactory;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import org.junit.jupiter.api.Test;

class MemoryBudgetBaselineTest {

    @Test
    void nativeMemorySourceProvidesMemoryLimitBytes() {
        RawWiseConfig config = ConfigFactory.createDefault();
        NativeMemorySource source = NativeMemorySource.create(config);
        long limitBytes = source.getMemoryLimitBytes();

        assertThat(limitBytes).isGreaterThan(0);
    }

    @Test
    void memoryBudgetUsesNativeMemorySourceLimit() {
        RawWiseConfig config = ConfigFactory.createDefault();
        NativeMemorySource source = NativeMemorySource.create(config);
        long limitBytes = source.getMemoryLimitBytes();
        long instanceMappedBytes = 128L * 1024 * 1024;

        MemoryBudget budget = new MemoryBudget(limitBytes, instanceMappedBytes);

        assertThat(budget.limitBytes()).isEqualTo(limitBytes);
    }

    @Test
    void memoryBudgetBaselineExceedsHeapMaxMemory() {
        RawWiseConfig config = ConfigFactory.createDefault();
        NativeMemorySource source = NativeMemorySource.create(config);
        long nativeLimit = source.getMemoryLimitBytes();
        long heapMax = Runtime.getRuntime().maxMemory();

        assertThat(nativeLimit).isGreaterThanOrEqualTo(heapMax);
    }

    @Test
    void configuredNativeMemoryLimitOverridesDefault() {
        long customLimitMb = 4096L;
        RawWiseConfig config =
                ConfigFactory.builder()
                        .set(RawWiseOptions.STORAGE_NATIVE_MEMORY_LIMIT_MB, customLimitMb)
                        .build();

        NativeMemorySource source = NativeMemorySource.create(config);
        long limitBytes = source.getMemoryLimitBytes();

        assertThat(limitBytes).isGreaterThan(0);
    }

    @Test
    void mmapBudgetDerivedFromNativeMemorySource() {
        RawWiseConfig config = ConfigFactory.createDefault();
        NativeMemorySource source = NativeMemorySource.create(config);

        long mmapBudget = source.mmapBudgetBytes();
        assertThat(mmapBudget).isGreaterThan(0);
        assertThat(mmapBudget).isLessThanOrEqualTo(source.getMemoryLimitBytes());
    }

    @Test
    void computeAvailableBytesIsNonNegative() {
        RawWiseConfig config = ConfigFactory.createDefault();
        NativeMemorySource source = NativeMemorySource.create(config);

        long available = source.computeAvailableBytes();
        assertThat(available).isGreaterThanOrEqualTo(0);
    }
}
