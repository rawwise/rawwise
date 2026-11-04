package io.github.rawwise.engine.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ContainerMemoryGuardTest {

    private RawWiseConfig config;

    @BeforeEach
    void setUp() {
        config = mock(RawWiseConfig.class);
        when(config.get(RawWiseOptions.CONTAINER_MEMORY_WARNING_THRESHOLD_RATIO)).thenReturn(0.15);
        when(config.get(RawWiseOptions.CONTAINER_MEMORY_CRITICAL_THRESHOLD_RATIO)).thenReturn(0.05);
        when(config.get(RawWiseOptions.CONTAINER_MEMORY_MIN_WARNING_BYTES)).thenReturn(536870912L);
        when(config.get(RawWiseOptions.CONTAINER_MEMORY_MIN_CRITICAL_BYTES)).thenReturn(268435456L);
    }

    private NativeMemorySource mockSource(long limitBytes, long availableBytes) {
        NativeMemorySource source = mock(NativeMemorySource.class);
        when(source.getMemoryLimitBytes()).thenReturn(limitBytes);
        when(source.computeAvailableBytes()).thenReturn(availableBytes);
        return source;
    }

    @Test
    void returnsLowWhenAvailableIsSufficient() {
        NativeMemorySource source = mockSource(8L * 1024 * 1024 * 1024, 4L * 1024 * 1024 * 1024);
        ContainerMemoryGuard guard = new ContainerMemoryGuard(source, config);
        assertThat(guard.getMemoryPressureLevel())
                .isEqualTo(ContainerMemoryGuard.MemoryPressureLevel.LOW);
        assertThat(guard.isNativeMemoryLow()).isFalse();
    }

    @Test
    void returnsHighWhenAvailableBelowWarningThreshold() {
        long limit = 8L * 1024 * 1024 * 1024;
        long available = 500L * 1024 * 1024;
        NativeMemorySource source = mockSource(limit, available);
        ContainerMemoryGuard guard = new ContainerMemoryGuard(source, config);
        assertThat(guard.getMemoryPressureLevel())
                .isEqualTo(ContainerMemoryGuard.MemoryPressureLevel.HIGH);
        assertThat(guard.isNativeMemoryLow()).isTrue();
    }

    @Test
    void returnsCriticalWhenAvailableBelowCriticalThreshold() {
        long limit = 8L * 1024 * 1024 * 1024;
        long available = 100L * 1024 * 1024;
        NativeMemorySource source = mockSource(limit, available);
        ContainerMemoryGuard guard = new ContainerMemoryGuard(source, config);
        assertThat(guard.getMemoryPressureLevel())
                .isEqualTo(ContainerMemoryGuard.MemoryPressureLevel.CRITICAL);
        assertThat(guard.isNativeMemoryLow()).isTrue();
    }

    @Test
    void returnsCriticalWhenAvailableIsZero() {
        long limit = 8L * 1024 * 1024 * 1024;
        NativeMemorySource source = mockSource(limit, 0L);
        ContainerMemoryGuard guard = new ContainerMemoryGuard(source, config);
        assertThat(guard.getMemoryPressureLevel())
                .isEqualTo(ContainerMemoryGuard.MemoryPressureLevel.CRITICAL);
    }

    @Test
    void returnsCriticalWhenAvailableIsNegative() {
        long limit = 8L * 1024 * 1024 * 1024;
        NativeMemorySource source = mockSource(limit, -100L);
        ContainerMemoryGuard guard = new ContainerMemoryGuard(source, config);
        assertThat(guard.getMemoryPressureLevel())
                .isEqualTo(ContainerMemoryGuard.MemoryPressureLevel.CRITICAL);
    }

    @Test
    void returnsUnknownWhenAvailableIsMinValue() {
        long limit = 8L * 1024 * 1024 * 1024;
        NativeMemorySource source = mockSource(limit, Long.MIN_VALUE);
        ContainerMemoryGuard guard = new ContainerMemoryGuard(source, config);
        assertThat(guard.getMemoryPressureLevel())
                .isEqualTo(ContainerMemoryGuard.MemoryPressureLevel.UNKNOWN);
        assertThat(guard.isNativeMemoryLow()).isFalse();
    }

    @Test
    void returnsUnknownWhenAvailableIsMaxValue() {
        long limit = 8L * 1024 * 1024 * 1024;
        NativeMemorySource source = mockSource(limit, Long.MAX_VALUE);
        ContainerMemoryGuard guard = new ContainerMemoryGuard(source, config);
        assertThat(guard.getMemoryPressureLevel())
                .isEqualTo(ContainerMemoryGuard.MemoryPressureLevel.UNKNOWN);
    }

    @Test
    void usesMinThresholdsWhenLimitIsInvalid() {
        NativeMemorySource source = mock(NativeMemorySource.class);
        when(source.getMemoryLimitBytes()).thenReturn(-1L);
        when(source.computeAvailableBytes()).thenReturn(600L * 1024 * 1024);

        ContainerMemoryGuard guard = new ContainerMemoryGuard(source, config);
        assertThat(guard.getMemoryPressureLevel())
                .isEqualTo(ContainerMemoryGuard.MemoryPressureLevel.LOW);
    }

    @Test
    void usesMinThresholdsWhenLimitIsMaxValue() {
        NativeMemorySource source = mock(NativeMemorySource.class);
        when(source.getMemoryLimitBytes()).thenReturn(Long.MAX_VALUE);
        when(source.computeAvailableBytes()).thenReturn(100L * 1024 * 1024);

        ContainerMemoryGuard guard = new ContainerMemoryGuard(source, config);
        assertThat(guard.getMemoryPressureLevel())
                .isEqualTo(ContainerMemoryGuard.MemoryPressureLevel.CRITICAL);
    }
}
