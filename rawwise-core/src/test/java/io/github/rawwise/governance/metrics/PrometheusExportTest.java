package io.github.rawwise.governance.metrics;

import static org.junit.jupiter.api.Assertions.*;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

class PrometheusExportTest {

    @Test
    void initializeSetsRegistry() {
        var registry = new SimpleMeterRegistry();
        RawWiseMetrics.initialize(registry);
        assertTrue(RawWiseMetrics.isInitialized());
        assertSame(registry, RawWiseMetrics.registry());
    }

    @Test
    void counterRegistersAndIncrements() {
        var registry = new SimpleMeterRegistry();
        RawWiseMetrics.initialize(registry);

        var counter = RawWiseMetrics.counter("test.counter", "tag1", "value1");
        counter.increment();
        counter.increment(5);
        assertEquals(6.0, counter.count());
    }

    @Test
    void timerRecordsDuration() {
        var registry = new SimpleMeterRegistry();
        RawWiseMetrics.initialize(registry);

        var timer = RawWiseMetrics.timer("test.timer");
        timer.record(java.time.Duration.ofMillis(100));
        assertEquals(1, timer.count());
        assertTrue(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS) >= 100);
    }

    @Test
    void throwsWhenNotInitialized() throws Exception {
        var field = RawWiseMetrics.class.getDeclaredField("instance");
        field.setAccessible(true);
        var original = field.get(null);
        try {
            field.set(null, null);
            assertThrows(IllegalStateException.class, RawWiseMetrics::registry);
            assertFalse(RawWiseMetrics.isInitialized());
        } finally {
            field.set(null, original);
        }
    }
}
