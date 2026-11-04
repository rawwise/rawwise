package io.github.rawwise.governance.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RawWiseMetrics {

    private static volatile MeterRegistry instance;

    public static void initialize(MeterRegistry registry) {
        instance = registry;
        log.info("RawWiseMetrics initialized with {}", registry.getClass().getSimpleName());
    }

    public static MeterRegistry registry() {
        if (instance == null) {
            throw new IllegalStateException("RawWiseMetrics not initialized");
        }
        return instance;
    }

    public static boolean isInitialized() {
        return instance != null;
    }

    public static Timer timer(String name, String... tags) {
        return Timer.builder(name).tags(tags).register(registry());
    }

    public static Counter counter(String name, String... tags) {
        return Counter.builder(name).tags(tags).register(registry());
    }

    public static void gauge(String name, Number value, String... tags) {
        registry().gauge(name, io.micrometer.core.instrument.Tags.of(tags), value);
    }

    private RawWiseMetrics() {}
}
