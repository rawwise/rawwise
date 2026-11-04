package io.github.rawwise.axiom.pipeline.benchmark;

import java.util.Map;

public record BenchmarkConfig(
        String experimentName,
        int warmupRounds,
        int measureRounds,
        Map<String, Object> parameters) {
    public BenchmarkConfig {
        if (experimentName == null || experimentName.isBlank()) {
            throw new IllegalArgumentException("experimentName is required");
        }
        warmupRounds = Math.max(0, warmupRounds);
        measureRounds = Math.max(1, measureRounds);
        parameters = parameters == null ? Map.of() : Map.copyOf(parameters);
    }
}
