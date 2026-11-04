package io.github.rawwise.axiom.pipeline.benchmark;

import java.util.List;
import java.util.Map;

public record BenchmarkResult(
        String experimentName,
        boolean hasRawWise,
        int sampleCount,
        List<Map<String, Object>> samples,
        Map<String, Double> aggregateMetrics) {
    public record Delta(
            BenchmarkResult baseline,
            BenchmarkResult experiment,
            Map<String, Double> improvements) {
        public double improvement(String metric) {
            return improvements.getOrDefault(metric, 0.0);
        }
    }
}
