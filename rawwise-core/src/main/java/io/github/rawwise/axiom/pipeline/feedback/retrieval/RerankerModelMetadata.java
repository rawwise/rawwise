package io.github.rawwise.axiom.pipeline.feedback.retrieval;

import java.time.Instant;
import java.util.Map;

public record RerankerModelMetadata(
        String version,
        String modelPath,
        Instant trainedAt,
        int trainingSampleCount,
        Map<String, Double> validationMetrics,
        Map<String, Double> weights) {

    public RerankerModelMetadata {
        validationMetrics = validationMetrics == null ? Map.of() : Map.copyOf(validationMetrics);
        weights = weights == null ? Map.of() : Map.copyOf(weights);
    }

    public static RerankerModelMetadata empty(String version, String modelPath, Instant trainedAt) {
        return new RerankerModelMetadata(version, modelPath, trainedAt, 0, Map.of(), Map.of());
    }
}
