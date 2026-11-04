package io.github.rawwise.axiom.pipeline.cognition;

import java.util.Map;

/** - "". */
public record EvaluationCriteria(
        Map<String, Double> targetDimensions, double minReliableSNR, String assessmentMethod) {
    public static EvaluationCriteria defaultCriteria() {
        return new EvaluationCriteria(Map.of(), 2.0, "default");
    }
}
