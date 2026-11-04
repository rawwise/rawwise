package io.github.rawwise.axiom.pipeline.cognition;

import java.util.Map;

/** Cognitive Decision (opaque wrapper). */
public record ExecutionResult(
        String decisionId,
        boolean success,
        Map<String, Double> rawMetrics,
        String rawOutput,
        long executionTimeMs) {}
