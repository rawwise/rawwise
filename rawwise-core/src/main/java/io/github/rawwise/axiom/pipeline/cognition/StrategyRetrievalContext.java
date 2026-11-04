package io.github.rawwise.axiom.pipeline.cognition;

import java.util.Map;

/** strategyretrievalcontext. */
public record StrategyRetrievalContext(
        String sceneDescription, Map<String, Double> currentMetrics) {}
