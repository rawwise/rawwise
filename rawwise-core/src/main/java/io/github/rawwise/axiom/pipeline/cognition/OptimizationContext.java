package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.axiom.pipeline.feedback.CollectedFeedback;
import io.github.rawwise.model.record.StrategyAction;
import java.util.List;
import java.util.Map;

/** strategy optimizationcontext. */
public record OptimizationContext(
        String sceneDescription,
        Map<String, Double> currentMetrics,
        List<CollectedFeedback> feedbackHistory,
        List<StrategyAction> existingStrategies,
        String tableId) {}
