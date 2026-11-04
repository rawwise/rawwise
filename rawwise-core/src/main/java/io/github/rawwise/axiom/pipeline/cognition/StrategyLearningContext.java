package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.axiom.pipeline.feedback.CollectedFeedback;
import io.github.rawwise.model.record.StrategyAction;
import java.util.List;

/** strategy learningcontext. */
public record StrategyLearningContext(
        List<CollectedFeedback> feedbackHistory,
        String systemStateSummary,
        List<StrategyAction> existingStrategies,
        String tableId) {}
