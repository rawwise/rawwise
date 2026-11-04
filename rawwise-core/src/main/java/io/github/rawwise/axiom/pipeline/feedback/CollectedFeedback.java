package io.github.rawwise.axiom.pipeline.feedback;

import java.util.Map;

/** aggregated feedback data. view Version follows Contract 6.1(annotation version binding). */
public record CollectedFeedback(
        String viewId,
        String viewVersion,
        Map<String, Double> metrics,
        double overallScore,
        String feedbackType,
        double annotationValue,
        String feedbackSource,
        long timestamp) {}
