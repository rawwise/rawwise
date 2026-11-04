package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.model.record.FeedbackAnnotation;

@FunctionalInterface
public interface CorrectionCallback {
    void onCorrection(String viewId, FeedbackAnnotation correction, int depth);
}
