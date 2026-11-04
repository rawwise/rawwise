package io.github.rawwise.axiom.pipeline.feedback;

import io.github.rawwise.axiom.pipeline.derivation.Layer;
import java.util.Map;

public record DefaultFeedbackSignal(
        String feedbackId,
        FeedbackSource source,
        FeedbackPath path,
        SignalType signalType,
        String targetEntityId,
        Layer targetLayer,
        double score,
        Map<String, Object> payload,
        long timestamp)
        implements FeedbackSignal {}
