package io.github.rawwise.axiom.pipeline.feedback;

import io.github.rawwise.axiom.pipeline.derivation.Layer;
import java.util.Map;

public interface FeedbackSignal {
    String feedbackId();

    FeedbackSource source();

    FeedbackPath path();

    SignalType signalType();

    String targetEntityId();

    Layer targetLayer();

    double score();

    Map<String, Object> payload();

    long timestamp();

    enum FeedbackSource {
        SELF,
        ENV,
        HUMAN
    }

    enum FeedbackPath {
        FACT_LAYER,
        EVAL_LAYER
    }

    enum SignalType {
        QUALITY_SCORE,
        CORRECTION,
        PREFERENCE,
        REWARD
    }
}
