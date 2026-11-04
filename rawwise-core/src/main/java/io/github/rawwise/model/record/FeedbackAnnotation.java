package io.github.rawwise.model.record;

public interface FeedbackAnnotation {
    String annotationId();

    String viewId();

    String annotatorId();

    AnnotationType type();

    Object value();

    double confidence();

    long timestamp();

    default double decayWeight() {
        return 1.0;
    }

    public enum AnnotationType {
        PREFERENCE,
        CORRECTION,
        QUALITY_SCORE,
        DECISION_ADOPTED
    }
}
