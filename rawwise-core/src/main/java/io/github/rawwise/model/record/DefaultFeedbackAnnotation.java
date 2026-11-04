package io.github.rawwise.model.record;

public record DefaultFeedbackAnnotation(
        String annotationId,
        String viewId,
        String annotatorId,
        AnnotationType type,
        Object value,
        double confidence,
        long timestamp,
        double decayWeight)
        implements FeedbackAnnotation {
    public DefaultFeedbackAnnotation(
            String annotationId,
            String viewId,
            String annotatorId,
            AnnotationType type,
            Object value,
            double confidence,
            long timestamp) {
        this(annotationId, viewId, annotatorId, type, value, confidence, timestamp, 1.0);
    }
}
