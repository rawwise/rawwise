package io.github.rawwise.axiom.pipeline.feedback;

public record LoopRecord(
        String viewId,
        String annotationId,
        double previousQualityScore,
        double newQualityScore,
        boolean cascaded,
        LoopStatus status,
        double assetValueSnapshot,
        long timestamp) {}
