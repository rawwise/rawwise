package io.github.rawwise.model.record;

import java.util.List;
import java.util.Optional;

public record View(
        String viewId,
        List<String> sourceChunkIds,
        ViewType viewType,
        int version,
        byte[] content,
        Optional<float[]> embedding,
        Optional<Double> qualityScore,
        List<FeedbackAnnotation> annotations) {
    public enum ViewType {
        RAW_IDENTITY,
        QUERY_RESULT,
        EMBEDDING_INDEX,
        SUMMARY,
        CLASSIFICATION,
        RETRIEVAL_TOPK
    }
}
