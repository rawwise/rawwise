package io.github.rawwise.axiom.pipeline.feedback.retrieval;

import java.util.List;

public record AnnotateContext(String originalQuery, List<String> topKHits) {

    public static AnnotateContext empty() {
        return new AnnotateContext("", List.of());
    }
}
