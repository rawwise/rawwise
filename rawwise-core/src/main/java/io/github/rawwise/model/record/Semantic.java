package io.github.rawwise.model.record;

import java.util.List;

public record Semantic(
        String semanticId, List<String> sourceViewIds, SemanticType semanticType, byte[] content) {
    public enum SemanticType {
        CONCEPT,
        RELATION,
        PATTERN,
        PREFERENCE
    }
}
