package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.axiom.pipeline.KnowledgeDataAccessor;
import io.github.rawwise.model.record.Semantic;
import java.util.List;
import java.util.Map;

public interface SemanticGenerationStrategy {
    String name();

    Semantic.SemanticType supportedType();

    boolean appliesTo(List<String> viewIds, String tableId);

    byte[] buildContent(
            List<String> viewIds, Map<String, Object> params, KnowledgeDataAccessor accessor);

    double selfConfidence(byte[] content, List<String> viewIds);
}
