package io.github.rawwise.axiom.pipeline.derivation;

import io.github.rawwise.axiom.pipeline.KnowledgeDataAccessor;
import io.github.rawwise.model.record.View;
import java.util.List;
import java.util.Map;

public interface ViewGenerationStrategy {
    String name();

    View.ViewType supportedViewType();

    boolean appliesTo(List<String> chunkIds, String tableId);

    byte[] buildContent(
            List<String> chunkIds, Map<String, Object> params, KnowledgeDataAccessor accessor);

    double selfConfidence(byte[] content, List<String> chunkIds);
}
