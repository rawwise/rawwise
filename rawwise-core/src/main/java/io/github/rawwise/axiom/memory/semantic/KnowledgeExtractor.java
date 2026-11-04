package io.github.rawwise.axiom.memory.semantic;

import io.github.rawwise.governance.context.ContextEntry;
import java.util.List;

public interface KnowledgeExtractor {
    ExtractionResult extract(
            String agentId, String graphId, List<ContextEntry> entries, ExtractionPolicy policy)
            throws GraphException;

    boolean isAvailable();
}
