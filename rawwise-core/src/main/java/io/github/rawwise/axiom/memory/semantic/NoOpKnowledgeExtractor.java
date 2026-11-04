package io.github.rawwise.axiom.memory.semantic;

import io.github.rawwise.governance.context.ContextEntry;
import java.util.List;

public class NoOpKnowledgeExtractor implements KnowledgeExtractor {

    @Override
    public ExtractionResult extract(
            String agentId, String graphId, List<ContextEntry> entries, ExtractionPolicy policy) {
        return new ExtractionResult(agentId, graphId, 0, 0, 0, null, 0);
    }

    @Override
    public boolean isAvailable() {
        return false;
    }
}
