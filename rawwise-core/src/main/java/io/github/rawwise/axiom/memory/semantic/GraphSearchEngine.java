package io.github.rawwise.axiom.memory.semantic;

import java.util.List;

public interface GraphSearchEngine {
    List<VectorSearchHit> vectorSearch(String graphId, String query, int topK)
            throws GraphException;

    ExtractionResult extractFromContext(String agentId, String graphId, ExtractionPolicy policy)
            throws GraphException;

    int reindexGraph(String graphId, String newModelIdentifier) throws GraphException;
}
