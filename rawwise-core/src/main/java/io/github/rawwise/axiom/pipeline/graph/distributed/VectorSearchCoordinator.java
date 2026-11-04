package io.github.rawwise.axiom.pipeline.graph.distributed;

import java.util.List;

@FunctionalInterface
public interface VectorSearchCoordinator {

    List<RrfFusion.ScoredHit> search(String tableId, List<String> queries, int topK);
}
