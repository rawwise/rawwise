package io.github.rawwise.axiom.pipeline.graph.distributed;

import java.util.List;

@FunctionalInterface
public interface GraphSearchCoordinator {

    List<RrfFusion.ScoredHit> search(String tableId, List<String> queries, int topK, int maxHops);
}
