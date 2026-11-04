package io.github.rawwise.axiom.pipeline.graph.distributed;

import io.github.rawwise.axiom.pipeline.graph.EdgeFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphSearchCoordinatorImpl implements GraphSearchCoordinator {

    private final GraphCentroidRouter router;
    private final GraphShardCoordinator coordinator;

    public GraphSearchCoordinatorImpl(
            GraphCentroidRouter router, GraphShardCoordinator coordinator) {
        this.router = Objects.requireNonNull(router, "router");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
    }

    @Override
    public List<RrfFusion.ScoredHit> search(
            String tableId, List<String> queries, int topK, int maxHops) {
        if (queries == null || queries.isEmpty()) return List.of();
        Set<String> seedEntities = new LinkedHashSet<>();
        String sampleId = tableId == null ? "" : tableId;
        for (String q : queries) {
            if (q == null || q.isBlank()) continue;
            Optional<String> entityId = router.resolveEntityId(sampleId, q);
            if (entityId.isEmpty()) {
                log.debug(
                        "GraphSearchCoordinator: name '{}' did not resolve to entityId (sampleId={}); skipping",
                        q,
                        sampleId);
                continue;
            }
            seedEntities.add(entityId.get());
        }
        if (seedEntities.isEmpty()) return List.of();

        Map<String, Double> scoredEntities =
                coordinator.traverse(seedEntities, maxHops, EdgeFilter.allowAll());
        if (scoredEntities.isEmpty()) return List.of();

        List<Map.Entry<String, Double>> sorted = new ArrayList<>(scoredEntities.entrySet());
        sorted.sort(Map.Entry.<String, Double>comparingByValue().reversed());

        List<RrfFusion.ScoredHit> hits = new ArrayList<>();
        int limit = Math.min(sorted.size(), Math.max(topK, 1));
        for (int i = 0; i < limit; i++) {
            Map.Entry<String, Double> e = sorted.get(i);
            Map<String, Object> meta = new HashMap<>();
            meta.put("retrievalPath", "GRAPH_AND_VECTOR");
            meta.put("entityId", e.getKey());
            hits.add(new RrfFusion.ScoredHit(e.getKey(), e.getValue(), meta));
        }
        return hits;
    }
}
