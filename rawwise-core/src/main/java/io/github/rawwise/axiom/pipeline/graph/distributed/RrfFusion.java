package io.github.rawwise.axiom.pipeline.graph.distributed;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Builder;

public final class RrfFusion {

    public static final int DEFAULT_RRF_K = 60;
    public static final Map<String, Double> DEFAULT_WEIGHTS =
            Map.of(
                    "vector", 0.4,
                    "keyword", 0.3,
                    "graph", 0.3);

    private RrfFusion() {}

    @Builder
    public record ScoredHit(String id, double score, Map<String, Object> metadata) {
        public ScoredHit {
            metadata = metadata == null ? Map.of() : Collections.unmodifiableMap(metadata);
        }
    }

    public static List<ScoredHit> fuse(
            Map<String, List<ScoredHit>> sourcesByName, Map<String, Double> weights, int rrfK) {
        int k = rrfK <= 0 ? DEFAULT_RRF_K : rrfK;
        Map<String, Double> effectiveWeights =
                (weights == null || weights.isEmpty()) ? DEFAULT_WEIGHTS : weights;
        Map<String, Double> fused = new HashMap<>();
        Map<String, Map<String, Object>> metadataById = new HashMap<>();

        for (Map.Entry<String, List<ScoredHit>> source : sourcesByName.entrySet()) {
            String name = source.getKey();
            List<ScoredHit> hits = source.getValue();
            if (hits == null || hits.isEmpty()) continue;
            double w = effectiveWeights.getOrDefault(name, 0.0);
            if (w <= 0.0) continue;
            int rank = 1;
            for (ScoredHit h : hits) {
                double contribution = w / (k + rank);
                fused.merge(h.id(), contribution, Double::sum);
                metadataById.putIfAbsent(h.id(), h.metadata());
                rank++;
            }
        }

        return fused.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .map(
                        e ->
                                new ScoredHit(
                                        e.getKey(),
                                        e.getValue(),
                                        metadataById.getOrDefault(e.getKey(), Map.of())))
                .collect(java.util.stream.Collectors.toCollection(java.util.ArrayList::new));
    }

    public static List<ScoredHit> fuse(Map<String, List<ScoredHit>> sourcesByName) {
        return fuse(sourcesByName, DEFAULT_WEIGHTS, DEFAULT_RRF_K);
    }

    public static Map<String, List<ScoredHit>> sourcesOf(
            List<ScoredHit> vector, List<ScoredHit> keyword, List<ScoredHit> graph) {
        Map<String, List<ScoredHit>> sources = new LinkedHashMap<>();
        if (vector != null) sources.put("vector", vector);
        if (keyword != null) sources.put("keyword", keyword);
        if (graph != null) sources.put("graph", graph);
        return sources;
    }
}
