package io.github.rawwise.axiom.pipeline.graph.distributed;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @deprecated For testing only. Production code should use {@link Vector Search Coordinator
 *     Adapter}.
 */
@Deprecated
public class SimpleStubVectorSearchCoordinator implements VectorSearchCoordinator {

    private final Map<String, double[]> vectors;
    private final Function<String, double[]> queryEmbedder;

    public SimpleStubVectorSearchCoordinator() {
        this(Map.of(), q -> new double[0]);
    }

    public SimpleStubVectorSearchCoordinator(
            Map<String, double[]> vectors, Function<String, double[]> queryEmbedder) {
        this.vectors = vectors == null ? Map.of() : Map.copyOf(vectors);
        this.queryEmbedder = queryEmbedder == null ? q -> new double[0] : queryEmbedder;
    }

    @Override
    public List<RrfFusion.ScoredHit> search(String tableId, List<String> queries, int topK) {
        if (queries == null || queries.isEmpty() || vectors.isEmpty()) return List.of();
        List<RrfFusion.ScoredHit> hits = new ArrayList<>();
        for (Map.Entry<String, double[]> entry : vectors.entrySet()) {
            String docId = entry.getKey();
            double[] docVec = entry.getValue();
            double bestSim = 0.0;
            for (String q : queries) {
                if (q == null || q.isBlank()) continue;
                double[] qVec = queryEmbedder.apply(q);
                double sim = cosineSimilarity(qVec, docVec);
                if (sim > bestSim) bestSim = sim;
            }
            if (bestSim <= 0.0) continue;
            Map<String, Object> meta = new HashMap<>();
            meta.put("retrievalPath", "VECTOR");
            meta.put("similarity", bestSim);
            hits.add(new RrfFusion.ScoredHit(docId, bestSim, meta));
        }
        hits.sort(Comparator.comparingDouble(RrfFusion.ScoredHit::score).reversed());
        int limit = Math.min(hits.size(), Math.max(topK, 1));
        return new ArrayList<>(hits.subList(0, limit));
    }

    private static double cosineSimilarity(double[] a, double[] b) {
        if (a == null || b == null || a.length == 0 || b.length == 0 || a.length != b.length) {
            return 0.0;
        }
        double dot = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        double denom = Math.sqrt(normA) * Math.sqrt(normB);
        return denom == 0.0 ? 0.0 : dot / denom;
    }
}
