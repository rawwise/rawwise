package io.github.rawwise.axiom.pipeline.graph.distributed;

import io.github.rawwise.engine.vector.DistributedSearchCoordinator;
import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.IvfRouter;
import io.github.rawwise.engine.vector.VectorSearchResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VectorSearchCoordinatorAdapter implements VectorSearchCoordinator {

    private static final long SEARCH_TIMEOUT_MS = 5000L;
    private static final int RRF_K = 60;

    private final EmbeddingProvider embeddingProvider;
    private final DistributedSearchCoordinator delegate;
    private final IvfRouter ivfRouter;

    public VectorSearchCoordinatorAdapter(
            EmbeddingProvider embeddingProvider,
            DistributedSearchCoordinator delegate,
            IvfRouter ivfRouter) {
        this.embeddingProvider = embeddingProvider;
        this.delegate = delegate;
        this.ivfRouter = ivfRouter;
    }

    @Override
    public List<RrfFusion.ScoredHit> search(String tableId, List<String> queries, int topK) {
        if (queries == null || queries.isEmpty() || embeddingProvider == null || delegate == null) {
            return List.of();
        }
        List<RrfFusion.ScoredHit> allHits = new ArrayList<>();
        for (String query : queries) {
            if (query == null || query.isBlank()) continue;
            float[] embedding;
            try {
                embedding = embeddingProvider.embed(query);
            } catch (RuntimeException e) {
                log.warn(
                        "VectorSearchCoordinatorAdapter: embed failed for query='{}': {}",
                        query,
                        e.getMessage());
                continue;
            }
            List<VectorSearchResult> results = doDistributedSearch(tableId, embedding, topK);
            int rank = 1;
            for (VectorSearchResult r : results) {
                Map<String, Object> meta =
                        Map.of("retrievalPath", "VECTOR", "entryKey", r.entryKey());
                allHits.add(new RrfFusion.ScoredHit(r.entryKey(), 1.0 / (RRF_K + rank), meta));
                rank++;
            }
        }
        return allHits;
    }

    private List<VectorSearchResult> doDistributedSearch(
            String tableId, float[] embedding, int topK) {
        try {
            if (ivfRouter != null && ivfRouter.shardCount() > 0) {
                int nprobe = Math.min(4, Math.max(1, delegate.shardCount()));
                return delegate.distributedSearch(
                        tableId, embedding, topK, SEARCH_TIMEOUT_MS, nprobe, ivfRouter);
            }
            return delegate.distributedSearch(tableId, embedding, topK);
        } catch (RuntimeException e) {
            log.warn(
                    "VectorSearchCoordinatorAdapter: distributedSearch failed: {}", e.getMessage());
            return List.of();
        }
    }
}
