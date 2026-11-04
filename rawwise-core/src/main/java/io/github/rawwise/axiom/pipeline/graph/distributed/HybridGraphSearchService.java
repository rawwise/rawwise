package io.github.rawwise.axiom.pipeline.graph.distributed;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HybridGraphSearchService implements AutoCloseable {

    public static final long DEFAULT_OVERALL_TIMEOUT_MS = 800L;
    public static final int DEFAULT_GRAPH_MAX_HOPS = 2;
    public static final int DEFAULT_MULTI_QUERY_EXPANSION = 4;

    private final VectorSearchCoordinator vectorCoordinator;
    private final KeywordSearchCoordinator keywordCoordinator;
    private final GraphSearchCoordinator graphCoordinator;
    private final QueryReformulationStage reformulationStage;
    private final long overallTimeoutMs;
    private final ExecutorService executor;

    public HybridGraphSearchService(
            VectorSearchCoordinator vectorCoordinator,
            KeywordSearchCoordinator keywordCoordinator,
            GraphSearchCoordinator graphCoordinator,
            QueryReformulationStage reformulationStage) {
        this(
                vectorCoordinator,
                keywordCoordinator,
                graphCoordinator,
                reformulationStage,
                DEFAULT_OVERALL_TIMEOUT_MS);
    }

    public HybridGraphSearchService(
            VectorSearchCoordinator vectorCoordinator,
            KeywordSearchCoordinator keywordCoordinator,
            GraphSearchCoordinator graphCoordinator,
            QueryReformulationStage reformulationStage,
            long overallTimeoutMs) {
        this.vectorCoordinator = Objects.requireNonNull(vectorCoordinator, "vectorCoordinator");
        this.keywordCoordinator = Objects.requireNonNull(keywordCoordinator, "keywordCoordinator");
        this.graphCoordinator = Objects.requireNonNull(graphCoordinator, "graphCoordinator");
        this.reformulationStage =
                reformulationStage == null
                        ? QueryReformulationStage.identity()
                        : reformulationStage;
        this.overallTimeoutMs =
                overallTimeoutMs <= 0 ? DEFAULT_OVERALL_TIMEOUT_MS : overallTimeoutMs;
        this.executor =
                Executors.newFixedThreadPool(
                        3,
                        r -> {
                            Thread t = new Thread(r, "hybrid-graph-search");
                            t.setDaemon(true);
                            return t;
                        });
    }

    public HybridSearchResponse hybridSearch(HybridSearchRequest req) {
        Objects.requireNonNull(req, "req");
        long start = System.currentTimeMillis();

        List<String> queries =
                req.multiQuery()
                        ? reformulationStage.expand(req.query(), DEFAULT_MULTI_QUERY_EXPANSION)
                        : List.of(req.query());
        if (queries == null || queries.isEmpty()) queries = List.of(req.query());

        int topK = Math.max(req.topK(), 1);
        int graphHops = req.graphMaxHops() == null ? DEFAULT_GRAPH_MAX_HOPS : req.graphMaxHops();
        String tableId = req.tableId();

        final List<String> finalQueries = queries;

        long[] vectorMs = {0};
        long[] keywordMs = {0};
        long[] graphMs = {0};

        CompletableFuture<List<RrfFusion.ScoredHit>> vectorF =
                CompletableFuture.supplyAsync(
                        () -> {
                            long t = System.currentTimeMillis();
                            try {
                                return vectorCoordinator.search(tableId, finalQueries, topK);
                            } finally {
                                vectorMs[0] = System.currentTimeMillis() - t;
                            }
                        },
                        executor);

        CompletableFuture<List<RrfFusion.ScoredHit>> keywordF =
                CompletableFuture.supplyAsync(
                        () -> {
                            long t = System.currentTimeMillis();
                            try {
                                return keywordCoordinator.search(tableId, finalQueries, topK);
                            } finally {
                                keywordMs[0] = System.currentTimeMillis() - t;
                            }
                        },
                        executor);

        CompletableFuture<List<RrfFusion.ScoredHit>> graphF =
                CompletableFuture.supplyAsync(
                        () -> {
                            long t = System.currentTimeMillis();
                            try {
                                return graphCoordinator.search(
                                        tableId, finalQueries, topK, graphHops);
                            } finally {
                                graphMs[0] = System.currentTimeMillis() - t;
                            }
                        },
                        executor);

        try {
            CompletableFuture.allOf(vectorF, keywordF, graphF)
                    .get(overallTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException te) {
            log.warn(
                    "HybridGraphSearchService allOf timed out after {}ms, returning partial results",
                    overallTimeoutMs);
        } catch (java.util.concurrent.CancellationException ce) {
            log.warn("HybridGraphSearchService allOf cancelled: {}", ce.getMessage());
        } catch (java.util.concurrent.ExecutionException ee) {
            log.warn("HybridGraphSearchService allOf execution error: {}", ee.getMessage());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("HybridGraphSearchService allOf interrupted");
        }

        List<RrfFusion.ScoredHit> vectorHits = safeGet(vectorF);
        List<RrfFusion.ScoredHit> keywordHits = safeGet(keywordF);
        List<RrfFusion.ScoredHit> graphHits = safeGet(graphF);

        if (vectorHits.isEmpty()
                && keywordHits.isEmpty()
                && graphHits.isEmpty()
                && vectorF.isCompletedExceptionally()
                && keywordF.isCompletedExceptionally()
                && graphF.isCompletedExceptionally()) {
            throw new RuntimeException("All hybrid search coordinators failed");
        }

        long rrfStart = System.currentTimeMillis();
        Map<String, List<RrfFusion.ScoredHit>> sources =
                RrfFusion.sourcesOf(vectorHits, keywordHits, graphHits);
        List<RrfFusion.ScoredHit> fused =
                RrfFusion.fuse(
                        sources,
                        req.weights(),
                        req.rrfK() == null ? RrfFusion.DEFAULT_RRF_K : req.rrfK());
        long rrfMs = System.currentTimeMillis() - rrfStart;

        List<HybridSearchResponse.Hit> hits = new ArrayList<>();
        int limit = Math.min(fused.size(), topK);
        for (int i = 0; i < limit; i++) {
            RrfFusion.ScoredHit h = fused.get(i);
            Map<String, Object> meta = h.metadata();
            String retrievalPath =
                    meta == null ? null : Objects.toString(meta.get("retrievalPath"), null);
            HybridSearchResponse.Provenance prov =
                    HybridSearchResponse.Provenance.builder()
                            .rawKey(h.id())
                            .chain(List.of())
                            .retrievalPath(retrievalPath)
                            .build();
            hits.add(
                    HybridSearchResponse.Hit.builder()
                            .viewId(h.id())
                            .score(h.score())
                            .summary(null)
                            .provenance(prov)
                            .build());
        }

        long totalMs = System.currentTimeMillis() - start;
        HybridSearchResponse.LatencyBreakdown latency =
                HybridSearchResponse.LatencyBreakdown.builder()
                        .vectorMs(vectorMs[0])
                        .keywordMs(keywordMs[0])
                        .graphMs(graphMs[0])
                        .rrfMs(rrfMs)
                        .totalMs(totalMs)
                        .build();

        return HybridSearchResponse.builder().hits(hits).latencyBreakdown(latency).build();
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static List<RrfFusion.ScoredHit> safeGet(
            CompletableFuture<List<RrfFusion.ScoredHit>> f) {
        try {
            if (f.isDone() && !f.isCompletedExceptionally()) {
                return f.getNow(List.of());
            }
        } catch (java.util.concurrent.CompletionException
                | java.util.concurrent.CancellationException e) {
            log.debug("safeGet completion/cancellation: {}", e.getMessage());
        } catch (RuntimeException e) {
            log.debug("safeGet unexpected runtime failure: {}", e.getMessage());
        }
        return List.of();
    }
}
