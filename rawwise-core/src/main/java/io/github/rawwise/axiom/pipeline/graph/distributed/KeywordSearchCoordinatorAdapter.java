package io.github.rawwise.axiom.pipeline.graph.distributed;

import io.github.rawwise.server.ExecutorPointQueryServiceGrpc;
import io.github.rawwise.server.KeywordSearchHit;
import io.github.rawwise.server.KeywordSearchRequest;
import io.github.rawwise.server.KeywordSearchResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KeywordSearchCoordinatorAdapter implements KeywordSearchCoordinator {

    private static final int RRF_K = 60;
    private static final long STUB_DEADLINE_SECONDS = 10;
    private static final long FUTURE_TIMEOUT_SECONDS = 15;

    private final List<ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub> stubs;
    private final ExecutorService searchExecutor;

    public KeywordSearchCoordinatorAdapter(
            List<ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub> stubs) {
        this.stubs = stubs != null ? List.copyOf(stubs) : List.of();
        this.searchExecutor =
                Executors.newFixedThreadPool(
                        Math.max(2, this.stubs.size()),
                        r -> {
                            Thread t = new Thread(r, "keyword-search-coordinator");
                            t.setDaemon(true);
                            return t;
                        });
    }

    @Override
    public List<RrfFusion.ScoredHit> search(String tableId, List<String> queries, int topK) {
        if (queries == null || queries.isEmpty() || stubs.isEmpty()) {
            return List.of();
        }
        List<RrfFusion.ScoredHit> allHits = new ArrayList<>();
        for (String query : queries) {
            if (query == null || query.isBlank()) continue;
            allHits.addAll(searchSingleQuery(tableId, query, topK));
        }
        return allHits;
    }

    private List<RrfFusion.ScoredHit> searchSingleQuery(String tableId, String query, int topK) {
        KeywordSearchRequest kwReq =
                KeywordSearchRequest.newBuilder()
                        .setTableId(tableId == null ? "" : tableId)
                        .setQuery(query)
                        .setTopK(topK)
                        .build();

        List<CompletableFuture<KeywordSearchResponse>> futures = new ArrayList<>();
        for (ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub stub : stubs) {
            futures.add(
                    CompletableFuture.supplyAsync(
                            () ->
                                    stub.withDeadlineAfter(STUB_DEADLINE_SECONDS, TimeUnit.SECONDS)
                                            .keywordSearch(kwReq),
                            searchExecutor));
        }

        List<RrfFusion.ScoredHit> hits = new ArrayList<>();
        int rank = 1;
        for (CompletableFuture<KeywordSearchResponse> f : futures) {
            try {
                KeywordSearchResponse resp = f.get(FUTURE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                for (KeywordSearchHit hit : resp.getHitsList()) {
                    Map<String, Object> meta = Map.of("retrievalPath", "KEYWORD");
                    hits.add(new RrfFusion.ScoredHit(hit.getDocId(), 1.0 / (RRF_K + rank), meta));
                    rank++;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Keyword search interrupted for query: {}", query, e);
            } catch (ExecutionException | TimeoutException e) {
                log.warn("Keyword search failed for query: {}", query, e);
            }
        }
        return hits;
    }

    public void close() {
        searchExecutor.shutdown();
        try {
            if (!searchExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                searchExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            searchExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
