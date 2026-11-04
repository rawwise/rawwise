package io.github.rawwise.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.axiom.pipeline.graph.distributed.GraphSearchCoordinator;
import io.github.rawwise.axiom.pipeline.graph.distributed.HybridGraphSearchService;
import io.github.rawwise.axiom.pipeline.graph.distributed.KeywordSearchCoordinator;
import io.github.rawwise.axiom.pipeline.graph.distributed.RrfFusion;
import io.github.rawwise.axiom.pipeline.graph.distributed.VectorSearchCoordinator;
import io.github.rawwise.backend.server.service.AxiomServiceImpl;
import io.github.rawwise.proto.axiom.AxiomSearchRequest;
import io.github.rawwise.proto.axiom.AxiomSearchResponse;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

// Note: Tests normalization fallback when reranker model is unavailable.
@DisplayName("NormalizationFallback - no reranker min-max degradationtest")
class NormalizationFallbackTest {

    private AxiomServiceImpl axiomService;

    @BeforeEach
    void setUp() {
        System.clearProperty("rawwise.axiom.search.path");
        axiomService = new AxiomServiceImpl();
        HybridGraphSearchService hybridService = createHybridService();
        axiomService.setHybridSearchService(hybridService);
    }

    @AfterEach
    void tearDown() {
        System.clearProperty("rawwise.axiom.search.path");
        if (axiomService != null) {
            try {
                axiomService.close();
            } catch (Exception e) {
                /* tearDown best-effort */
            }
        }
    }

    private HybridGraphSearchService createHybridService() {
        VectorSearchCoordinator vecCoord =
                (tableId, queries, topK) ->
                        List.of(
                                new RrfFusion.ScoredHit(
                                        "view:r:001", 0.95, Map.of("source", "vector")),
                                new RrfFusion.ScoredHit(
                                        "view:r:002", 0.80, Map.of("source", "vector")),
                                new RrfFusion.ScoredHit(
                                        "view:r:003", 0.60, Map.of("source", "vector")));
        KeywordSearchCoordinator kwCoord =
                (tableId, queries, topK) ->
                        List.of(
                                new RrfFusion.ScoredHit(
                                        "view:r:002", 0.90, Map.of("source", "keyword")));
        GraphSearchCoordinator graphCoord =
                (tableId, queries, topK, maxHops) ->
                        List.of(
                                new RrfFusion.ScoredHit(
                                        "view:r:003", 0.85, Map.of("source", "graph")));
        return new HybridGraphSearchService(vecCoord, kwCoord, graphCoord, null);
    }

    private AxiomSearchResponse search(String query) {
        List<AxiomSearchResponse> captured = new ArrayList<>(1);
        AtomicBoolean completed = new AtomicBoolean(false);
        AxiomSearchRequest request =
                AxiomSearchRequest.newBuilder()
                        .setQuery(query)
                        .setTableId("default.test_table")
                        .setTopK(10)
                        .setSnapshotId(0)
                        .build();
        axiomService.search(
                request,
                new StreamObserver<>() {
                    @Override
                    public void onNext(AxiomSearchResponse value) {
                        captured.add(value);
                    }

                    @Override
                    public void onError(Throwable t) {
                        completed.set(true);
                    }

                    @Override
                    public void onCompleted() {
                        completed.set(true);
                    }
                });
        return captured.isEmpty() ? null : captured.get(0);
    }

    @Nested
    @DisplayName("S1-HappyPath - no reranker model min-max")
    class HappyPath {

        @Test
        @DisplayName("rerankerModel is null search (no-op passthrough)")
        void nullRerankerModelSearchReturnsResults() {
            AxiomSearchResponse response = search("data lake architecture");

            assertThat(response).isNotNull();
            assertThat(response.getResultsList()).isNotEmpty();
        }

        @Test
        @DisplayName("no reranker after min-max in [0,1]")
        void minMaxNormalizationAppliedWithoutReranker() {
            AxiomSearchResponse response = search("AI-native data lake");

            assertThat(response).isNotNull();
            assertThat(response.getResultsList()).isNotEmpty();

            for (var result : response.getResultsList()) {
                assertThat(result.getScore()).isBetween(0.0f, 1.0f);
            }
        }

        @Test
        @DisplayName("multiple times search sortconsistent(no reranker RRF)")
        void multipleSearchesProduceConsistentOrdering() {
            AxiomSearchResponse r1 = search("consistent query");
            AxiomSearchResponse r2 = search("consistent query");

            assertThat(r1).isNotNull();
            assertThat(r2).isNotNull();
            assertThat(r1.getResultsCount()).isEqualTo(r2.getResultsCount());

            for (int i = 0; i < r1.getResultsCount(); i++) {
                assertThat(r1.getResults(i).getViewId()).isEqualTo(r2.getResults(i).getViewId());
            }
        }
    }

    @Nested
    @DisplayName("S4-Boundary - reranker degrade when unavailable")
    class Boundary {

        @Test
        @DisplayName("rerankerSwitcher is null search")
        void nullRerankerSwitcherSearchSucceeds() {
            AxiomSearchResponse response = search("no switcher");

            assertThat(response).isNotNull();
            assertThat(response.getResultsList()).isNotEmpty();
        }

        @Test
        @DisplayName("topK=1 search singleitem in [0,1]")
        void topK1ReturnsSingleResult() {
            List<AxiomSearchResponse> captured = new ArrayList<>(1);
            AxiomSearchRequest request =
                    AxiomSearchRequest.newBuilder()
                            .setQuery("single result")
                            .setTableId("default.test_table")
                            .setTopK(1)
                            .setSnapshotId(0)
                            .build();
            axiomService.search(
                    request,
                    new StreamObserver<>() {
                        @Override
                        public void onNext(AxiomSearchResponse value) {
                            captured.add(value);
                        }

                        @Override
                        public void onError(Throwable t) {}

                        @Override
                        public void onCompleted() {}
                    });

            assertThat(captured).isNotEmpty();
            AxiomSearchResponse response = captured.get(0);
            assertThat(response.getResultsCount()).isLessThanOrEqualTo(1);
            if (response.getResultsCount() > 0) {
                assertThat(response.getResults(0).getScore()).isBetween(0.0f, 1.0f);
            }
        }
    }
}
