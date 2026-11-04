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

// Note: This test verifies routing logic, not full E2E search. Uses simplified coordinators.
@DisplayName("HybridSearchActivation - hybrid routinglogictest")
class HybridSearchActivationTest {

    private AxiomServiceImpl axiomService;

    @BeforeEach
    void setUp() {
        System.clearProperty("rawwise.axiom.search.path");
        axiomService = new AxiomServiceImpl();
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

    private HybridGraphSearchService createHybridService(boolean shouldThrow) {
        VectorSearchCoordinator vecCoord =
                (tableId, queries, topK) -> {
                    if (shouldThrow) throw new RuntimeException("vector coordinator crashed");
                    return List.of(
                            new RrfFusion.ScoredHit(
                                    "view:vec:001", 0.95, Map.of("source", "vector")),
                            new RrfFusion.ScoredHit(
                                    "view:vec:002", 0.80, Map.of("source", "vector")));
                };
        KeywordSearchCoordinator kwCoord =
                (tableId, queries, topK) -> {
                    if (shouldThrow) throw new RuntimeException("keyword coordinator crashed");
                    return List.of(
                            new RrfFusion.ScoredHit(
                                    "view:kw:001", 0.90, Map.of("source", "keyword")));
                };
        GraphSearchCoordinator graphCoord =
                (tableId, queries, topK, maxHops) -> {
                    if (shouldThrow) throw new RuntimeException("graph coordinator crashed");
                    return List.of(
                            new RrfFusion.ScoredHit(
                                    "view:graph:001", 0.85, Map.of("source", "graph")));
                };
        return new HybridGraphSearchService(vecCoord, kwCoord, graphCoord, null);
    }

    private AxiomSearchResponse search(String query, String tableId) {
        List<AxiomSearchResponse> captured = new ArrayList<>(1);
        AtomicBoolean completed = new AtomicBoolean(false);
        AxiomSearchRequest request =
                AxiomSearchRequest.newBuilder()
                        .setQuery(query)
                        .setTableId(tableId)
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
    @DisplayName("S1-HappyPath - defaultconfig hybrid path")
    class HappyPath {

        @Test
        @DisplayName("default search.path=hybrid search go through hybrid path non-")
        void defaultConfigSearchGoesHybrid() {
            HybridGraphSearchService hybridService = createHybridService(false);
            axiomService.setHybridSearchService(hybridService);

            AxiomSearchResponse response = search("test query", "default.test_table");

            assertThat(response).isNotNull();
            assertThat(response.getResultsList()).isNotEmpty();
            assertThat(response.getResultsCount()).isGreaterThan(0);
        }

        @Test
        @DisplayName("explicit search.path=hybrid search go through hybrid path")
        void explicitHybridPathSearchReturnsResults() {
            System.setProperty("rawwise.axiom.search.path", "hybrid");
            HybridGraphSearchService hybridService = createHybridService(false);
            axiomService.setHybridSearchService(hybridService);

            AxiomSearchResponse response = search("explicit hybrid", "default.test_table");

            assertThat(response).isNotNull();
            assertThat(response.getResultsList()).isNotEmpty();
        }

        @Test
        @DisplayName("search.path=legacy go through hybrid path")
        void legacyPathSkipsHybrid() {
            System.setProperty("rawwise.axiom.search.path", "legacy");
            AtomicBoolean hybridCalled = new AtomicBoolean(false);
            VectorSearchCoordinator vecCoord =
                    (tableId, queries, topK) -> {
                        hybridCalled.set(true);
                        return List.of(new RrfFusion.ScoredHit("view:1", 0.9, Map.of()));
                    };
            KeywordSearchCoordinator kwCoord = (tableId, queries, topK) -> List.of();
            GraphSearchCoordinator graphCoord = (tableId, queries, topK, maxHops) -> List.of();
            HybridGraphSearchService hybridService =
                    new HybridGraphSearchService(vecCoord, kwCoord, graphCoord, null);
            axiomService.setHybridSearchService(hybridService);

            search("legacy query", "default.test_table");

            assertThat(hybridCalled.get()).isFalse();
        }
    }

    @Nested
    @DisplayName("S3-FaultRecovery - hybrid exceptionhandle")
    class FaultRecovery {

        @Test
        @DisplayName("hybrid exception toerror should(executeHybridSearch internal catch)")
        void hybridExceptionProducesErrorResponse() {
            HybridGraphSearchService hybridService = createHybridService(true);
            axiomService.setHybridSearchService(hybridService);

            List<Throwable> errors = new ArrayList<>(1);
            AtomicBoolean completed = new AtomicBoolean(false);
            AxiomSearchRequest request =
                    AxiomSearchRequest.newBuilder()
                            .setQuery("crash query")
                            .setTableId("default.test_table")
                            .setTopK(10)
                            .setSnapshotId(0)
                            .build();
            axiomService.search(
                    request,
                    new StreamObserver<>() {
                        @Override
                        public void onNext(AxiomSearchResponse value) {}

                        @Override
                        public void onError(Throwable t) {
                            errors.add(t);
                            completed.set(true);
                        }

                        @Override
                        public void onCompleted() {
                            completed.set(true);
                        }
                    });

            assertThat(completed.get()).isTrue();
            assertThat(errors).isNotEmpty();
        }

        @Test
        @DisplayName("hybridSearchService is null search go through hybrid path")
        void nullHybridServiceDoesNotThrow() {
            axiomService.setHybridSearchService(null);

            List<Throwable> errors = new ArrayList<>(1);
            AtomicBoolean completed = new AtomicBoolean(false);
            AxiomSearchRequest request =
                    AxiomSearchRequest.newBuilder()
                            .setQuery("no hybrid")
                            .setTableId("default.test_table")
                            .setTopK(10)
                            .setSnapshotId(0)
                            .build();
            axiomService.search(
                    request,
                    new StreamObserver<>() {
                        @Override
                        public void onNext(AxiomSearchResponse value) {}

                        @Override
                        public void onError(Throwable t) {
                            errors.add(t);
                            completed.set(true);
                        }

                        @Override
                        public void onCompleted() {
                            completed.set(true);
                        }
                    });

            assertThat(completed.get()).isTrue();
        }
    }
}
