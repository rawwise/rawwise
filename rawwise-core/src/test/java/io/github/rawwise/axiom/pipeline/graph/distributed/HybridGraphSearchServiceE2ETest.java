package io.github.rawwise.axiom.pipeline.graph.distributed;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@Slf4j
@DisplayName("HybridGraphSearchService E2E - vector/keyword/graph fusion pipeline")
class HybridGraphSearchServiceE2ETest {

    private HybridGraphSearchService service;

    @BeforeEach
    void setUp() {
        VectorSearchCoordinator vector = new SimpleStubVectorSearchCoordinator();
        KeywordSearchCoordinator keyword = (tableId, queries, topK) -> List.of();
        GraphSearchCoordinator graph =
                (tableId, queries, topK, maxHops) ->
                        List.of(
                                new RrfFusion.ScoredHit(
                                        "graph-hit-1", 0.9, Map.of("retrievalPath", "graph-only")));
        QueryReformulationStage reform = (q, n) -> List.of(q);
        service = new HybridGraphSearchService(vector, keyword, graph, reform, 1000L);
    }

    @AfterEach
    void tearDown() {
        if (service != null) service.close();
    }

    @Test
    @DisplayName(
            "hybridSearch with only graph hit returns the graph-hit with provenance and latencyBreakdown")
    void hybridSearchReturnsGraphHit() {
        HybridSearchRequest req =
                HybridSearchRequest.builder()
                        .tableId("test_table")
                        .query("Caroline support group")
                        .topK(10)
                        .multiQuery(false)
                        .build();

        HybridSearchResponse resp = service.hybridSearch(req);

        assertThat(resp).isNotNull();
        assertThat(resp.latencyBreakdown()).isNotNull();
        assertThat(resp.latencyBreakdown().totalMs()).isGreaterThanOrEqualTo(0L);
        assertThat(resp.hits()).hasSize(1);
        HybridSearchResponse.Hit hit = resp.hits().get(0);
        assertThat(hit.viewId()).isEqualTo("graph-hit-1");
        assertThat(hit.provenance().retrievalPath()).isEqualTo("graph-only");
        assertThat(hit.score()).isGreaterThan(0.0);
    }

    @Test
    @DisplayName("hybridSearch with multiQuery=true and identity reformulation works the same")
    void multiQueryWithIdentityReformulation() {
        HybridSearchRequest req =
                HybridSearchRequest.builder()
                        .tableId("test_table")
                        .query("test query")
                        .topK(5)
                        .multiQuery(true)
                        .build();

        HybridSearchResponse resp = service.hybridSearch(req);
        assertThat(resp).isNotNull();
        assertThat(resp.hits()).hasSize(1);
    }

    @Test
    @DisplayName("all coordinators returning empty -> empty hits, latencyBreakdown still present")
    void allEmptyCoordinators() {
        try (HybridGraphSearchService emptyService =
                new HybridGraphSearchService(
                        (tid, q, k) -> List.of(),
                        (tid, q, k) -> List.of(),
                        (tid, q, k, hops) -> List.of(),
                        QueryReformulationStage.identity(),
                        1000L)) {

            HybridSearchRequest req =
                    HybridSearchRequest.builder().tableId("t").query("q").topK(5).build();
            HybridSearchResponse resp = emptyService.hybridSearch(req);
            assertThat(resp).isNotNull();
            assertThat(resp.hits()).isEmpty();
            assertThat(resp.latencyBreakdown()).isNotNull();
        }
    }
}
