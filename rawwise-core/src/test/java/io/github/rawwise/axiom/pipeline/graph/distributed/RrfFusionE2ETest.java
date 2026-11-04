package io.github.rawwise.axiom.pipeline.graph.distributed;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("RrfFusion E2E - reciprocal rank fusion across vector/keyword/graph")
class RrfFusionE2ETest {

    @Test
    @DisplayName(
            "fuse with default weights and rrfK=60 ranks docs appearing in multiple sources higher")
    void fuseMultipleSources() {
        List<RrfFusion.ScoredHit> vector =
                List.of(
                        new RrfFusion.ScoredHit("d1", 0.9, Map.of()),
                        new RrfFusion.ScoredHit("d2", 0.7, Map.of()),
                        new RrfFusion.ScoredHit("d3", 0.5, Map.of()));
        List<RrfFusion.ScoredHit> keyword =
                List.of(
                        new RrfFusion.ScoredHit("d2", 0.8, Map.of()),
                        new RrfFusion.ScoredHit("d4", 0.6, Map.of()));
        List<RrfFusion.ScoredHit> graph = List.of(new RrfFusion.ScoredHit("d3", 0.95, Map.of()));

        Map<String, List<RrfFusion.ScoredHit>> sources =
                RrfFusion.sourcesOf(vector, keyword, graph);
        List<RrfFusion.ScoredHit> fused =
                RrfFusion.fuse(sources, RrfFusion.DEFAULT_WEIGHTS, RrfFusion.DEFAULT_RRF_K);

        assertThat(fused).isNotEmpty();
        Map<String, Double> scoreById = new LinkedHashMap<>();
        for (RrfFusion.ScoredHit hit : fused) {
            scoreById.put(hit.id(), hit.score());
        }

        assertThat(scoreById).containsKeys("d1", "d2", "d3", "d4");

        // d2 appears in both vector(rank=2) and keyword(rank=1)
        // d3 appears in both vector(rank=3) and graph(rank=1)
        // d1 only in vector(rank=1), d4 only in keyword(rank=2)
        // RRF rewards multi-source presence, so d2 and d3 should outrank d1 and d4.
        double d1 = scoreById.get("d1");
        double d2 = scoreById.get("d2");
        double d3 = scoreById.get("d3");
        double d4 = scoreById.get("d4");

        assertThat(d2).isGreaterThan(d1);
        assertThat(d2).isGreaterThan(d4);
        assertThat(d3).isGreaterThan(d1);
        assertThat(d3).isGreaterThan(d4);
    }

    @Test
    @DisplayName("Pbt_RRFScoreInRange relaxed: all fused scores are strictly positive")
    void scoresStrictlyPositive() {
        Map<String, List<RrfFusion.ScoredHit>> sources =
                RrfFusion.sourcesOf(
                        List.of(
                                new RrfFusion.ScoredHit("a", 1.0, Map.of()),
                                new RrfFusion.ScoredHit("b", 0.5, Map.of())),
                        List.of(new RrfFusion.ScoredHit("a", 0.7, Map.of())),
                        List.of(new RrfFusion.ScoredHit("b", 0.3, Map.of())));
        List<RrfFusion.ScoredHit> fused = RrfFusion.fuse(sources);
        for (RrfFusion.ScoredHit hit : fused) {
            assertThat(hit.score()).isGreaterThan(0.0);
        }
    }

    @Test
    @DisplayName("empty sources -> empty fused result")
    void emptySourcesReturnEmpty() {
        List<RrfFusion.ScoredHit> fused =
                RrfFusion.fuse(RrfFusion.sourcesOf(List.of(), List.of(), List.of()));
        assertThat(fused).isEmpty();
    }
}
