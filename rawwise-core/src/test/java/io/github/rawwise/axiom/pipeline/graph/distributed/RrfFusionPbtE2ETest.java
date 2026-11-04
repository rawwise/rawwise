package io.github.rawwise.axiom.pipeline.graph.distributed;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Property-based E2E tests for {@link RrfFusion} mathematical invariants (per Phase C design doc
 * §16.3 PBT requirements).
 *
 * <p>Invariants verified:
 *
 * <ul>
 *   <li><b>Pbt_RRFScoreInRange</b>: all fused scores are in (0, sum(weights)] (since each rank
 *       contributes at most w/k+1 and w/k+rank decreases with rank).
 *   <li><b>Pbt_RRFRankMonotone</b>: within a single source, doc at lower rank contributes more than
 *       doc at higher rank.
 *   <li><b>Pbt_RRFOrderingStable</b>: fused output is sorted strictly by score desc.
 *   <li><b>Pbt_RRFMultiSourceBoost</b>: a doc appearing in N sources outranks a doc appearing in 1
 *       source at the same rank, given equal weights.
 * </ul>
 */
@DisplayName("RrfFusion PBT - mathematical invariants over random inputs")
class RrfFusionPbtE2ETest {

    private static final long SEED = 42L;
    private static final int TRIALS = 30;
    private static final int MAX_HITS_PER_SOURCE = 20;

    @Test
    @DisplayName("Pbt_RRFScoreInRange: all fused scores are positive and bounded")
    void scoresAreInRange() {
        Random rng = new Random(SEED);
        double totalWeight =
                RrfFusion.DEFAULT_WEIGHTS.values().stream().mapToDouble(Double::doubleValue).sum();
        // Upper bound: sum of (w_source / (k+1)) for all sources where same doc tops every source.
        // i.e. sum(weights) / (k+1)
        double upperBound = totalWeight / (RrfFusion.DEFAULT_RRF_K + 1.0) + 1e-9;
        for (int trial = 0; trial < TRIALS; trial++) {
            Map<String, List<RrfFusion.ScoredHit>> sources = randomSources(rng);
            List<RrfFusion.ScoredHit> fused = RrfFusion.fuse(sources);
            for (RrfFusion.ScoredHit hit : fused) {
                assertThat(hit.score())
                        .as("trial=%d hit=%s score must be > 0", trial, hit.id())
                        .isGreaterThan(0.0);
                assertThat(hit.score())
                        .as(
                                "trial=%d hit=%s score must be ≤ %f (upper bound)",
                                trial, hit.id(), upperBound)
                        .isLessThanOrEqualTo(upperBound);
            }
        }
    }

    @Test
    @DisplayName(
            "Pbt_RRFRankMonotone: within a single source, lower rank gives higher contribution")
    void rankMonotoneWithinSource() {
        List<RrfFusion.ScoredHit> vector = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            vector.add(new RrfFusion.ScoredHit("d" + i, 1.0 - i * 0.01, Map.of()));
        }
        Map<String, List<RrfFusion.ScoredHit>> sources = new LinkedHashMap<>();
        sources.put("vector", vector);
        List<RrfFusion.ScoredHit> fused = RrfFusion.fuse(sources);

        // Build ordered position lookup: same order as fused list (sorted by score desc)
        Map<String, Integer> finalPos = new LinkedHashMap<>();
        for (int i = 0; i < fused.size(); i++) {
            finalPos.put(fused.get(i).id(), i);
        }
        // d0 (input rank=1) must come before d9 (input rank=10) in fused output
        assertThat(finalPos.get("d0")).isLessThan(finalPos.get("d9"));
    }

    @Test
    @DisplayName("Pbt_RRFOrderingStable: fused output is sorted strictly by score desc")
    void orderingByScoreDesc() {
        Random rng = new Random(SEED + 1);
        for (int trial = 0; trial < TRIALS; trial++) {
            Map<String, List<RrfFusion.ScoredHit>> sources = randomSources(rng);
            List<RrfFusion.ScoredHit> fused = RrfFusion.fuse(sources);
            for (int i = 1; i < fused.size(); i++) {
                assertThat(fused.get(i - 1).score())
                        .as("trial=%d fused[%d].score >= fused[%d].score", trial, i - 1, i)
                        .isGreaterThanOrEqualTo(fused.get(i).score());
            }
        }
    }

    @Test
    @DisplayName("Pbt_RRFMultiSourceBoost: doc in N sources outranks doc in 1 source at same rank")
    void multiSourceBoost() {
        // 'common' appears at rank=1 in all 3 sources
        // 'vec-only' appears at rank=1 only in vector
        List<RrfFusion.ScoredHit> vector =
                List.of(
                        new RrfFusion.ScoredHit("common", 0.9, Map.of()),
                        new RrfFusion.ScoredHit("vec-only", 0.88, Map.of()));
        List<RrfFusion.ScoredHit> keyword =
                List.of(new RrfFusion.ScoredHit("common", 0.7, Map.of()));
        List<RrfFusion.ScoredHit> graph = List.of(new RrfFusion.ScoredHit("common", 0.6, Map.of()));

        Map<String, List<RrfFusion.ScoredHit>> sources =
                RrfFusion.sourcesOf(vector, keyword, graph);
        List<RrfFusion.ScoredHit> fused = RrfFusion.fuse(sources);

        double commonScore = 0;
        double vecOnlyScore = 0;
        for (RrfFusion.ScoredHit hit : fused) {
            if ("common".equals(hit.id())) commonScore = hit.score();
            if ("vec-only".equals(hit.id())) vecOnlyScore = hit.score();
        }
        assertThat(commonScore)
                .as("doc in 3 sources must outrank doc in 1 source")
                .isGreaterThan(vecOnlyScore);
    }

    private static Map<String, List<RrfFusion.ScoredHit>> randomSources(Random rng) {
        int vCount = rng.nextInt(MAX_HITS_PER_SOURCE) + 1;
        int kCount = rng.nextInt(MAX_HITS_PER_SOURCE) + 1;
        int gCount = rng.nextInt(MAX_HITS_PER_SOURCE) + 1;
        List<RrfFusion.ScoredHit> vector = new ArrayList<>();
        List<RrfFusion.ScoredHit> keyword = new ArrayList<>();
        List<RrfFusion.ScoredHit> graph = new ArrayList<>();
        java.util.LinkedHashSet<String> vSeen = new java.util.LinkedHashSet<>();
        java.util.LinkedHashSet<String> kSeen = new java.util.LinkedHashSet<>();
        java.util.LinkedHashSet<String> gSeen = new java.util.LinkedHashSet<>();
        for (int i = 0; i < vCount; i++) {
            String id = "v" + i;
            if (vSeen.add(id)) {
                vector.add(new RrfFusion.ScoredHit(id, rng.nextDouble(), Map.of()));
            }
        }
        for (int i = 0; i < kCount; i++) {
            String id = rng.nextBoolean() ? "v" + (i % Math.max(vCount, 1)) : "k" + i;
            if (kSeen.add(id)) {
                keyword.add(new RrfFusion.ScoredHit(id, rng.nextDouble(), Map.of()));
            }
        }
        for (int i = 0; i < gCount; i++) {
            String id = rng.nextBoolean() ? "v" + (i % Math.max(vCount, 1)) : "g" + i;
            if (gSeen.add(id)) {
                graph.add(new RrfFusion.ScoredHit(id, rng.nextDouble(), Map.of()));
            }
        }
        return RrfFusion.sourcesOf(vector, keyword, graph);
    }
}
