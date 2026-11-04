package io.github.rawwise.axiom.memory.semantic;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * E2E test wrapper for {@link GraphTraversalMicrobench}.
 *
 * <p>Drives the microbench through the production GraphEngine entry path (DefaultGraphEngine +
 * InMemory backends) and asserts on real latency invariants.
 *
 * <p>The default {@code runMicrobench} test runs N=1000 (CI-friendly). Larger scales (N=10000,
 * N=100000) are exposed via @Disabled tests for manual runs — invoke them by removing @Disabled or
 * via the main class directly.
 */
@Slf4j
class GraphTraversalE2EBenchTest {

    @Test
    void runMicrobench() throws Exception {
        GraphTraversalMicrobench.Result r = GraphTraversalMicrobench.runOne(1_000);
        assertNotNull(r.hop1, "hop1 stats must be present");
        assertNotNull(r.hop2, "hop2 stats must be present");
        assertTrue(r.totalEdges > 0, "edges must have been built, got " + r.totalEdges);
        assertTrue(r.hop1.avgNeighbors > 0, "single-hop should return some neighbors");
        assertTrue(
                r.hop2.avgNeighbors >= r.hop1.avgNeighbors,
                "2-hop neighbor count should be >= 1-hop, got hop1="
                        + r.hop1.avgNeighbors
                        + " hop2="
                        + r.hop2.avgNeighbors);
        assertTrue(
                r.hop1.p95Ms < 50.0,
                "single-hop p95 should be < 50ms at N=1000, got " + r.hop1.p95Ms + "ms");
        assertTrue(
                r.hop2.p95Ms < 200.0,
                "2-hop p95 should be < 200ms at N=1000, got " + r.hop2.p95Ms + "ms");
        log.info("N=1000 hop1 p95={}ms hop2 p95={}ms", r.hop1.p95Ms, r.hop2.p95Ms);
    }

    @Test
    @Disabled("manual benchmark, run via Microbench main 10k")
    void runN10k() throws Exception {
        GraphTraversalMicrobench.Result r = GraphTraversalMicrobench.runOne(10_000);
        assertNotNull(r.hop1);
        assertNotNull(r.hop2);
    }

    @Test
    @Disabled("manual benchmark, run via Microbench main 100k")
    void runN100k() throws Exception {
        GraphTraversalMicrobench.Result r = GraphTraversalMicrobench.runOne(100_000);
        assertNotNull(r.hop1);
        assertNotNull(r.hop2);
    }
}
