package io.github.rawwise.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rawwise.axiom.pipeline.benchmark.BenchmarkConfig;
import io.github.rawwise.axiom.pipeline.benchmark.BenchmarkFramework;
import io.github.rawwise.axiom.pipeline.benchmark.BenchmarkResult;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class BenchmarkFrameworkE2ETest {

    @Test
    void runBaselineProducesResult() {
        BenchmarkFramework framework = new BenchmarkFramework();
        BenchmarkConfig config = new BenchmarkConfig("test", 3, 5, Map.of());
        List<String> prompts = List.of("prompt-1", "prompt-2");

        BenchmarkResult result =
                framework.runBaseline(
                        config, prompts, prompt -> Map.of("output", prompt + "_output"));

        assertNotNull(result);
        assertEquals("test", result.experimentName());
        assertEquals(2 * 5, result.sampleCount());
        assertFalse(result.hasRawWise());
    }

    @Test
    void runWithRawWiseProducesResult() {
        BenchmarkFramework framework = new BenchmarkFramework();
        BenchmarkConfig config =
                new BenchmarkConfig("test-rw", 2, 3, Map.of("rawwise.enabled", "true"));
        List<String> prompts = List.of("prompt-1");

        BenchmarkResult result =
                framework.runWithRawWise(
                        config,
                        prompts,
                        prompt -> Map.of("output", prompt + "_rawwise", "score", 0.85));

        assertNotNull(result);
        assertTrue(result.hasRawWise());
        assertEquals(1 * 3, result.sampleCount());
    }

    @Test
    void compareReturnsDelta() {
        BenchmarkFramework framework = new BenchmarkFramework();
        BenchmarkConfig config = new BenchmarkConfig("compare-test", 1, 1, Map.of());
        List<String> prompts = List.of("p1");

        BenchmarkResult baseline =
                framework.runBaseline(config, prompts, p -> Map.of("score", 0.75));
        BenchmarkResult experiment =
                framework.runWithRawWise(config, prompts, p -> Map.of("score", 0.82));

        BenchmarkResult.Delta delta = framework.compare(baseline, experiment);

        assertNotNull(delta);
        assertEquals(0.07, delta.improvement("score"), 0.001);
    }

    @Test
    void concurrentRunBaselineAndWithRawWise_noConflict() throws Exception {
        BenchmarkFramework framework = new BenchmarkFramework();
        BenchmarkConfig config = new BenchmarkConfig("concurrent-test", 1, 1, Map.of());
        List<String> prompts = List.of("p1", "p2");

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var baselineFuture =
                    executor.submit(
                            () ->
                                    framework.runBaseline(
                                            config, prompts, p -> Map.of("score", 0.7)));
            var rawwiseFuture =
                    executor.submit(
                            () ->
                                    framework.runWithRawWise(
                                            config, prompts, p -> Map.of("score", 0.8)));

            BenchmarkResult baseline = baselineFuture.get(5, TimeUnit.SECONDS);
            BenchmarkResult experiment = rawwiseFuture.get(5, TimeUnit.SECONDS);

            assertNotNull(baseline);
            assertNotNull(experiment);
            assertFalse(baseline.hasRawWise());
            assertTrue(experiment.hasRawWise());
        }
    }

    @Test
    void aggregateNumericMetricsCalculatesMean() {
        List<Map<String, Object>> samples =
                List.of(
                        Map.of("score", 0.7, "latency", 10.0),
                        Map.of("score", 0.8, "latency", 20.0));

        Map<String, Double> aggregated = BenchmarkFramework.aggregateNumericMetrics(samples);

        assertEquals(0.75, aggregated.get("score"), 0.001);
        assertEquals(15.0, aggregated.get("latency"), 0.001);
    }

    @Test
    void configRejectsNullExperimentName() {
        IllegalArgumentException ex =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new BenchmarkConfig(null, 0, 1, Map.of()));
        assertTrue(ex.getMessage().contains("experimentName"));
    }
}
