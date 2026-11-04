package io.github.rawwise.benchmark.vbench;

import io.github.rawwise.axiom.pipeline.benchmark.BenchmarkConfig;
import io.github.rawwise.axiom.pipeline.benchmark.BenchmarkFramework;
import io.github.rawwise.axiom.pipeline.benchmark.BenchmarkResult;
import io.github.rawwise.benchmark.vbench.executors.CogVideoXClient;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * VBench comparison experiment main entry point.
 *
 * <p>Workflow: 1. Initialize CogVideoXClient + VBenchAdapter 2. runBaseline: no RawWise
 * whendirectlygenerategeneratevideo → VBench evaluation → 16 dimensionscore 3. runWithRawWise: use
 * RawWise retrievalconditionassistedgenerategenerate → VBench evaluation → 16 dimensionscore 4.
 * compare: computetwo groupsscoredifference,Output comparison report
 *
 * <p>Integration with RawWise generic BenchmarkFramework: - Reuses BenchmarkConfig /
 * BenchmarkResult / BenchmarkFramework - Video-specific logic encapsulated in generateBaseline() /
 * generateWithRawWise() twoitemsfunctionsin
 */
@Slf4j
public class VideoGenerationBenchmarkRunner {

    private final BenchmarkFramework framework;
    private final VBenchAdapter vbenchAdapter;
    private final CogVideoXClient videoClient;

    public VideoGenerationBenchmarkRunner() {
        this(new BenchmarkFramework(), new VBenchAdapter(), new CogVideoXClient());
    }

    public VideoGenerationBenchmarkRunner(
            BenchmarkFramework framework,
            VBenchAdapter vbenchAdapter,
            CogVideoXClient videoClient) {
        this.framework = framework;
        this.vbenchAdapter = vbenchAdapter;
        this.videoClient = videoClient;
    }

    /** Run full comparison experiment: baseline first, then with-rawwise, then compare. */
    public ExperimentReport runExperiment(BenchmarkConfig config, List<String> prompts) {
        log.info(
                "Starting VBench experiment: {} with {} prompts",
                config.experimentName(),
                prompts.size());

        BenchmarkResult baseline = framework.runBaseline(config, prompts, this::generateBaseline);
        BenchmarkResult experiment =
                framework.runWithRawWise(config, prompts, this::generateWithRawWise);
        BenchmarkResult.Delta delta = framework.compare(baseline, experiment);

        log.info(
                "VBench experiment completed: baseline avg={}, rawwise avg={}",
                baseline.aggregateMetrics().getOrDefault("vbench_avg", 0.0),
                experiment.aggregateMetrics().getOrDefault("vbench_avg", 0.0));

        return new ExperimentReport(baseline, experiment, delta);
    }

    Map<String, Object> generateBaseline(String prompt) {
        CogVideoXClient.GenerationRequest request = CogVideoXClient.GenerationRequest.of(prompt);
        CogVideoXClient.GenerationResponse response = videoClient.generate(request);
        Map<String, Double> vbenchScores =
                vbenchAdapter.evaluate(prompt, response.videoBytes(), false);

        Map<String, Object> result = new HashMap<>(vbenchScores);
        result.put("generation_latency_ms", response.generationLatencyMs());
        result.put("vbench_avg", vbenchAdapter.averageScore(vbenchScores));
        return result;
    }

    Map<String, Object> generateWithRawWise(String prompt) {
        // Simulation: RawWise retrieved reference vectors + correction conditions passed to video
        // model
        float[] referenceEmbedding = new float[] {0.1f, 0.2f, 0.3f, 0.4f};
        String enhancedPrompt = "[RawWise-enhanced] " + prompt;

        CogVideoXClient.GenerationRequest request =
                new CogVideoXClient.GenerationRequest(
                        enhancedPrompt, referenceEmbedding, null, "0-end", Map.of());
        CogVideoXClient.GenerationResponse response = videoClient.generate(request);
        Map<String, Double> vbenchScores =
                vbenchAdapter.evaluate(prompt, response.videoBytes(), true);

        Map<String, Object> result = new HashMap<>(vbenchScores);
        result.put("generation_latency_ms", response.generationLatencyMs());
        result.put("vbench_avg", vbenchAdapter.averageScore(vbenchScores));
        return result;
    }

    public record ExperimentReport(
            BenchmarkResult baseline, BenchmarkResult experiment, BenchmarkResult.Delta delta) {
        public String summary() {
            StringBuilder sb = new StringBuilder();
            sb.append("=== VBench Experiment Report ===\n");
            sb.append(
                    String.format(
                            "Baseline samples=%d, RawWise samples=%d%n",
                            baseline.sampleCount(), experiment.sampleCount()));
            sb.append(
                    String.format(
                            "VBench avg: baseline=%.4f, rawwise=%.4f, delta=%+.4f%n",
                            baseline.aggregateMetrics().getOrDefault("vbench_avg", 0.0),
                            experiment.aggregateMetrics().getOrDefault("vbench_avg", 0.0),
                            delta.improvement("vbench_avg")));
            sb.append("\nPer-dimension delta:\n");
            for (String dim : VBenchAdapter.VBENCH_DIMENSIONS) {
                double imp = delta.improvement(dim);
                if (Math.abs(imp) > 0.001) {
                    sb.append(String.format("  %s: %+.4f%n", dim, imp));
                }
            }
            return sb.toString();
        }
    }
}
