package io.github.rawwise.benchmark.vbench;

import io.github.rawwise.axiom.pipeline.benchmark.BenchmarkConfig;
import io.github.rawwise.axiom.pipeline.benchmark.BenchmarkResult;
import io.github.rawwise.axiom.pipeline.cognition.ActionExecutor;
import io.github.rawwise.axiom.pipeline.cognition.ActionExecutorRegistry;
import io.github.rawwise.axiom.pipeline.cognition.CognitionEngine;
import io.github.rawwise.axiom.pipeline.evaluation.ExternalEvaluationProvider;
import io.github.rawwise.benchmark.vbench.chunking.VideoFrameChunkingStrategy;
import io.github.rawwise.benchmark.vbench.embedding.ClipEmbeddingProvider;
import io.github.rawwise.benchmark.vbench.executors.VideoGenerationActionExecutor;
import io.github.rawwise.benchmark.vbench.providers.ClipScoreProvider;
import io.github.rawwise.engine.vector.MultimodalEmbeddingProvider;
import io.github.rawwise.model.record.Action;
import io.github.rawwise.model.record.Chunk;
import java.util.List;
import java.util.Map;

/**
 * VBench end-to-end smoke verification: executable main, runs all components to verify integration
 * correctness.
 *
 * <p>Verification points: 1. ClipEmbeddingProvider implements MultimodalEmbeddingProvider SPI
 * (multimodal embedding) 2. ClipScoreProvider implements ExternalEvaluationProvider SPI (video
 * evaluation) 3. VideoGenerationActionExecutor implements ActionExecutor SPI (correction trigger)
 * 4. ActionExecutorRegistry registers and resolves CORRECT action 5. VideoFrameChunkingStrategy
 * implements ChunkingStrategy SPI (video frame splitting) 6. VBenchAdapter outputs 16-dimension
 * scores 7. VideoGenerationBenchmarkRunner full comparison experiment
 *
 * <p>runrow: mvn exec:java -pl rawwise-benchmark \
 * -Dexec.mainClass=io.github.rawwise.benchmark.vbench.VBenchSmokeRunner
 */
public final class VBenchSmokeRunner {

    private VBenchSmokeRunner() {}

    public static void main(String[] args) {
        System.out.println("=== VBench Smoke Verification ===\n");

        verifyMultimodalEmbedding();
        verifyClipScoreProvider();
        verifyVideoGenerationExecutor();
        verifyActionExecutorRegistry();
        verifyVideoFrameChunking();
        verifyVBenchAdapter();
        VideoGenerationBenchmarkRunner.ExperimentReport report = verifyEndToEndExperiment();

        System.out.println("\n=== Final Experiment Report ===");
        System.out.println(report.summary());
        System.out.println("\n[PASS] All smoke checks passed.");
    }

    private static void verifyMultimodalEmbedding() {
        System.out.println(
                "[1/7] Verifying MultimodalEmbeddingProvider (ClipEmbeddingProvider)...");
        MultimodalEmbeddingProvider provider = new ClipEmbeddingProvider();
        float[] textVec = provider.embed("a red car");
        float[] imageVec = provider.embedImage(new byte[] {1, 2, 3, 4, 5});
        float[] audioVec = provider.embedAudio(new byte[] {6, 7, 8});
        assertEqual(ClipEmbeddingProvider.CLIP_EMBEDDING_DIM, textVec.length, "text dim");
        assertEqual(ClipEmbeddingProvider.CLIP_EMBEDDING_DIM, imageVec.length, "image dim");
        assertEqual(ClipEmbeddingProvider.CLIP_EMBEDDING_DIM, audioVec.length, "audio dim");
        assertTrue(provider.modelIdentifier().contains("clip"), "model id contains clip");
        System.out.println(
                "  ✓ 3 modalities × " + ClipEmbeddingProvider.CLIP_EMBEDDING_DIM + " dim");
    }

    private static void verifyClipScoreProvider() {
        System.out.println("[2/7] Verifying ClipScoreProvider (ExternalEvaluationProvider)...");
        ExternalEvaluationProvider provider = new ClipScoreProvider();
        Map<String, Object> params =
                Map.of(
                        ClipScoreProvider.PARAM_CLIP_AESTHETIC, 0.72,
                        ClipScoreProvider.PARAM_CLIP_TEXT_IMAGE, 0.85,
                        ClipScoreProvider.PARAM_SUBJECT_CONSISTENCY, 0.78);
        var aesthetic = provider.evaluate(null, "PERCEPTUAL_QUALITY", params);
        var following = provider.evaluate(null, "INSTRUCTION_FOLLOWING", params);
        var coherence = provider.evaluate(null, "DOMAIN_COHERENCE", params);
        assertTrue(aesthetic.isPresent() && aesthetic.get() == 0.72, "perceptual quality");
        assertTrue(following.isPresent() && following.get() == 0.85, "instruction following");
        assertTrue(coherence.isPresent() && coherence.get() == 0.78, "domain coherence");
        System.out.println("  ✓ 3 video evaluation dimensions wired");
    }

    private static void verifyVideoGenerationExecutor() {
        System.out.println("[3/7] Verifying VideoGenerationActionExecutor (ActionExecutor)...");
        ActionExecutor executor = new VideoGenerationActionExecutor();
        assertTrue(
                executor.supportedActions().contains(Action.ActionType.CORRECT),
                "supports CORRECT");

        Action action =
                new Action(
                        "act-test-1",
                        List.of("sem-1"),
                        Action.ActionType.CORRECT,
                        new byte[0],
                        true);
        StubExecutionContext context =
                new StubExecutionContext(
                        Map.of(
                                "prompt", "A young woman in a red jacket",
                                "reference_embedding", new float[] {0.1f, 0.2f, 0.3f},
                                "drift_description", "Frame 37: jacket color drift",
                                "frame_range", "37"));

        CognitionEngine.ActionResult result = executor.execute(action, context);
        assertTrue(result.success(), "execution succeeded");
        assertTrue(result.artifacts().containsKey("model_version"), "has model_version");
        System.out.println(
                "  ✓ Action executed, latency="
                        + result.artifacts().get("generation_latency_ms")
                        + "ms");
    }

    private static void verifyActionExecutorRegistry() {
        System.out.println("[4/7] Verifying ActionExecutorRegistry integration...");
        ActionExecutorRegistry registry = new ActionExecutorRegistry();
        VideoGenerationActionExecutor videoExecutor = new VideoGenerationActionExecutor();
        registry.register(Action.ActionType.CORRECT, videoExecutor);

        ActionExecutor resolved = registry.resolve(Action.ActionType.CORRECT);
        assertTrue(
                resolved instanceof VideoGenerationActionExecutor,
                "resolved to VideoGenerationActionExecutor");
        assertEqual(videoExecutor.executorId(), resolved.executorId(), "same instance");
        System.out.println("  ✓ CORRECT action routes to video generation executor");
    }

    private static void verifyVideoFrameChunking() {
        System.out.println("[5/7] Verifying VideoFrameChunkingStrategy (ChunkingStrategy)...");
        VideoFrameChunkingStrategy strategy = new VideoFrameChunkingStrategy();
        assertEqual(Chunk.ChunkType.IMAGE_REGION, strategy.supportedType(), "supported type");

        byte[] mockVideo = new byte[1024 * 5]; // 5KB mock video
        for (int i = 0; i < mockVideo.length; i++) mockVideo[i] = (byte) (i % 256);

        List<byte[]> frames =
                strategy.split(
                        mockVideo,
                        Map.of(
                                VideoFrameChunkingStrategy.PARAM_FRAME_RATE, 24,
                                VideoFrameChunkingStrategy.PARAM_BYTES_PER_FRAME, 512));
        assertEqual(10, frames.size(), "10 frames from 5KB / 512B each");

        VideoFrameChunkingStrategy.FrameHeader header =
                VideoFrameChunkingStrategy.decodeFrameHeader(frames.get(0));
        assertEqual(0, header.frameIndex(), "frame 0 index");
        assertEqual(24, header.frameRate(), "frame rate");
        System.out.println("  ✓ Split into " + frames.size() + " frames");
    }

    private static void verifyVBenchAdapter() {
        System.out.println("[6/7] Verifying VBenchAdapter...");
        VBenchAdapter adapter = new VBenchAdapter();
        Map<String, Double> baseline = adapter.evaluate("A horse galloping", new byte[] {1}, false);
        Map<String, Double> rawwise = adapter.evaluate("A horse galloping", new byte[] {1}, true);

        assertEqual(16, baseline.size(), "16 VBench dimensions");
        assertTrue(
                rawwise.get("subject_consistency") > baseline.get("subject_consistency"),
                "rawwise improves subject_consistency");
        assertTrue(
                rawwise.get("temporal_flickering") > baseline.get("temporal_flickering"),
                "rawwise improves temporal_flickering");
        System.out.println(
                "  ✓ Baseline avg="
                        + String.format("%.4f", adapter.averageScore(baseline))
                        + ", RawWise avg="
                        + String.format("%.4f", adapter.averageScore(rawwise)));
    }

    private static VideoGenerationBenchmarkRunner.ExperimentReport verifyEndToEndExperiment() {
        System.out.println("[7/7] Running end-to-end experiment...");
        VideoGenerationBenchmarkRunner runner = new VideoGenerationBenchmarkRunner();
        BenchmarkConfig config = new BenchmarkConfig("smoke-test", 0, 1, Map.of());
        VideoGenerationBenchmarkRunner.ExperimentReport report =
                runner.runExperiment(config, VideoPromptDataset.miniSet());

        BenchmarkResult baseline = report.baseline();
        BenchmarkResult experiment = report.experiment();
        assertEqual(
                VideoPromptDataset.miniSet().size(), baseline.sampleCount(), "baseline samples");
        assertEqual(
                VideoPromptDataset.miniSet().size(),
                experiment.sampleCount(),
                "experiment samples");
        assertTrue(
                experiment.aggregateMetrics().getOrDefault("vbench_avg", 0.0)
                        > baseline.aggregateMetrics().getOrDefault("vbench_avg", 0.0),
                "rawwise vbench_avg > baseline");
        System.out.println("  ✓ Experiment completed");
        return report;
    }

    private static void assertEqual(Object expected, Object actual, String msg) {
        if (expected instanceof Number en && actual instanceof Number an) {
            if (en.doubleValue() != an.doubleValue()) {
                throw new AssertionError(msg + ": expected " + expected + " but got " + actual);
            }
        } else if (!java.util.Objects.equals(expected, actual)) {
            throw new AssertionError(msg + ": expected " + expected + " but got " + actual);
        }
    }

    private static void assertTrue(boolean cond, String msg) {
        if (!cond) {
            throw new AssertionError("Assertion failed: " + msg);
        }
    }

    static class StubExecutionContext
            implements io.github.rawwise.axiom.pipeline.cognition.ExecutionContext {
        private final Map<String, Object> params;

        StubExecutionContext(Map<String, Object> params) {
            this.params = params;
        }

        @Override
        public io.github.rawwise.model.record.Raw raw() {
            return null;
        }

        @Override
        public List<io.github.rawwise.model.record.Chunk> chunks() {
            return List.of();
        }

        @Override
        public List<io.github.rawwise.model.record.View> views() {
            return List.of();
        }

        @Override
        public java.util.Optional<io.github.rawwise.model.record.Semantic> semantics() {
            return java.util.Optional.empty();
        }

        @Override
        public Map<String, Object> params() {
            return params;
        }
    }
}
