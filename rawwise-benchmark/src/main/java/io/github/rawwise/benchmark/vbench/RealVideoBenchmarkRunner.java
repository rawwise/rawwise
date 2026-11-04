package io.github.rawwise.benchmark.vbench;

import io.github.rawwise.benchmark.vbench.config.VBenchConfig;
import io.github.rawwise.benchmark.vbench.executors.HttpVideoGenerationClient;
import io.github.rawwise.benchmark.vbench.executors.HttpVideoGenerationClient.VideoGenerationResult;
import io.github.rawwise.benchmark.vbench.providers.PythonClipScoreProvider;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * realactualvideogenerategenerate → write to disk → CLIP evaluation end-to-end Runner.
 *
 * <p>Workflow: 1. Load VBench config (API key / endpoint / output directory) 2. Generate two videos
 * per prompt: baseline + rawwise 3. Write videos to disk (with.meta.json metadata) 4. Call Python
 * CLIP evaluation (using video first frame or screenshot as input) 5. Output comparison report
 *
 * <p>Usage: # 1. Configure: copy config.properties.template to ~/.rawwise/vbench/config.properties
 * # or use environment variable RAWWISE_VBENCH_PROVIDER_API_KEY=xxx # 2. runrow: mvn exec:java -pl
 * rawwise-benchmark \ -Dexec.mainClass=io.github.rawwise.benchmark.vbench.RealVideoBenchmarkRunner
 * \ -Dexec.classpathScope=compile
 *
 * <p># Print config only (no actual run): mvn exec:java -pl rawwise-benchmark \
 * -Dexec.mainClass=io.github.rawwise.benchmark.vbench.RealVideoBenchmarkRunner \
 * -Dexec.args="--print-config"
 *
 * <p>Video artifact location (default): benchmark/vbench/videos/baseline/{prompt_id}_run_0.mp4
 * benchmark/vbench/videos/rawwise/{prompt_id}_run_0.mp4
 * benchmark/vbench/videos/<tag>/{prompt_id}_run_0.meta.json
 */
public final class RealVideoBenchmarkRunner {

    private RealVideoBenchmarkRunner() {}

    public static void main(String[] args) {
        VBenchConfig config = new VBenchConfig();
        config.printEffective();

        boolean printOnly = args.length > 0 && "--print-config".equals(args[0]);
        boolean evalOnly = args.length > 0 && "--eval-only".equals(args[0]);
        if (printOnly) {
            System.out.println("\n[print-config mode] No videos generated.");
            return;
        }

        if (config.getApiKey() == null
                || config.getApiKey().isBlank()
                || "YOUR_API_KEY_HERE".equals(config.getApiKey())) {
            System.out.println("\n⚠️  No real API key configured. Will use STUB generation.");
            System.out.println("   To use real video generation:");
            System.out.println(
                    "   1. cp rawwise-benchmark/benchmark/vbench/config.properties.template ~/.rawwise/vbench/config.properties");
            System.out.println("   2. Edit ~/.rawwise/vbench/config.properties with real API key");
            System.out.println("   3. Or set env: RAWWISE_VBENCH_PROVIDER_API_KEY=xxx\n");
        }

        HttpVideoGenerationClient videoClient = new HttpVideoGenerationClient(config);
        PythonClipScoreProvider clipProvider = new PythonClipScoreProvider();

        List<String> prompts =
                List.of(
                        "A young woman in a red jacket walking down a street",
                        "A horse galloping across an open prairie");

        System.out.println("\n=== Running Real Video Benchmark ===");
        System.out.println("Prompts: " + prompts.size());
        System.out.println("Tags: baseline + rawwise");
        System.out.println("Video output: " + config.getVideosOutputDir());
        System.out.println();

        int promptIdx = 1;
        for (String prompt : prompts) {
            String promptId = "prompt_" + String.format("%03d", promptIdx);
            System.out.printf("[%s] prompt: %s%n", promptId, prompt);

            VideoGenerationResult baseline =
                    evalOnly
                            ? existingResult(config, "baseline", promptId, 0)
                            : generateAndReport(
                                    videoClient, prompt, "baseline", promptId, 0, Map.of());

            Map<String, Object> rawwiseConditions = new HashMap<>();
            rawwiseConditions.put("reference_embedding", new float[] {0.1f, 0.2f, 0.3f});
            rawwiseConditions.put("drift_description", "<RawWise condition stub>");

            VideoGenerationResult rawwise =
                    evalOnly
                            ? existingResult(config, "rawwise", promptId, 0)
                            : generateAndReport(
                                    videoClient,
                                    "[RawWise-enhanced] " + prompt,
                                    "rawwise",
                                    promptId,
                                    0,
                                    rawwiseConditions);

            evaluateWithClip(clipProvider, prompt, baseline, "baseline");
            evaluateWithClip(clipProvider, prompt, rawwise, "rawwise");

            System.out.println();
            promptIdx++;
        }

        System.out.println("=== Real Video Benchmark Complete ===");
        System.out.println(
                "Videos saved under: " + Path.of(config.getVideosOutputDir()).toAbsolutePath());
    }

    private static VideoGenerationResult generateAndReport(
            HttpVideoGenerationClient client,
            String prompt,
            String tag,
            String promptId,
            int runId,
            Map<String, Object> conditions) {
        try {
            VideoGenerationResult result =
                    client.generate(prompt, tag, promptId, runId, conditions);
            System.out.printf(
                    "  ✓ [%s] generated → %s (%d bytes, %d ms)%n",
                    tag, result.videoPath, result.fileSize, result.elapsedMs);
            return result;
        } catch (Exception e) {
            System.out.printf("  ✗ [%s] generation FAILED: %s%n", tag, e.getMessage());
            throw e;
        }
    }

    private static VideoGenerationResult existingResult(
            VBenchConfig config, String tag, String promptId, int runId) {
        Path path = Path.of(config.getVideosOutputDir(), tag, promptId + "_run_" + runId + ".mp4");
        if (!Files.exists(path)) {
            throw new IllegalStateException("Missing existing video for --eval-only: " + path);
        }
        VideoGenerationResult result = new VideoGenerationResult();
        result.videoPath = path;
        try {
            result.fileSize = Files.size(path);
        } catch (Exception e) {
            result.fileSize = -1;
        }
        result.modelVersion = config.getApiModel();
        System.out.printf("  ✓ [%s] using existing → %s (%d bytes)%n", tag, path, result.fileSize);
        return result;
    }

    private static void evaluateWithClip(
            PythonClipScoreProvider provider,
            String prompt,
            VideoGenerationResult videoResult,
            String tag) {
        try {
            ProcessBuilder pb =
                    new ProcessBuilder(
                            "rawwise-benchmark/benchmark/vbench/.venv/bin/python",
                            "rawwise-benchmark/benchmark/vbench/vbench_mps_eval.py",
                            "--video",
                            videoResult.videoPath.toString(),
                            "--prompt",
                            prompt,
                            "--num-frames",
                            "8");
            pb.directory(Path.of(System.getProperty("user.dir")).toFile());
            pb.environment().put("HF_ENDPOINT", "https://hf-mirror.com");
            pb.redirectErrorStream(true);
            Process process = pb.start();
            String output = new String(process.getInputStream().readAllBytes());
            int exit = process.waitFor();
            if (exit != 0) {
                System.out.printf("  ✗ [%s] VBench-subset failed: %s%n", tag, output.strip());
                return;
            }
            String json = output.strip();
            int lastLine = json.lastIndexOf('\n');
            if (lastLine >= 0) {
                json = json.substring(lastLine + 1);
            }
            System.out.printf("  ✓ [%s] VBench-subset: %s%n", tag, json);
        } catch (Exception e) {
            System.out.printf("  ✗ [%s] VBench-subset failed: %s%n", tag, e.getMessage());
        }
    }
}
