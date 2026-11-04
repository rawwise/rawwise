package io.github.rawwise.benchmark.vbench.feedback;

import java.nio.file.Path;

public final class VideoFeedbackLoopRunner {
    private VideoFeedbackLoopRunner() {}

    public static void main(String[] args) throws Exception {
        Path resultFile =
                args.length > 0
                        ? Path.of(args[0])
                        : Path.of(
                                "benchmark/vbench/eval_results/prompt_001_real_cogvideox3_mps_subset.json");
        String axiomBaseUrl =
                System.getProperty(
                        "rawwise.axiom.base-url",
                        System.getenv()
                                .getOrDefault("RAWWISE_AXIOM_BASE_URL", "http://localhost:8080"));
        boolean useLlmFeedback = args.length <= 1 || !"--no-llm-feedback".equals(args[1]);

        VideoFeedbackLoop loop =
                new VideoFeedbackLoop(
                        new HttpAxiomClient(axiomBaseUrl), new RuleBasedLlmVideoFeedbackProvider());
        VideoFeedbackLoopReport report = loop.ingestAndAnnotate(resultFile, useLlmFeedback);
        System.out.printf(
                "Ingested video feedback loop: promptId=%s, records=%d, autoFeedback=%d, llmFeedback=%d%n",
                report.promptId(),
                report.ingestedRecordCount(),
                report.autoFeedbackCount(),
                report.llmFeedbackCount());
    }
}
