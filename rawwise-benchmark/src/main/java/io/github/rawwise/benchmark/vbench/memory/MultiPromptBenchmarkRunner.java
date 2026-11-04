package io.github.rawwise.benchmark.vbench.memory;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.benchmark.vbench.VideoPromptDataset;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Multi-prompt batch Closed-Loop Benchmark Runner.
 *
 * <p>foreach prompt run A→B→C three rounds,collectresultsafterdostatisticstest: : B > A(prompt
 * enhancement effective) : C > B(feedback loop additional gain) : C > A(comprehensive gain)
 */
@Slf4j
public class MultiPromptBenchmarkRunner {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final String rawwiseBaseUrl;
    private final List<String> prompts;
    private final List<ClosedLoopBenchmarkRunner.PromptResult> results = new ArrayList<>();

    public MultiPromptBenchmarkRunner(String rawwiseBaseUrl, List<String> prompts) {
        this.rawwiseBaseUrl = rawwiseBaseUrl;
        this.prompts = prompts;
    }

    public void run() {
        System.out.println("=== Multi-Prompt Closed-Loop Benchmark ===");
        System.out.println("Prompts: " + prompts.size());
        System.out.println("RawWise: " + rawwiseBaseUrl);
        System.out.println();

        for (int i = 0; i < prompts.size(); i++) {
            String prompt = prompts.get(i);
            String promptId = String.format("multi_p%02d", i + 1);

            System.out.println("================================================================");
            System.out.printf(
                    "[%d/%d] Prompt: %s%n",
                    i + 1, prompts.size(), prompt.substring(0, Math.min(60, prompt.length())));
            System.out.println("================================================================");

            try {
                ClosedLoopBenchmarkRunner runner =
                        new ClosedLoopBenchmarkRunner(rawwiseBaseUrl, prompt, promptId);
                runner.runClosedLoop();
                ClosedLoopBenchmarkRunner.PromptResult result = runner.getLastResult();
                if (result != null) {
                    results.add(result);
                    System.out.printf(
                            "[%d/%d] DONE: A=%.4f B=%.4f C=%.4f%n%n",
                            i + 1,
                            prompts.size(),
                            result.scoreA(),
                            result.scoreB(),
                            result.scoreC());
                }
            } catch (Exception e) {
                System.out.printf("[%d/%d] FAILED: %s%n%n", i + 1, prompts.size(), e.getMessage());
            }
        }

        printSummary();
        saveResults();
    }

    private void printSummary() {
        System.out.println();
        System.out.println("================================================================");
        System.out.println("=== STATISTICAL SUMMARY ===");
        System.out.println("================================================================");
        System.out.println();

        System.out.printf(
                "%-4s %-50s %8s %8s %8s %8s %8s%n",
                "#", "Prompt (truncated)", "A", "B", "C", "B-A", "C-B");
        System.out.println("-".repeat(120));

        for (int i = 0; i < results.size(); i++) {
            ClosedLoopBenchmarkRunner.PromptResult r = results.get(i);
            System.out.printf(
                    "%-4d %-50s %8.4f %8.4f %8.4f %+8.4f %+8.4f%n",
                    i + 1,
                    r.prompt().substring(0, Math.min(50, r.prompt().length())),
                    r.scoreA(),
                    r.scoreB(),
                    r.scoreC(),
                    r.scoreB() - r.scoreA(),
                    r.scoreC() - r.scoreB());
        }

        System.out.println("-".repeat(120));

        double meanA =
                results.stream()
                        .mapToDouble(ClosedLoopBenchmarkRunner.PromptResult::scoreA)
                        .average()
                        .orElse(0);
        double meanB =
                results.stream()
                        .mapToDouble(ClosedLoopBenchmarkRunner.PromptResult::scoreB)
                        .average()
                        .orElse(0);
        double meanC =
                results.stream()
                        .mapToDouble(ClosedLoopBenchmarkRunner.PromptResult::scoreC)
                        .average()
                        .orElse(0);

        double stdA =
                stdDev(
                        results.stream()
                                .mapToDouble(ClosedLoopBenchmarkRunner.PromptResult::scoreA)
                                .toArray());
        double stdB =
                stdDev(
                        results.stream()
                                .mapToDouble(ClosedLoopBenchmarkRunner.PromptResult::scoreB)
                                .toArray());
        double stdC =
                stdDev(
                        results.stream()
                                .mapToDouble(ClosedLoopBenchmarkRunner.PromptResult::scoreC)
                                .toArray());

        System.out.printf(
                "%-4s %-50s %8.4f %8.4f %8.4f %+8.4f %+8.4f%n",
                "", "MEAN", meanA, meanB, meanC, meanB - meanA, meanC - meanB);
        System.out.printf("%-4s %-50s %8.4f %8.4f %8.4f%n", "", "STD", stdA, stdB, stdC);

        System.out.println();
        System.out.println("=== HYPOTHESIS TESTING ===");

        double[] deltaAB = results.stream().mapToDouble(r -> r.scoreB() - r.scoreA()).toArray();
        double[] deltaBC = results.stream().mapToDouble(r -> r.scoreC() - r.scoreB()).toArray();
        double[] deltaAC = results.stream().mapToDouble(r -> r.scoreC() - r.scoreA()).toArray();

        double meanAB = mean(deltaAB);
        double meanBC = mean(deltaBC);
        double meanAC = mean(deltaAC);

        double pAB = pairedTTestPValue(deltaAB);
        double pBC = pairedTTestPValue(deltaBC);
        double pAC = pairedTTestPValue(deltaAC);

        int winAB = countPositive(deltaAB);
        int winBC = countPositive(deltaBC);
        int winAC = countPositive(deltaAC);

        System.out.printf(
                "H1 (A->B): mean_delta=%+.4f, p=%.4f, wins=%d/%d %s%n",
                meanAB, pAB, winAB, results.size(), pAB < 0.05 ? "*** SIGNIFICANT" : "");
        System.out.printf(
                "H2 (B->C): mean_delta=%+.4f, p=%.4f, wins=%d/%d %s%n",
                meanBC, pBC, winBC, results.size(), pBC < 0.05 ? "*** SIGNIFICANT" : "");
        System.out.printf(
                "H3 (A->C): mean_delta=%+.4f, p=%.4f, wins=%d/%d %s%n",
                meanAC, pAC, winAC, results.size(), pAC < 0.05 ? "*** SIGNIFICANT" : "");

        System.out.println();
        System.out.println("=== CONCLUSION ===");
        if (pAC < 0.05 && meanAC > 0) {
            System.out.println("RawWise AI-native brings significant improvement (A->C).");
            if (pBC < 0.05 && meanBC > 0) {
                System.out.println(
                        "Feedback loop brings additional significant gain (B->C). H2 VERIFIED.");
            } else {
                System.out.println("Feedback loop gain (B->C) not statistically significant yet.");
            }
        } else {
            System.out.println(
                    "No significant improvement detected. Need more samples or better enhancement.");
        }
    }

    private void saveResults() {
        try {
            String outputPath = "benchmark/vbench/eval_results/multi_prompt_results.json";
            List<Map<String, Object>> jsonResults = new ArrayList<>();
            for (ClosedLoopBenchmarkRunner.PromptResult r : results) {
                Map<String, Object> entry = new HashMap<>();
                entry.put("prompt", r.prompt());
                entry.put("prompt_id", r.promptId());
                entry.put("score_a", r.scoreA());
                entry.put("score_b", r.scoreB());
                entry.put("score_c", r.scoreC());
                entry.put("delta_ab", r.scoreB() - r.scoreA());
                entry.put("delta_bc", r.scoreC() - r.scoreB());
                entry.put("delta_ac", r.scoreC() - r.scoreA());
                jsonResults.add(entry);
            }
            Map<String, Object> root = new HashMap<>();
            root.put("results", jsonResults);
            root.put("total_prompts", results.size());

            double meanA =
                    results.stream()
                            .mapToDouble(ClosedLoopBenchmarkRunner.PromptResult::scoreA)
                            .average()
                            .orElse(0);
            double meanB =
                    results.stream()
                            .mapToDouble(ClosedLoopBenchmarkRunner.PromptResult::scoreB)
                            .average()
                            .orElse(0);
            double meanC =
                    results.stream()
                            .mapToDouble(ClosedLoopBenchmarkRunner.PromptResult::scoreC)
                            .average()
                            .orElse(0);
            root.put("mean_a", meanA);
            root.put("mean_b", meanB);
            root.put("mean_c", meanC);
            root.put("mean_delta_ab", meanB - meanA);
            root.put("mean_delta_bc", meanC - meanB);
            root.put("mean_delta_ac", meanC - meanA);

            try (FileWriter fw = new FileWriter(outputPath)) {
                MAPPER.writerWithDefaultPrettyPrinter().writeValue(fw, root);
            }
            System.out.println("\nResults saved to: " + outputPath);
        } catch (Exception e) {
            System.out.println("Failed to save results: " + e.getMessage());
        }
    }

    private static double mean(double[] values) {
        if (values.length == 0) return 0;
        double sum = 0;
        for (double v : values) sum += v;
        return sum / values.length;
    }

    private static double stdDev(double[] values) {
        if (values.length < 2) return 0;
        double m = mean(values);
        double sumSq = 0;
        for (double v : values) sumSq += (v - m) * (v - m);
        return Math.sqrt(sumSq / (values.length - 1));
    }

    private static int countPositive(double[] values) {
        int count = 0;
        for (double v : values) {
            if (v > 0) count++;
        }
        return count;
    }

    /**
     * One-tailed paired t-test p-value (: mean > 0). Using normal approximation (sufficient when
     * n>=15).
     */
    private static double pairedTTestPValue(double[] deltas) {
        if (deltas.length < 2) return 1.0;
        double m = mean(deltas);
        double sd = stdDev(deltas);
        if (sd == 0) return m > 0 ? 0.0 : 1.0;
        double se = sd / Math.sqrt(deltas.length);
        double t = m / se;
        double p = 0.5 * (1 - erf(Math.abs(t) / Math.sqrt(2)));
        return m > 0 ? p : 1.0 - p;
    }

    private static double erf(double x) {
        double t = 1.0 / (1.0 + 0.3275911 * x);
        double y =
                1.0
                        - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736)
                                                * t
                                        + 0.254829592)
                                * t
                                * Math.exp(-x * x);
        return y;
    }

    public static void main(String[] args) {
        String baseUrl = System.getProperty("rawwise.base.url", "http://localhost:8080");

        List<String> prompts;
        int maxPrompts;
        if (args.length > 0) {
            maxPrompts = Integer.parseInt(args[0]);
        } else {
            maxPrompts = 15;
        }

        prompts = VideoPromptDataset.smallSet();
        if (maxPrompts < prompts.size()) {
            prompts = prompts.subList(0, maxPrompts);
        }

        System.out.println("Starting multi-prompt benchmark with " + prompts.size() + " prompts");
        new MultiPromptBenchmarkRunner(baseUrl, prompts).run();
    }
}
