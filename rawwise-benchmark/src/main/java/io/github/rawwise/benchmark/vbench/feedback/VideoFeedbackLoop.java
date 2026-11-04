package io.github.rawwise.benchmark.vbench.feedback;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class VideoFeedbackLoop {
    private static final String RUN_TABLE = "default.video_generation_runs";
    private static final String EVAL_TABLE = "default.video_evaluations";
    private static final double TEMPORAL_THRESHOLD = 0.60;
    private static final double OVERALL_THRESHOLD = 0.75;

    private final AxiomClient axiomClient;
    private final LlmVideoFeedbackProvider llmFeedbackProvider;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public VideoFeedbackLoop(
            AxiomClient axiomClient, LlmVideoFeedbackProvider llmFeedbackProvider) {
        this.axiomClient = axiomClient;
        this.llmFeedbackProvider = llmFeedbackProvider;
    }

    public VideoFeedbackLoopReport ingestAndAnnotate(Path resultFile, boolean useLlmFeedback)
            throws Exception {
        JsonNode result = objectMapper.readTree(Files.readString(resultFile));
        String promptId = result.path("prompt_id").asText();
        String prompt = result.path("prompt").asText();
        String provider = result.path("provider").asText();
        String model = result.path("model").asText();

        String baselineViewId =
                ingestRun(promptId, "baseline", prompt, provider, model, result.path("baseline"));
        String rawwiseViewId =
                ingestRun(promptId, "rawwise", prompt, provider, model, result.path("rawwise"));
        ingestEvaluation(promptId, result);

        int autoFeedback = annotateAutomaticFeedback(baselineViewId, rawwiseViewId, result);
        int llmFeedback = 0;
        if (useLlmFeedback) {
            List<AxiomAnnotation> annotations =
                    llmFeedbackProvider.judge(baselineViewId, rawwiseViewId, result);
            for (AxiomAnnotation annotation : annotations) {
                axiomClient.annotate(annotation);
            }
            llmFeedback = annotations.size();
        }
        return new VideoFeedbackLoopReport(promptId, 3, autoFeedback, llmFeedback);
    }

    private String ingestRun(
            String promptId, String tag, String prompt, String provider, String model, JsonNode run)
            throws Exception {
        String key = "vbench:" + promptId + ":" + tag;
        String body = objectMapper.writeValueAsString(run);
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("prompt_id", promptId);
        properties.put("tag", tag);
        properties.put("prompt", prompt);
        properties.put("provider", provider);
        properties.put("model", model);
        properties.put("video_path", run.path("video_path").asText());
        properties.put("vbench_subset_score", run.path("vbench_subset_score").asText());
        properties.put("ingested_at", Instant.now().toString());
        properties.put("text_content", prompt + " " + tag + " " + run.path("scores").toString());
        return axiomClient.ingest(new AxiomIngest(RUN_TABLE, key, body, false, properties));
    }

    private void ingestEvaluation(String promptId, JsonNode result) throws Exception {
        String key = "vbench:" + promptId + ":evaluation";
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("prompt_id", promptId);
        properties.put(
                "baseline_score", result.path("baseline").path("vbench_subset_score").asText());
        properties.put(
                "rawwise_score", result.path("rawwise").path("vbench_subset_score").asText());
        properties.put("delta_score", result.path("delta").path("vbench_subset_score").asText());
        properties.put("relative_pct", result.path("delta").path("relative_pct").asText());
        properties.put("text_content", "vbench subset delta " + result.path("delta").toString());
        axiomClient.ingest(
                new AxiomIngest(
                        EVAL_TABLE,
                        key,
                        objectMapper.writeValueAsString(result),
                        false,
                        properties));
    }

    private int annotateAutomaticFeedback(
            String baselineViewId, String rawwiseViewId, JsonNode result) {
        int count = 0;
        double baselineTemporal = score(result, "baseline", "temporal_flickering");
        if (baselineTemporal < TEMPORAL_THRESHOLD) {
            axiomClient.annotate(
                    new AxiomAnnotation(
                            baselineViewId,
                            "NEGATIVE",
                            "temporal_flickering="
                                    + baselineTemporal
                                    + " below threshold "
                                    + TEMPORAL_THRESHOLD,
                            "vbench-mps-subset",
                            0.85));
            count++;
        }
        double baselineOverall = score(result, "baseline", "overall_consistency");
        if (baselineOverall < OVERALL_THRESHOLD) {
            axiomClient.annotate(
                    new AxiomAnnotation(
                            baselineViewId,
                            "NEGATIVE",
                            "overall_consistency="
                                    + baselineOverall
                                    + " below threshold "
                                    + OVERALL_THRESHOLD,
                            "vbench-mps-subset",
                            0.80));
            count++;
        }
        double delta = result.path("delta").path("vbench_subset_score").asDouble(0.0);
        double relative = result.path("delta").path("relative_pct").asDouble(0.0);
        if (delta > 0.0) {
            axiomClient.annotate(
                    new AxiomAnnotation(
                            rawwiseViewId,
                            "POSITIVE",
                            "vbench_subset_score improved by " + delta + " (" + relative + "%)",
                            "vbench-mps-subset",
                            0.75));
            count++;
        }
        double aestheticDelta = result.path("delta").path("aesthetic_quality").asDouble(0.0);
        if (aestheticDelta < 0.0) {
            axiomClient.annotate(
                    new AxiomAnnotation(
                            rawwiseViewId,
                            "NEGATIVE",
                            "aesthetic_quality decreased by " + aestheticDelta,
                            "vbench-mps-subset",
                            0.70));
            count++;
        }
        return count;
    }

    private double score(JsonNode result, String tag, String metric) {
        return result.path(tag).path("scores").path(metric).asDouble(0.0);
    }
}
