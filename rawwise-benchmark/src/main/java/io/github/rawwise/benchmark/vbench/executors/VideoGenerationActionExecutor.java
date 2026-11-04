package io.github.rawwise.benchmark.vbench.executors;

import io.github.rawwise.axiom.pipeline.cognition.ActionExecutor;
import io.github.rawwise.axiom.pipeline.cognition.ActionPlan;
import io.github.rawwise.axiom.pipeline.cognition.CognitionEngine;
import io.github.rawwise.axiom.pipeline.cognition.ExecutionContext;
import io.github.rawwise.benchmark.vbench.memory.PromptEnhancement;
import io.github.rawwise.benchmark.vbench.memory.PromptEnhancementPlanner;
import io.github.rawwise.model.record.Action;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * Video generation ActionExecutor: consumes RawWise generic ActionExecutor SPI +
 * ActionExecutorRegistry.
 *
 * <p>Registration path: ActionExecutorRegistry.register(Action.ActionType.CORRECT, new
 * VideoGenerationActionExecutor(client))
 *
 * <p>Trigger path: FeedbackLoopCloser.onAnnotation(viewId, violation) →
 * CognitionEngine.deriveAction(CORRECT, params) → ActionExecutorRegistry.resolve(CORRECT) →
 * VideoGenerationActionExecutor.execute() → CogVideoXClient.generate(request) → regenerated video
 * frames
 *
 * <p>Input Action.params convention (constructed by ConditionConstructionPipeline): - prompt:
 * String (text condition) - reference_embedding: float[] (visual reference vector) -
 * drift_description: String (drift description) - frame_range: String (frame range, e.g. "37" or
 * "30-40")
 */
@Slf4j
public class VideoGenerationActionExecutor implements ActionExecutor {

    private final String executorId;
    private final CogVideoXClient client;
    private final PromptEnhancementPlanner promptPlanner;

    public VideoGenerationActionExecutor() {
        this(new CogVideoXClient(), null);
    }

    public VideoGenerationActionExecutor(CogVideoXClient client) {
        this(client, null);
    }

    public VideoGenerationActionExecutor(
            CogVideoXClient client, PromptEnhancementPlanner promptPlanner) {
        this.executorId = "video-gen-executor-" + UUID.randomUUID().toString().substring(0, 8);
        this.client = client;
        this.promptPlanner = promptPlanner;
    }

    @Override
    public String executorId() {
        return executorId;
    }

    @Override
    public Set<Action.ActionType> supportedActions() {
        return Set.of(Action.ActionType.CORRECT);
    }

    @Override
    public CognitionEngine.ActionResult execute(Action action, ExecutionContext context) {
        if (action == null) {
            return failureResult("unknown", "action is null");
        }
        if (!supportedActions().contains(action.actionType())) {
            return failureResult(
                    action.actionId(), "Unsupported action type: " + action.actionType());
        }

        Map<String, Object> params = mergeParams(action, context);
        String prompt = extractString(params, "prompt", "text_condition");
        if (prompt == null || prompt.isBlank()) {
            return failureResult(action.actionId(), "prompt/text_condition is required");
        }

        PromptEnhancement promptEnhancement = enhancePrompt(prompt);
        String generationPrompt = promptEnhancement.enhancedPrompt();

        float[] referenceEmbedding = extractFloatArray(params, "reference_embedding");
        String driftDescription = extractString(params, "drift_description");
        String frameRange = extractString(params, "frame_range");
        if (frameRange == null) {
            frameRange = "0-end";
        }

        try {
            CogVideoXClient.GenerationRequest request =
                    new CogVideoXClient.GenerationRequest(
                            generationPrompt,
                            referenceEmbedding,
                            driftDescription,
                            frameRange,
                            Map.of());
            CogVideoXClient.GenerationResponse response = client.generate(request);

            Map<String, Object> artifacts = new HashMap<>();
            artifacts.put("video_bytes_length", response.videoBytes().length);
            artifacts.put("generation_latency_ms", response.generationLatencyMs());
            artifacts.put("model_version", response.modelVersion());
            artifacts.put("frame_range", frameRange);
            artifacts.put("rawise_prompt_enhanced", promptEnhancement.enhanced());
            artifacts.put("rawise_prompt_constraints", promptEnhancement.constraints());
            artifacts.put("rawise_prompt_source_memory_ids", promptEnhancement.sourceMemoryIds());
            artifacts.putAll(response.metadata());

            log.info(
                    "Video regenerated: actionId={}, frameRange={}, latencyMs={}",
                    action.actionId(),
                    frameRange,
                    response.generationLatencyMs());

            return new CognitionEngine.ActionResult(
                    action.actionId(),
                    true,
                    "Regenerated video for frame range " + frameRange,
                    artifacts,
                    System.currentTimeMillis());
        } catch (Exception e) {
            log.error("Video generation failed for action {}", action.actionId(), e);
            return failureResult(action.actionId(), e.getMessage());
        }
    }

    @Override
    public CognitionEngine.PlanResult executePlan(ActionPlan plan, ExecutionContext context) {
        if (plan == null || plan.actions().isEmpty()) {
            return new CognitionEngine.PlanResult(
                    "empty", false, List.of(), "Empty plan", System.currentTimeMillis());
        }

        List<CognitionEngine.ActionResult> stepResults = new ArrayList<>();
        boolean allSuccess = true;

        for (Action action : plan.actions()) {
            CognitionEngine.ActionResult result = execute(action, context);
            stepResults.add(result);
            if (!result.success()) {
                allSuccess = false;
            }
        }

        return new CognitionEngine.PlanResult(
                plan.planId(),
                allSuccess,
                stepResults,
                allSuccess
                        ? "All " + stepResults.size() + " video generations succeeded"
                        : "Some video generations failed",
                System.currentTimeMillis());
    }

    private PromptEnhancement enhancePrompt(String prompt) {
        if (promptPlanner == null) {
            return new PromptEnhancement(prompt, prompt, List.of(), List.of());
        }
        return promptPlanner.enhance(prompt);
    }

    private Map<String, Object> mergeParams(Action action, ExecutionContext context) {
        Map<String, Object> merged = new HashMap<>();
        if (context != null && context.params() != null) {
            merged.putAll(context.params());
        }
        return merged;
    }

    private String extractString(Map<String, Object> params, String... keys) {
        for (String key : keys) {
            Object val = params.get(key);
            if (val != null) {
                return val.toString();
            }
        }
        return null;
    }

    private float[] extractFloatArray(Map<String, Object> params, String key) {
        Object val = params.get(key);
        if (val == null) {
            return null;
        }
        if (val instanceof float[] arr) {
            return arr;
        }
        if (val instanceof double[] darr) {
            float[] result = new float[darr.length];
            for (int i = 0; i < darr.length; i++) {
                result[i] = (float) darr[i];
            }
            return result;
        }
        return null;
    }

    private CognitionEngine.ActionResult failureResult(String actionId, String reason) {
        return new CognitionEngine.ActionResult(
                actionId, false, reason, Map.of("error", reason), System.currentTimeMillis());
    }
}
