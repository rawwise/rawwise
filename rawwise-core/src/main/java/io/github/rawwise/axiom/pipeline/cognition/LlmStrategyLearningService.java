package io.github.rawwise.axiom.pipeline.cognition;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.rawwise.axiom.pipeline.feedback.CollectedFeedback;
import io.github.rawwise.model.record.StrategyAction;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * LLM strategy learning.
 *
 * <p>via LLM,statusstrategy, Strategy Action write M_t..
 *
 * <p>generation,generation,text.
 */
@Slf4j
public class LlmStrategyLearningService implements StrategyLearningService {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String endpoint;
    private final String apiKey;
    private final String model;
    private final HttpClient httpClient;

    public LlmStrategyLearningService(String endpoint, String apiKey, String model) {
        this.endpoint = endpoint;
        this.apiKey = apiKey;
        this.model = model;
        this.httpClient =
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(10))
                        .proxy(java.net.ProxySelector.of(null))
                        .build();
    }

    @Override
    public Optional<StrategyAction> learn(StrategyLearningContext context) {
        if (!isAvailable()) {
            return Optional.empty();
        }

        try {
            String prompt = buildLearningPrompt(context);
            String llmResponse = callLlm(prompt);
            JsonNode json = parseJsonResponse(llmResponse);

            if (json == null) {
                log.warn("LLM returned unparseable JSON response for strategy learning");
                return Optional.empty();
            }

            String sceneCondition = json.path("sceneCondition").asText("").trim();
            String strategyContent = json.path("strategyContent").asText("").trim();
            String expectedEffect = json.path("expectedEffect").asText("").trim();

            Map<String, Double> conditionMetrics = new HashMap<>();
            JsonNode metricsNode = json.path("conditionMetrics");
            if (metricsNode.isObject()) {
                metricsNode
                        .fields()
                        .forEachRemaining(
                                entry -> {
                                    conditionMetrics.put(
                                            entry.getKey(), entry.getValue().asDouble());
                                });
            }

            if (sceneCondition.isEmpty()
                    || strategyContent.isEmpty()
                    || expectedEffect.isEmpty()
                    || conditionMetrics.isEmpty()) {
                log.warn(
                        "LLM strategy learning returned incomplete fields: sceneCondition={}, strategyContent={}, expectedEffect={}, metrics={}",
                        sceneCondition.isEmpty() ? "MISSING" : "OK",
                        strategyContent.isEmpty() ? "MISSING" : "OK",
                        expectedEffect.isEmpty() ? "MISSING" : "OK",
                        conditionMetrics.isEmpty() ? "MISSING" : "OK");
                return Optional.empty();
            }

            List<String> sourceViewIds =
                    context.feedbackHistory().stream()
                            .map(CollectedFeedback::viewId)
                            .filter(Objects::nonNull)
                            .distinct()
                            .collect(Collectors.toList());

            List<String> sourceFeedbackIds =
                    context.feedbackHistory().stream()
                            .map(f -> "feedback:" + Integer.toUnsignedString(f.hashCode()))
                            .distinct()
                            .collect(Collectors.toList());

            String strategyId = "strategy:" + UUID.randomUUID().toString();
            long now = System.currentTimeMillis();

            StrategyAction strategy =
                    new StrategyAction(
                            strategyId,
                            sceneCondition,
                            conditionMetrics,
                            strategyContent,
                            expectedEffect,
                            List.of(),
                            0.0,
                            sourceViewIds,
                            sourceFeedbackIds,
                            now,
                            now);

            log.info(
                    "LLM strategy learning: learned strategy={}, scene={}, sourceViews={}, sourceFeedbacks={}",
                    strategyId,
                    sceneCondition,
                    sourceViewIds.size(),
                    sourceFeedbackIds.size());

            return Optional.of(strategy);
        } catch (Exception e) {
            log.warn("LLM strategy learning failed: {}", e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public boolean isAvailable() {
        return endpoint != null
                && !endpoint.isEmpty()
                && apiKey != null
                && !apiKey.isEmpty()
                && model != null
                && !model.isEmpty();
    }

    /**
     * strategy learning prompt.
     *
     * <p>.LLM: 1. (,score,) 2. status 3. strategy()
     */
    private String buildLearningPrompt(StrategyLearningContext context) {
        StringBuilder sb = new StringBuilder();
        sb.append(
                "You are a strategy learning engine. Your job is to learn effective strategies from feedback.\n\n");

        sb.append("=== FEEDBACK HISTORY ===\n");
        if (context.feedbackHistory() == null || context.feedbackHistory().isEmpty()) {
            sb.append("(no feedback available)\n");
        } else {
            for (int i = 0; i < context.feedbackHistory().size(); i++) {
                CollectedFeedback fb = context.feedbackHistory().get(i);
                sb.append("  ").append(i + 1).append(". ");
                sb.append("viewId=").append(fb.viewId()).append(", ");
                sb.append("metrics=").append(fb.metrics()).append(", ");
                sb.append("overallScore=").append(fb.overallScore()).append(", ");
                sb.append("feedbackType=").append(fb.feedbackType()).append(", ");
                sb.append("annotationValue=").append(fb.annotationValue()).append(", ");
                sb.append("feedbackSource=").append(fb.feedbackSource());
                sb.append("\n");
            }
        }
        sb.append("\n");

        sb.append("=== CURRENT SYSTEM STATE ===\n");
        if (context.systemStateSummary() == null || context.systemStateSummary().isBlank()) {
            sb.append("(no state summary available)\n");
        } else {
            sb.append(context.systemStateSummary()).append("\n");
        }
        sb.append("\n");

        sb.append("=== EXISTING SIMILAR STRATEGIES ===\n");
        if (context.existingStrategies() == null || context.existingStrategies().isEmpty()) {
            sb.append("(no existing strategies)\n");
        } else {
            for (int i = 0; i < context.existingStrategies().size(); i++) {
                StrategyAction existing = context.existingStrategies().get(i);
                sb.append("  ").append(i + 1).append(". ");
                sb.append("sceneCondition=")
                        .append(truncate(existing.sceneCondition(), 200))
                        .append(", ");
                sb.append("strategyContent=")
                        .append(truncate(existing.strategyContent(), 200))
                        .append(", ");
                sb.append("expectedEffect=").append(truncate(existing.expectedEffect(), 100));
                sb.append("\n");
            }
        }
        sb.append("\n");

        sb.append("=== YOUR TASK ===\n");
        sb.append("Based on the feedback history, identify:\n");
        sb.append("1. What scene does this strategy apply to? (sceneCondition)\n");
        sb.append("2. What are the key metrics describing this scene? (conditionMetrics)\n");
        sb.append("3. What strategy would likely improve the outcome? (strategyContent)\n");
        sb.append("4. What effect is expected? (expectedEffect)\n");
        sb.append("\n");
        sb.append(
                "IMPORTANT: Pay special attention to dimensions marked DEGRADED in the system state.\n");
        sb.append(
                "Your strategy should target the weakest dimensions and avoid repeating changes that degraded performance.\n");
        sb.append("If prior meta-patterns are available, use them to guide your strategy.\n");
        sb.append("\n");
        sb.append("Output as JSON:\n");
        sb.append("{\n");
        sb.append("  \"sceneCondition\": \"...\",\n");
        sb.append("  \"conditionMetrics\": {\"metric_name\": value},\n");
        sb.append("  \"strategyContent\": \"...\",\n");
        sb.append("  \"expectedEffect\": \"...\"\n");
        sb.append("}\n");

        return sb.toString();
    }

    private String callLlm(String prompt) throws IOException, InterruptedException {
        ObjectNode requestBody = MAPPER.createObjectNode();
        requestBody.put("model", model);

        ArrayNode messages = requestBody.putArray("messages");
        messages.add(MAPPER.createObjectNode().put("role", "user").put("content", prompt));

        HttpRequest httpRequest =
                HttpRequest.newBuilder()
                        .uri(URI.create(endpoint))
                        .header("Content-Type", "application/json")
                        .header("Authorization", "Bearer " + apiKey)
                        .POST(
                                HttpRequest.BodyPublishers.ofString(
                                        MAPPER.writeValueAsString(requestBody)))
                        .timeout(Duration.ofSeconds(30))
                        .build();

        HttpResponse<String> response =
                httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException(
                    "LLM API returned status " + response.statusCode() + ": " + response.body());
        }

        JsonNode root = MAPPER.readTree(response.body());
        return root.path("choices").path(0).path("message").path("content").asText("").trim();
    }

    private JsonNode parseJsonResponse(String llmResponse) {
        if (llmResponse == null || llmResponse.isBlank()) return null;
        String result = llmResponse.trim();
        if (result.startsWith("```")) {
            result = result.replaceAll("^```\\w*\\n?", "").replaceAll("\\n?```$", "");
        }
        try {
            return MAPPER.readTree(result.trim());
        } catch (Exception e) {
            log.warn("Failed to parse LLM JSON response: {}", e.getMessage());
            return null;
        }
    }

    private String truncate(String s, int maxLen) {
        if (s == null) return "";
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
    }
}
