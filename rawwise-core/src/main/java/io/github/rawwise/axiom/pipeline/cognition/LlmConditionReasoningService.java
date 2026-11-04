package io.github.rawwise.axiom.pipeline.cognition;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * LLM condition.
 *
 * <p>via LLM retrieval,,information, conditiontext..
 *
 * <p>generation,generation,text.
 */
@Slf4j
public class LlmConditionReasoningService implements ConditionReasoningService {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String endpoint;
    private final String apiKey;
    private final String model;
    private final HttpClient httpClient;

    public LlmConditionReasoningService(String endpoint, String apiKey, String model) {
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
    public ConditionReasoningResult reason(ConditionReasoningContext context) {
        if (!isAvailable()) {
            return new ConditionReasoningResult(
                    context.originalCondition(), context.originalCondition(), List.of(), "noop");
        }

        try {
            String prompt = buildReasoningPrompt(context);
            String llmResponse = callLlm(prompt);
            String improvedCondition = extractCondition(llmResponse);

            if (improvedCondition == null || improvedCondition.isBlank()) {
                log.warn("LLM returned empty condition, falling back to original");
                return new ConditionReasoningResult(
                        context.originalCondition(),
                        context.originalCondition(),
                        List.of(),
                        "llm-empty-fallback");
            }

            List<String> factIds =
                    context.retrievedFacts().stream()
                            .limit(5)
                            .map(f -> "fact:" + Math.abs(f.hashCode()))
                            .collect(Collectors.toList());

            log.info(
                    "LLM condition reasoning: original={} chars, improved={} chars, facts={}",
                    context.originalCondition().length(),
                    improvedCondition.length(),
                    factIds.size());

            return new ConditionReasoningResult(
                    context.originalCondition(), improvedCondition, factIds, "llm");
        } catch (Exception e) {
            log.warn(
                    "LLM condition reasoning failed, falling back to original: {}", e.getMessage());
            return new ConditionReasoningResult(
                    context.originalCondition(),
                    context.originalCondition(),
                    List.of(),
                    "llm-error-fallback");
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
     * prompt.
     *
     * <p>.LLM: 1. 2. (+ information) 3. 4. 5.,
     */
    private String buildReasoningPrompt(ConditionReasoningContext context) {
        StringBuilder sb = new StringBuilder();
        sb.append(
                "You are a condition improvement engine. Your task is to improve a request/condition based on past results and memory.\n\n");

        // 1. condition
        sb.append("=== ORIGINAL REQUEST ===\n");
        sb.append(context.originalCondition()).append("\n\n");

        // 2. ()
        if (!context.evaluationScores().isEmpty()) {
            sb.append("=== CURRENT EVALUATION SCORES ===\n");
            for (Map.Entry<String, Double> entry : context.evaluationScores().entrySet()) {
                sb.append("  ")
                        .append(entry.getKey())
                        .append(": ")
                        .append(entry.getValue())
                        .append("\n");
            }
            sb.append("\n");
        }

        // 3. information()
        if (!context.violationMetadata().isEmpty()) {
            sb.append("=== IDENTIFIED ISSUES (below threshold) ===\n");
            for (Map.Entry<String, Double> entry : context.violationMetadata().entrySet()) {
                sb.append("  ")
                        .append(entry.getKey())
                        .append(" = ")
                        .append(entry.getValue())
                        .append(" (needs improvement)\n");
            }
            sb.append("\n");
        }

        // 4. retrieval
        if (!context.retrievedFacts().isEmpty()) {
            sb.append("=== RELEVANT FACTS FROM MEMORY ===\n");
            for (int i = 0; i < context.retrievedFacts().size(); i++) {
                sb.append("  ")
                        .append(i + 1)
                        .append(". ")
                        .append(context.retrievedFacts().get(i))
                        .append("\n");
            }
            sb.append("\n");
        }

        // 5. digest
        if (!context.entitySummary().isEmpty()) {
            sb.append("=== ENTITY CONTEXT ===\n");
            for (Map.Entry<String, String> entry : context.entitySummary().entrySet()) {
                sb.append("  ")
                        .append(entry.getKey())
                        .append(": ")
                        .append(entry.getValue())
                        .append("\n");
            }
            sb.append("\n");
        }

        // 6.
        if (!context.previousAttempts().isEmpty()) {
            sb.append("=== PREVIOUS ATTEMPTS ===\n");
            for (ConditionReasoningContext.PreviousAttempt attempt : context.previousAttempts()) {
                sb.append("  Condition: ").append(truncate(attempt.condition(), 200)).append("\n");
                sb.append("  Outcome: ").append(attempt.outcome()).append("\n");
                if (!attempt.scores().isEmpty()) {
                    sb.append("  Scores: ").append(attempt.scores()).append("\n");
                }
                sb.append("\n");
            }
        }

        // 7.
        sb.append("=== YOUR TASK ===\n");
        sb.append(
                "Based on the above information, generate an improved version of the ORIGINAL REQUEST.\n");
        sb.append("Rules:\n");
        sb.append("1. Preserve the original intent and core content.\n");
        sb.append("2. Add specific, actionable guidance to address the IDENTIFIED ISSUES.\n");
        sb.append("3. Use RELEVANT FACTS FROM MEMORY to inform your improvements.\n");
        sb.append(
                "4. Avoid repeating strategies that led to poor outcomes in PREVIOUS ATTEMPTS.\n");
        sb.append(
                "5. Be concise — the output should be the improved request text only, no explanation.\n");
        sb.append(
                "6. Do not add meta-commentary like 'Here is the improved version:' — just output the text.\n");

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

    private String extractCondition(String llmResponse) {
        if (llmResponse == null || llmResponse.isBlank()) return null;
        // markdown blockmarker
        String result = llmResponse.trim();
        if (result.startsWith("```")) {
            result = result.replaceAll("^```\\w*\\n?", "").replaceAll("\\n?```$", "");
        }
        return result.trim();
    }

    private String truncate(String s, int maxLen) {
        if (s == null) return "";
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
    }
}
