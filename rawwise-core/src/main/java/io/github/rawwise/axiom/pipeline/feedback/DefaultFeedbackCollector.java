package io.github.rawwise.axiom.pipeline.feedback;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.engine.storage.KnowledgeStore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * default Feedback Collector implementation. Knowledge Store feedback_fact: view_score: key
 * aggregate.
 */
@Slf4j
public class DefaultFeedbackCollector implements FeedbackCollector {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final KnowledgeStore knowledgeStore;

    public DefaultFeedbackCollector(KnowledgeStore knowledgeStore) {
        this.knowledgeStore = knowledgeStore;
    }

    @Override
    public List<CollectedFeedback> collectByScene(
            String viewId, String viewVersion, String tableId) {
        List<CollectedFeedback> result = new ArrayList<>();

        // 1. view_annotations:<view Id> annotations
        String annPrefix = "view_annotations:" + viewId;
        result.addAll(
                scanFeedback(annPrefix.getBytes(StandardCharsets.UTF_8), viewId, viewVersion));

        // 2. view_score:<view Id>
        String scorePrefix = "view_score:" + viewId;
        result.addAll(
                scanScores(scorePrefix.getBytes(StandardCharsets.UTF_8), viewId, viewVersion));

        // 3. feedback_fact: target Entity Id == view Id record
        String factPrefix = "feedback_fact:";
        result.addAll(scanFacts(factPrefix.getBytes(StandardCharsets.UTF_8), viewId, viewVersion));

        result.sort((a, b) -> Long.compare(a.timestamp(), b.timestamp()));
        return result;
    }

    private List<CollectedFeedback> scanFeedback(byte[] prefix, String viewId, String viewVersion) {
        List<CollectedFeedback> result = new ArrayList<>();
        try {
            knowledgeStore
                    .scanAll(prefix)
                    .forEach(
                            kv -> {
                                try {
                                    JsonNode node = MAPPER.readTree(kv.value());
                                    String valueStr = node.path("value").asText("0");
                                    double annValue;
                                    try {
                                        annValue = Double.parseDouble(valueStr);
                                    } catch (NumberFormatException ex) {
                                        annValue = 0.0;
                                    }
                                    result.add(
                                            new CollectedFeedback(
                                                    viewId,
                                                    node.path("viewVersion").asText(viewVersion),
                                                    extractMetrics(node),
                                                    annValue,
                                                    node.path("type").asText("preference"),
                                                    annValue,
                                                    node.path("source").asText("HUMAN"),
                                                    node.path("timestamp")
                                                            .asLong(System.currentTimeMillis())));
                                } catch (IOException e) {
                                    log.warn(
                                            "Failed to parse feedback annotation: {}",
                                            e.getMessage());
                                }
                            });
        } catch (IOException e) {
            log.warn("Failed to scan feedback for viewId={}: {}", viewId, e.getMessage());
        }
        return result;
    }

    private List<CollectedFeedback> scanScores(byte[] prefix, String viewId, String viewVersion) {
        List<CollectedFeedback> result = new ArrayList<>();
        try {
            knowledgeStore
                    .scanAll(prefix)
                    .forEach(
                            kv -> {
                                try {
                                    JsonNode node = MAPPER.readTree(kv.value());
                                    result.add(
                                            new CollectedFeedback(
                                                    viewId,
                                                    viewVersion,
                                                    extractMetrics(node),
                                                    node.path("score").asDouble(0.0),
                                                    "quality_score",
                                                    node.path("score").asDouble(0.0),
                                                    node.path("source").asText("SELF"),
                                                    node.path("timestamp")
                                                            .asLong(System.currentTimeMillis())));
                                } catch (IOException e) {
                                    log.warn("Failed to parse score: {}", e.getMessage());
                                }
                            });
        } catch (IOException e) {
            log.warn("Failed to scan scores for viewId={}: {}", viewId, e.getMessage());
        }
        return result;
    }

    private List<CollectedFeedback> scanFacts(byte[] prefix, String viewId, String viewVersion) {
        List<CollectedFeedback> result = new ArrayList<>();
        try {
            knowledgeStore
                    .scanAll(prefix)
                    .forEach(
                            kv -> {
                                try {
                                    JsonNode node = MAPPER.readTree(kv.value());
                                    String targetId = node.path("targetEntityId").asText("");
                                    if (!targetId.equals(viewId)) return;
                                    result.add(
                                            new CollectedFeedback(
                                                    viewId,
                                                    viewVersion,
                                                    extractMetrics(node),
                                                    node.path("score").asDouble(0.0),
                                                    node.path("signalType")
                                                            .asText("quality_score")
                                                            .toLowerCase(),
                                                    node.path("score").asDouble(0.0),
                                                    node.path("source").asText("SELF"),
                                                    node.path("timestamp")
                                                            .asLong(System.currentTimeMillis())));
                                } catch (IOException e) {
                                    log.warn("Failed to parse fact: {}", e.getMessage());
                                }
                            });
        } catch (IOException e) {
            log.warn("Failed to scan facts for viewId={}: {}", viewId, e.getMessage());
        }
        return result;
    }

    private Map<String, Double> extractMetrics(JsonNode node) {
        Map<String, Double> metrics = new HashMap<>();
        JsonNode metricsNode = node.path("metrics");
        if (metricsNode.isObject()) {
            metricsNode
                    .fields()
                    .forEachRemaining(
                            entry -> metrics.put(entry.getKey(), entry.getValue().asDouble(0.0)));
        }
        if (metrics.isEmpty() && node.has("score")) {
            metrics.put("overall", node.get("score").asDouble(0.0));
        }
        return metrics;
    }
}
