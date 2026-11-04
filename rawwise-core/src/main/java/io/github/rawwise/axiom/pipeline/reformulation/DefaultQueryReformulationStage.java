package io.github.rawwise.axiom.pipeline.reformulation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.rawwise.axiom.pipeline.graph.distributed.QueryReformulationStage;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultQueryReformulationStage implements QueryReformulationStage {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final LlmClient llmClient;
    private final ReformulationConfig config;
    private final Cache<String, List<String>> queryCache;

    public DefaultQueryReformulationStage(LlmClient llmClient, ReformulationConfig config) {
        this.llmClient = llmClient;
        this.config = config;
        this.queryCache =
                Caffeine.newBuilder()
                        .expireAfterWrite(config.cacheTtl())
                        .maximumSize(config.cacheSize())
                        .recordStats()
                        .build();
    }

    @Override
    public List<String> expand(String query, int n) {
        if (query == null || query.isBlank()) {
            return List.of();
        }
        int effectiveN = n > 0 ? n : config.expandN();
        String key = query + ":" + effectiveN;
        List<String> cached = queryCache.getIfPresent(key);
        if (cached != null) {
            return cached;
        }
        List<String> expanded = computeExpansion(query, effectiveN);
        queryCache.put(key, expanded);
        return expanded;
    }

    private List<String> computeExpansion(String query, int n) {
        String prompt =
                String.format(
                        "Generate exactly %d diverse paraphrases of this retrieval query. "
                                + "Output JSON array. Query: \"%s\"",
                        n, query);
        try {
            String response = llmClient.complete(prompt, config.modelName(), config.maxTokens());
            return parseJsonArray(response, query);
        } catch (LlmException e) {
            log.warn("LLM reformulation failed for query='{}': {}", query, e.getMessage());
            return List.of(query);
        } catch (RuntimeException e) {
            log.warn("LLM reformulation runtime failure for query='{}': {}", query, e.getMessage());
            return List.of(query);
        }
    }

    private List<String> parseJsonArray(String response, String fallbackQuery) {
        if (response == null || response.isBlank()) {
            return List.of(fallbackQuery);
        }
        // Strip markdown code blocks if LLM wrapped output in ``` or ```json
        String cleaned = response.trim();
        if (cleaned.startsWith("```")) {
            int firstNewline = cleaned.indexOf('\n');
            if (firstNewline > 0) {
                cleaned = cleaned.substring(firstNewline + 1);
            }
            int lastFence = cleaned.lastIndexOf("```");
            if (lastFence > 0) {
                cleaned = cleaned.substring(0, lastFence);
            }
            cleaned = cleaned.trim();
        }
        // Extract JSON array from possibly mixed text (find first '[' to last ']')
        int firstBracket = cleaned.indexOf('[');
        int lastBracket = cleaned.lastIndexOf(']');
        if (firstBracket >= 0 && lastBracket > firstBracket) {
            cleaned = cleaned.substring(firstBracket, lastBracket + 1);
        }
        try {
            JsonNode node = MAPPER.readTree(cleaned);
            if (!node.isArray()) {
                log.warn(
                        "LLM response is not a JSON array for query='{}': {}",
                        fallbackQuery,
                        response);
                return List.of(fallbackQuery);
            }
            List<String> result = new ArrayList<>(node.size());
            for (JsonNode item : node) {
                if (item.isTextual()) {
                    result.add(item.asText());
                }
            }
            if (result.isEmpty()) {
                return List.of(fallbackQuery);
            }
            return List.copyOf(result);
        } catch (Exception e) {
            log.warn(
                    "Failed to parse LLM JSON array for query='{}': {}",
                    fallbackQuery,
                    e.getMessage());
            return List.of(fallbackQuery);
        }
    }

    public void invalidate(String query) {
        if (query == null) return;
        queryCache.asMap().keySet().removeIf(k -> k.startsWith(query + ":"));
    }

    public void invalidateAll() {
        queryCache.invalidateAll();
    }

    public double getCacheHitRate() {
        var stats = queryCache.stats();
        long total = stats.hitCount() + stats.missCount();
        return total > 0 ? (double) stats.hitCount() / total : 0.0;
    }
}
