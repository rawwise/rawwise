package io.github.rawwise.benchmark.vbench.memory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class RawWisePromptMemoryClient implements PromptMemoryClient {
    private static final String EVAL_TABLE = "default.video_evaluations";
    private static final String RUN_TABLE = "default.video_generation_runs";
    private static final int SEARCH_RETRY_COUNT = 3;
    private static final long SEARCH_RETRY_DELAY_MS = 2000;

    private final String baseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public RawWisePromptMemoryClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @Override
    public List<PromptMemoryRecord> recall(String prompt, int topK) {
        List<PromptMemoryRecord> records = new ArrayList<>();

        List<PromptMemoryRecord> evalRecords =
                searchWithRetry(
                        EVAL_TABLE,
                        prompt
                                + " vbench feedback temporal_flickering overall_consistency aesthetic",
                        topK);
        records.addAll(evalRecords);

        List<PromptMemoryRecord> runRecords =
                searchWithRetry(
                        RUN_TABLE,
                        prompt + " rawwise successful video generation",
                        Math.max(1, topK / 2));
        records.addAll(runRecords);

        log.info(
                "RawWise memory recall: prompt='{}', evalHits={}, runHits={}, total={}",
                prompt.substring(0, Math.min(60, prompt.length())),
                evalRecords.size(),
                runRecords.size(),
                records.size());

        return records.stream()
                .filter(r -> r.summary() != null && !r.summary().isBlank())
                .sorted((a, b) -> Double.compare(b.score(), a.score()))
                .limit(topK)
                .toList();
    }

    private List<PromptMemoryRecord> searchWithRetry(String tableId, String query, int topK) {
        for (int attempt = 0; attempt < SEARCH_RETRY_COUNT; attempt++) {
            List<PromptMemoryRecord> results = search(tableId, query, topK);
            if (!results.isEmpty()
                    && results.stream()
                            .anyMatch(r -> r.summary() != null && !r.summary().isBlank())) {
                return results;
            }
            if (attempt < SEARCH_RETRY_COUNT - 1) {
                log.debug(
                        "Search returned no usable results (attempt {}/{}), retrying in {}ms...",
                        attempt + 1,
                        SEARCH_RETRY_COUNT,
                        SEARCH_RETRY_DELAY_MS);
                try {
                    Thread.sleep(SEARCH_RETRY_DELAY_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return results;
                }
            }
        }
        return List.of();
    }

    private List<PromptMemoryRecord> search(String tableId, String query, int topK) {
        try {
            String body =
                    objectMapper.writeValueAsString(
                            Map.of(
                                    "tableId", tableId,
                                    "query", query,
                                    "topK", topK,
                                    "snapshotId", 0));
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(baseUrl + "/api/axiom/search"))
                            .timeout(Duration.ofSeconds(30))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(body))
                            .build();
            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                log.warn(
                        "Search failed: tableId={}, status={}, body={}",
                        tableId,
                        response.statusCode(),
                        response.body().substring(0, Math.min(200, response.body().length())));
                return List.of();
            }
            JsonNode root = objectMapper.readTree(response.body());
            List<PromptMemoryRecord> records = new ArrayList<>();
            for (JsonNode result : root.path("results")) {
                String viewId = result.path("viewId").asText("");
                double score = result.path("score").asDouble(0.0);
                String summary = result.path("summary").asText("");
                if (!viewId.isEmpty()) {
                    records.add(new PromptMemoryRecord(viewId, score, summary));
                }
            }
            log.debug(
                    "Search: tableId={}, query='{}', hits={}, topScore={}",
                    tableId,
                    query.substring(0, Math.min(40, query.length())),
                    records.size(),
                    records.isEmpty() ? 0.0 : records.get(0).score());
            return records;
        } catch (IOException e) {
            log.warn("Search IOException: tableId={}, error={}", tableId, e.getMessage());
            return List.of();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return List.of();
        }
    }
}
