package io.github.rawwise.axiom.memory;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.axiom.memory.exception.AxiomClientException;
import io.github.rawwise.axiom.memory.exception.AxiomException;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpAxiomClient implements AxiomClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final int DEFAULT_TOP_K = 1000;
    private static final long DEFAULT_TIMEOUT_SECONDS = 30;

    private final HttpClient httpClient;
    private final String baseUrl;
    private final String authToken;

    public HttpAxiomClient(String baseUrl) {
        this(baseUrl, null);
    }

    public HttpAxiomClient(String baseUrl, String authToken) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.authToken = authToken;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @Override
    public String ingest(String tableId, String key, String body, Map<String, String> properties) {
        Map<String, Object> requestMap = new LinkedHashMap<>();
        requestMap.put("tableId", tableId);
        requestMap.put("key", key);
        requestMap.put("body", body != null ? body : "");
        requestMap.put("immutable", false);
        requestMap.put("properties", properties != null ? properties : Map.of());
        try {
            String json = MAPPER.writeValueAsString(requestMap);
            HttpResponse<String> response = postJson("/api/axiom/ingest", json);
            ensureSuccess(response, "Axiom ingest", "tableId=" + tableId + " key=" + key);
            String recordKey = MAPPER.readTree(response.body()).path("recordKey").asText(key);
            log.debug(
                    "Axiom ingest success: tableId={} key={} recordKey={}",
                    tableId,
                    key,
                    recordKey);
            return recordKey;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Axiom ingest interrupted: tableId={} key={}", tableId, key);
            return key;
        } catch (AxiomException e) {
            throw e;
        } catch (Exception e) {
            log.warn(
                    "Axiom ingest failed (graceful fallback): tableId={} key={} {}",
                    tableId,
                    key,
                    e.getMessage());
            return key;
        }
    }

    @Override
    public List<Map<String, Object>> search(
            String tableId, String query, int topK, String namespace) {
        return search(tableId, query, topK, namespace, Map.of());
    }

    @Override
    public List<Map<String, Object>> search(
            String tableId, String query, int topK, String namespace, Map<String, String> filter) {
        Map<String, Object> requestMap = new LinkedHashMap<>();
        requestMap.put("tableId", tableId);
        requestMap.put("query", query != null ? query : "");
        requestMap.put("topK", topK > 0 ? topK : 10);
        requestMap.put("snapshotId", 0L);
        requestMap.put("filter", filter != null ? filter : Map.of());
        try {
            String json = MAPPER.writeValueAsString(requestMap);
            HttpResponse<String> response = postJson("/api/axiom/search", json);
            ensureSuccess(response, "Axiom search", "tableId=" + tableId + " query=" + query);
            List<Map<String, Object>> results = extractResults(response.body(), namespace);
            log.debug(
                    "Axiom search success: tableId={} query={} returned {} results",
                    tableId,
                    query,
                    results.size());
            return results;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Axiom search interrupted: tableId={} query={}", tableId, query);
            return List.of();
        } catch (AxiomException e) {
            throw e;
        } catch (Exception e) {
            log.warn(
                    "Axiom search failed (graceful fallback): tableId={} query={} {}",
                    tableId,
                    query,
                    e.getMessage());
            return List.of();
        }
    }

    @Override
    public Optional<Map<String, Object>> getEntry(String tableId, String key) {
        Map<String, Object> requestMap = new LinkedHashMap<>();
        requestMap.put("tableId", tableId);
        requestMap.put("entryKeys", List.of(key));
        try {
            String json = MAPPER.writeValueAsString(requestMap);
            HttpResponse<String> response = postJson("/api/axiom/fetch-details", json);
            ensureSuccess(response, "Axiom getEntry", "tableId=" + tableId + " key=" + key);
            var resultsNode = MAPPER.readTree(response.body()).path("results");
            if (!resultsNode.isArray()) {
                log.debug("Axiom getEntry no results array: tableId={} key={}", tableId, key);
                return Optional.empty();
            }
            for (var node : resultsNode) {
                if (!node.path("key").asText().equals(key)) {
                    continue;
                }
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("tableId", tableId);
                entry.put("key", key);
                entry.put("body", node.path("body").asText(node.path("rawText").asText("")));
                entry.put("properties", jsonToMap(node.path("properties")));
                log.debug("Axiom getEntry success: tableId={} key={}", tableId, key);
                return Optional.of(entry);
            }
            log.debug("Axiom getEntry not found in results: tableId={} key={}", tableId, key);
            return Optional.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Axiom getEntry interrupted: tableId={} key={}", tableId, key);
            return Optional.empty();
        } catch (AxiomException e) {
            throw e;
        } catch (Exception e) {
            log.warn(
                    "Axiom getEntry failed (graceful fallback): tableId={} key={} {}",
                    tableId,
                    key,
                    e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public List<Map<String, Object>> scan(String tableId, String namespace) {
        Map<String, String> filter = new LinkedHashMap<>();
        if (namespace != null && !namespace.isEmpty()) {
            filter.put("agent_id", namespace);
        }
        List<Map<String, Object>> searchResults =
                search(tableId, "", DEFAULT_TOP_K, namespace, filter);
        if (searchResults.size() >= DEFAULT_TOP_K) {
            log.warn(
                    "Axiom scan may be truncated: reached topK={} (tableId={} namespace={}). "
                            + "Search API has no pagination; entries beyond the cap are not listed.",
                    DEFAULT_TOP_K,
                    tableId,
                    namespace);
        }
        List<Map<String, Object>> entries = new ArrayList<>(searchResults.size());
        for (Map<String, Object> result : searchResults) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("tableId", tableId);
            entry.put("key", result.getOrDefault("entryKey", result.getOrDefault("viewId", "")));
            Object body = result.get("body");
            entry.put("body", body != null ? body : result.getOrDefault("summary", ""));
            Map<String, String> props = new LinkedHashMap<>();
            Object resultProps = result.get("properties");
            if (resultProps instanceof Map<?, ?> rp) {
                rp.forEach((k, v) -> props.put(String.valueOf(k), String.valueOf(v)));
            }
            if (props.isEmpty() && namespace != null && !namespace.isEmpty()) {
                props.put("agent_id", namespace);
            }
            entry.put("properties", props);
            entries.add(entry);
        }
        log.debug(
                "Axiom scan success: tableId={} namespace={} returned {} entries",
                tableId,
                namespace,
                entries.size());
        return entries;
    }

    private List<Map<String, Object>> extractResults(String responseBody, String namespace)
            throws IOException {
        var resultsNode = MAPPER.readTree(responseBody).path("results");
        if (!resultsNode.isArray()) {
            return List.of();
        }
        List<Map<String, Object>> results = new ArrayList<>();
        for (var node : resultsNode) {
            Map<String, Object> result = new LinkedHashMap<>();
            String entryKey = node.path("entryKey").asText(node.path("viewId").asText(""));
            result.put("entryKey", entryKey);
            result.put("viewId", node.path("viewId").asText(entryKey));
            result.put("score", node.path("score").asDouble(0.0));
            result.put("summary", node.path("summary").asText(""));
            Map<String, String> props = jsonToMap(node.path("properties"));
            if (props.isEmpty() && namespace != null && !namespace.isEmpty()) {
                props.put("agent_id", namespace);
            }
            result.put("properties", props);
            if (node.has("body")) {
                result.put("body", node.path("body").asText(""));
            }
            results.add(result);
        }
        return results;
    }

    private static Map<String, String> jsonToMap(com.fasterxml.jackson.databind.JsonNode node) {
        Map<String, String> map = new LinkedHashMap<>();
        if (node == null || !node.isObject()) {
            return map;
        }
        node.fields().forEachRemaining(f -> map.put(f.getKey(), f.getValue().asText()));
        return map;
    }

    /**
     * Validates a non-2xx response. Deterministic client errors (4xx) are surfaced as {@link
     * AxiomClientException} so callers can distinguish "request is fundamentally wrong" (e.g. table
     * not registered) from transient server failures; 5xx degrades to graceful fallback.
     */
    private void ensureSuccess(HttpResponse<String> response, String operation, String detail) {
        int code = response.statusCode();
        if (code >= 200 && code < 300) {
            return;
        }
        if (code >= 400 && code < 500) {
            throw new AxiomClientException(
                    code,
                    operation
                            + " failed (HTTP "
                            + code
                            + "): "
                            + detail
                            + " body="
                            + response.body());
        }
        log.warn(
                "{} failed (HTTP {}, graceful fallback): {} body={}",
                operation,
                code,
                detail,
                response.body());
    }

    private HttpResponse<String> postJson(String path, String body)
            throws IOException, InterruptedException {
        HttpRequest.Builder builder =
                HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + path))
                        .timeout(Duration.ofSeconds(DEFAULT_TIMEOUT_SECONDS))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body));
        if (authToken != null && !authToken.isEmpty()) {
            builder.header("Authorization", "Bearer " + authToken);
        }
        return httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }
}
