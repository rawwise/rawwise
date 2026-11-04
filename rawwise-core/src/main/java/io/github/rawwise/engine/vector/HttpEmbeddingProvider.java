package io.github.rawwise.engine.vector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpEmbeddingProvider implements EmbeddingProvider, AutoCloseable {

    /**
     * Embedding HTTP protocol format.
     *
     * <ul>
     *   <li>{@link #OPENAI_STANDARD} — OpenAI / SiliconFlow / DashScope: request {@code {"input":
     *       "text" | ["t1","t2"], "model": ...}}, response {@code {"data": [{"embedding": [...],
     *       "index": 0}, ...]}}
     *   <li>{@link #VOLCENGINE_MULTIMODAL} — Volcengine Ark doubao-embedding-vision: request {@code
     *       {"input": [{"type":"text","text":"..."}], "model": ...}} (single item), response {@code
     *       {"data": {"embedding": [...]}}} (data is object not array). does not support native
     *       batch; this class degrades to per-item calls in this mode.
     * </ul>
     */
    public enum EmbeddingFormat {
        OPENAI_STANDARD,
        VOLCENGINE_MULTIMODAL
    }

    private final HttpClient httpClient;
    private final String endpoint;
    private final String model;
    private final String apiKey;
    private final int dimension;
    private final int maxRetries;
    private final int batchSize;
    private final CircuitBreaker circuitBreaker;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Duration requestTimeout;
    private final Cache<String, float[]> embeddingCache;
    private final EmbeddingFormat format;

    public HttpEmbeddingProvider(String endpoint, String model, int dimension) {
        this(
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build(),
                endpoint,
                model,
                null,
                dimension,
                3,
                32,
                new CircuitBreaker(50, 30_000L),
                30_000);
    }

    public HttpEmbeddingProvider(
            HttpClient httpClient,
            String endpoint,
            String model,
            String apiKey,
            int dimension,
            int maxRetries,
            int batchSize,
            CircuitBreaker circuitBreaker,
            long requestTimeoutMs) {
        this(
                httpClient,
                endpoint,
                model,
                apiKey,
                dimension,
                maxRetries,
                batchSize,
                circuitBreaker,
                requestTimeoutMs,
                EmbeddingFormat.OPENAI_STANDARD);
    }

    public HttpEmbeddingProvider(
            HttpClient httpClient,
            String endpoint,
            String model,
            String apiKey,
            int dimension,
            int maxRetries,
            int batchSize,
            CircuitBreaker circuitBreaker,
            long requestTimeoutMs,
            EmbeddingFormat format) {
        this.httpClient = httpClient;
        this.endpoint = endpoint;
        this.model = model;
        this.apiKey = apiKey;
        this.dimension = dimension;
        this.maxRetries = maxRetries;
        this.batchSize = batchSize;
        this.circuitBreaker = circuitBreaker;
        this.requestTimeout = Duration.ofMillis(requestTimeoutMs);
        this.format = format == null ? EmbeddingFormat.OPENAI_STANDARD : format;
        this.embeddingCache =
                Caffeine.newBuilder()
                        .expireAfterWrite(1, TimeUnit.HOURS)
                        .maximumSize(10_000)
                        .recordStats()
                        .build();
    }

    private String cacheKey(String text) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest((text + "|" + model).getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            return text + "|" + model;
        }
    }

    public double getCacheHitRate() {
        var stats = embeddingCache.stats();
        long total = stats.hitCount() + stats.missCount();
        return total > 0 ? (double) stats.hitCount() / total : 0.0;
    }

    @Override
    public float[] embed(String text) {
        String key = cacheKey(text);
        float[] cached = embeddingCache.getIfPresent(key);
        if (cached != null) return cached;

        float[] result = withRetry(() -> doEmbed(text));
        embeddingCache.put(key, result);
        return result;
    }

    @Override
    public float[][] embedBatch(List<String> texts) {
        float[][] result = new float[texts.size()][];
        List<Integer> missed = new ArrayList<>();
        for (int i = 0; i < texts.size(); i++) {
            String key = cacheKey(texts.get(i));
            float[] cached = embeddingCache.getIfPresent(key);
            if (cached != null) {
                result[i] = cached;
            } else {
                missed.add(i);
            }
        }
        if (missed.isEmpty()) return result;

        int offset = 0;
        while (offset < missed.size()) {
            int end = Math.min(offset + batchSize, missed.size());
            List<String> batch = new ArrayList<>(end - offset);
            List<Integer> batchIndices = new ArrayList<>(end - offset);
            for (int i = offset; i < end; i++) {
                int idx = missed.get(i);
                batch.add(texts.get(idx));
                batchIndices.add(idx);
            }
            float[][] batchResult;
            if (format == EmbeddingFormat.VOLCENGINE_MULTIMODAL) {
                // VOLCENGINE_MULTIMODAL does not support native batch: per-item calls, preserves
                // cache and retry
                batchResult = new float[batch.size()][];
                for (int i = 0; i < batch.size(); i++) {
                    final String item = batch.get(i);
                    batchResult[i] = withRetry(() -> doEmbed(item));
                }
            } else {
                batchResult = withRetry(() -> doEmbedBatch(batch));
            }
            for (int i = 0; i < batchResult.length; i++) {
                int idx = batchIndices.get(i);
                result[idx] = batchResult[i];
                String key = cacheKey(texts.get(idx));
                embeddingCache.put(key, batchResult[i]);
            }
            offset = end;
        }
        return result;
    }

    private <T> T withRetry(Supplier<T> action) {
        circuitBreaker.beforeRequest();
        int attempt = 0;
        while (true) {
            try {
                T result = action.get();
                circuitBreaker.onSuccess();
                return result;
            } catch (Exception e) {
                if (attempt < maxRetries && isRetryable(e)) {
                    // Seconds-level backoff (1s, 2s, 4s...) so rate-limit (429) /
                    // 5xx windows have time to recover before a retry is counted as
                    // a circuit-breaker failure.
                    long backoff =
                            (1L << attempt) * 1000 + ThreadLocalRandom.current().nextLong(250);
                    try {
                        Thread.sleep(backoff);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new EmbeddingUnavailableException(
                                "Interrupted during retry backoff", ie);
                    }
                    attempt++;
                } else {
                    circuitBreaker.onFailure();
                    throw e;
                }
            }
        }
    }

    @Override
    public String modelIdentifier() {
        return model;
    }

    CircuitBreaker.State getCircuitBreakerState() {
        return circuitBreaker.getState();
    }

    @Override
    public void close() {
        try {
            httpClient.close();
        } catch (Exception e) {
            log.warn("Failed to close HttpClient", e);
        }
    }

    private HttpRequest.Builder requestBuilder() {
        var builder =
                HttpRequest.newBuilder()
                        .uri(URI.create(endpoint))
                        .header("Content-Type", "application/json")
                        .timeout(requestTimeout);
        if (apiKey != null && !apiKey.isBlank()) {
            builder.header("Authorization", "Bearer " + apiKey);
        }
        return builder;
    }

    private float[] doEmbed(String text) {
        try {
            String requestBody = objectMapper.writeValueAsString(buildSingleRequestBody(text));
            HttpRequest request =
                    requestBuilder().POST(HttpRequest.BodyPublishers.ofString(requestBody)).build();
            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                String body = response.body();
                String snippet =
                        body == null ? "" : (body.length() > 500 ? body.substring(0, 500) : body);
                log.error(
                        "Embedding API {} for endpoint={} model={} format={} request={} response={}",
                        response.statusCode(),
                        endpoint,
                        model,
                        format,
                        requestBody,
                        snippet);
                throw new EmbeddingApiException(
                        response.statusCode(),
                        "Embedding API returned " + response.statusCode() + " body=" + snippet);
            }
            return parseSingleEmbedding(response.body());
        } catch (EmbeddingApiException e) {
            throw new EmbeddingUnavailableException(e.getMessage(), e);
        } catch (EmbeddingUnavailableException e) {
            throw e;
        } catch (Exception e) {
            throw new EmbeddingUnavailableException("Embedding API call failed", e);
        }
    }

    private Map<String, Object> buildSingleRequestBody(String text) {
        if (format == EmbeddingFormat.VOLCENGINE_MULTIMODAL) {
            // Volcengine Ark multimodal: input must be [{type, text}] array
            Map<String, Object> textItem = Map.of("type", "text", "text", text);
            return Map.of("input", List.of(textItem), "model", model);
        }
        return Map.of("input", text, "model", model);
    }

    private float[][] doEmbedBatch(List<String> texts) {
        try {
            List<Object> inputList = new ArrayList<>(texts);
            String requestBody =
                    objectMapper.writeValueAsString(Map.of("input", inputList, "model", model));
            HttpRequest request =
                    requestBuilder().POST(HttpRequest.BodyPublishers.ofString(requestBody)).build();
            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new EmbeddingApiException(
                        response.statusCode(), "Embedding API returned " + response.statusCode());
            }
            return parseBatchEmbeddings(response.body(), texts.size());
        } catch (EmbeddingApiException e) {
            throw new EmbeddingUnavailableException(e.getMessage(), e);
        } catch (EmbeddingUnavailableException e) {
            throw e;
        } catch (Exception e) {
            throw new EmbeddingUnavailableException("Embedding batch API call failed", e);
        }
    }

    private float[] parseSingleEmbedding(String responseBody) {
        try {
            JsonNode root = objectMapper.readTree(responseBody);
            JsonNode embeddingNode;
            if (format == EmbeddingFormat.VOLCENGINE_MULTIMODAL) {
                // Volcengine multimodal: data is object, embedding is directly on data.embedding
                embeddingNode = root.at("/data/embedding");
            } else {
                embeddingNode = root.at("/data/0/embedding");
            }
            return validateAndExtractVector(embeddingNode);
        } catch (EmbeddingUnavailableException e) {
            throw e;
        } catch (Exception e) {
            throw new EmbeddingUnavailableException("Failed to parse embedding response", e);
        }
    }

    private float[][] parseBatchEmbeddings(String responseBody, int expectedCount) {
        try {
            JsonNode root = objectMapper.readTree(responseBody);
            JsonNode dataArray = root.at("/data");
            if (dataArray.size() < expectedCount) {
                throw new EmbeddingUnavailableException(
                        "Expected " + expectedCount + " embeddings, got " + dataArray.size());
            }
            float[][] results = new float[expectedCount][];
            for (int i = 0; i < expectedCount; i++) {
                JsonNode embeddingNode = dataArray.get(i).at("/embedding");
                results[i] = validateAndExtractVector(embeddingNode);
            }
            return results;
        } catch (EmbeddingUnavailableException e) {
            throw e;
        } catch (Exception e) {
            throw new EmbeddingUnavailableException("Failed to parse batch embedding response", e);
        }
    }

    private float[] validateAndExtractVector(JsonNode embeddingNode) {
        if (embeddingNode.isMissingNode() || embeddingNode.size() == 0) {
            throw new EmbeddingUnavailableException("Embedding node missing or empty in response");
        }
        if (embeddingNode.size() != dimension) {
            throw new EmbeddingUnavailableException(
                    "Dimension mismatch: expected " + dimension + ", got " + embeddingNode.size());
        }
        float[] vector = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            vector[i] = (float) embeddingNode.get(i).asDouble();
        }
        return vector;
    }

    private boolean isRetryable(Exception e) {
        if (e instanceof EmbeddingUnavailableException euve) {
            Throwable cause = euve.getCause();
            if (cause instanceof EmbeddingApiException apiEx) {
                int statusCode = apiEx.statusCode();
                return statusCode >= 500 || statusCode == 429;
            }
            return cause instanceof SocketTimeoutException
                    || cause instanceof ConnectException
                    || cause instanceof HttpTimeoutException;
        }
        return false;
    }

    static class EmbeddingApiException extends Exception {
        private final int statusCode;

        EmbeddingApiException(int statusCode, String message) {
            super(message);
            this.statusCode = statusCode;
        }

        int statusCode() {
            return statusCode;
        }
    }
}
