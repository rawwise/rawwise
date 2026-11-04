package io.github.rawwise.engine.vector;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory that creates an EmbeddingProvider from system properties.
 *
 * <p>Configuration keys (system properties):
 *
 * <ul>
 *   <li>{@code rawwise.embedding.provider.type} = {@code http} | {@code stub} | {@code noop} |
 *       {@code onnx} (default {@code noop})
 *   <li>{@code rawwise.embedding.provider.endpoint} = OpenAI-compatible HTTP endpoint, e.g. {@code
 *       http://localhost:8001/v1/embeddings}
 *   <li>{@code rawwise.embedding.provider.model} = model identifier, e.g. {@code
 *       Qwen/Qwen3-Embedding-0.6B}
 *   <li>{@code rawwise.embedding.provider.dimension} = embedding dimension (default 1024)
 *   <li>{@code rawwise.embedding.provider.api-key} = optional Bearer token (empty for self-hosted)
 *   <li>{@code rawwise.embedding.provider.timeout-ms} = HTTP request timeout (default 30000)
 *   <li>{@code rawwise.embedding.provider.max-retries} = retries on transient failure (default 3)
 *   <li>{@code rawwise.embedding.provider.batch-size} = batch size for embedBatch (default 32)
 *   <li>{@code rawwise.embedding.provider.onnx.model-path} = ONNX model file path (required for
 *       type=onnx)
 *   <li>{@code rawwise.embedding.provider.onnx.execution-provider} = ONNX execution provider
 *       (default CPU)
 * </ul>
 *
 * <p>On configuration error (e.g. missing endpoint for type=http), the factory degrades to {@link
 * NoOpEmbeddingProvider} and logs a warning rather than throwing.
 */
@Slf4j
public final class EmbeddingProviderFactory {

    public static final String KEY_TYPE = "rawwise.embedding.provider.type";
    public static final String KEY_ENDPOINT = "rawwise.embedding.provider.endpoint";
    public static final String KEY_MODEL = "rawwise.embedding.provider.model";
    public static final String KEY_DIMENSION = "rawwise.embedding.provider.dimension";
    public static final String KEY_API_KEY = "rawwise.embedding.provider.api-key";
    public static final String KEY_TIMEOUT_MS = "rawwise.embedding.provider.timeout-ms";
    public static final String KEY_MAX_RETRIES = "rawwise.embedding.provider.max-retries";
    public static final String KEY_BATCH_SIZE = "rawwise.embedding.provider.batch-size";
    public static final String KEY_FORMAT = "rawwise.embedding.provider.format";
    public static final String KEY_ONNX_MODEL_PATH = "rawwise.embedding.provider.onnx.model-path";
    public static final String KEY_ONNX_EXECUTION_PROVIDER =
            "rawwise.embedding.provider.onnx.execution-provider";

    public static final String DEFAULT_TYPE = "noop";
    public static final int DEFAULT_DIMENSION = 1024;
    public static final long DEFAULT_TIMEOUT_MS = 30_000L;
    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final int DEFAULT_BATCH_SIZE = 32;
    public static final String DEFAULT_FORMAT = "openai_standard";

    private EmbeddingProviderFactory() {}

    public static EmbeddingProvider createFromSystemProperties() {
        String type = System.getProperty(KEY_TYPE, DEFAULT_TYPE).trim().toLowerCase(Locale.ROOT);
        int dimension = parseInt(KEY_DIMENSION, DEFAULT_DIMENSION);

        if (dimension <= 0) {
            log.warn(
                    "Invalid {}={}, falling back to default {}",
                    KEY_DIMENSION,
                    dimension,
                    DEFAULT_DIMENSION);
            dimension = DEFAULT_DIMENSION;
        }

        switch (type) {
            case "http":
                return createHttp(dimension);
            case "stub":
                log.info(
                        "EmbeddingProvider created: type=stub dimension={} model=stub-deterministic-l2norm",
                        dimension);
                return new DeterministicStubEmbeddingProvider(dimension);
            case "noop":
                log.info(
                        "EmbeddingProvider created: type=noop dimension={} model=noop-zero-vector",
                        dimension);
                return new NoOpEmbeddingProvider(dimension);
            case "onnx":
                return createOnnx(dimension);
            default:
                log.warn(
                        "Unknown {}={}, falling back to noop (dimension={})",
                        KEY_TYPE,
                        type,
                        dimension);
                return new NoOpEmbeddingProvider(dimension);
        }
    }

    private static EmbeddingProvider createHttp(int dimension) {
        String endpoint = System.getProperty(KEY_ENDPOINT, "").trim();
        String model = System.getProperty(KEY_MODEL, "").trim();
        if (endpoint.isEmpty() || model.isEmpty()) {
            log.warn(
                    "type=http but {} or {} is empty (endpoint='{}', model='{}'); falling back to noop dimension={}",
                    KEY_ENDPOINT,
                    KEY_MODEL,
                    endpoint,
                    model,
                    dimension);
            return new NoOpEmbeddingProvider(dimension);
        }
        String apiKey = System.getProperty(KEY_API_KEY, "");
        long timeoutMs = parseLong(KEY_TIMEOUT_MS, DEFAULT_TIMEOUT_MS);
        int maxRetries = parseInt(KEY_MAX_RETRIES, DEFAULT_MAX_RETRIES);
        int batchSize = parseInt(KEY_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        HttpEmbeddingProvider.EmbeddingFormat format = parseFormat(KEY_FORMAT, DEFAULT_FORMAT);

        try {
            HttpClient httpClient =
                    HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            CircuitBreaker circuitBreaker = new CircuitBreaker(5, 30_000L);
            log.info(
                    "EmbeddingProvider created: type=http endpoint={} model={} dimension={} timeoutMs={} maxRetries={} batchSize={} format={}",
                    endpoint,
                    model,
                    dimension,
                    timeoutMs,
                    maxRetries,
                    batchSize,
                    format);
            return new HttpEmbeddingProvider(
                    httpClient,
                    endpoint,
                    model,
                    apiKey.isBlank() ? null : apiKey,
                    dimension,
                    maxRetries,
                    batchSize,
                    circuitBreaker,
                    timeoutMs,
                    format);
        } catch (Exception e) {
            log.warn(
                    "Failed to construct HttpEmbeddingProvider (endpoint={}, model={}); falling back to noop: {}",
                    endpoint,
                    model,
                    e.getMessage());
            return new NoOpEmbeddingProvider(dimension);
        }
    }

    private static EmbeddingProvider createOnnx(int dimension) {
        String modelPath = System.getProperty(KEY_ONNX_MODEL_PATH, "").trim();
        String executionProvider = System.getProperty(KEY_ONNX_EXECUTION_PROVIDER, "CPU").trim();
        if (modelPath.isEmpty() || !java.nio.file.Files.exists(java.nio.file.Path.of(modelPath))) {
            log.warn(
                    "type=onnx but {} is empty or file not found (modelPath='{}'); falling back to NoOpOnnx dimension={}",
                    KEY_ONNX_MODEL_PATH,
                    modelPath,
                    dimension);
            return new io.github.rawwise.engine.vector.onnx.NoOpOnnxEmbeddingProvider(dimension);
        }
        try {
            io.github.rawwise.engine.vector.onnx.OnnxEmbeddingConfig onnxConfig =
                    io.github.rawwise.engine.vector.onnx.OnnxEmbeddingConfig.builder()
                            .enabled(true)
                            .modelPath(modelPath)
                            .dimension(dimension)
                            .executionProvider(executionProvider)
                            .build();
            io.github.rawwise.engine.vector.onnx.OnnxEmbeddingProvider provider =
                    new io.github.rawwise.engine.vector.onnx.OnnxEmbeddingProvider(onnxConfig);
            log.info(
                    "EmbeddingProvider created: type=onnx modelPath={} dimension={} executionProvider={}",
                    modelPath,
                    dimension,
                    executionProvider);
            return provider;
        } catch (Exception e) {
            log.warn(
                    "Failed to construct OnnxEmbeddingProvider (modelPath={}); falling back to NoOpOnnx: {}",
                    modelPath,
                    e.getMessage());
            return new io.github.rawwise.engine.vector.onnx.NoOpOnnxEmbeddingProvider(dimension);
        }
    }

    private static HttpEmbeddingProvider.EmbeddingFormat parseFormat(
            String key, String defaultValue) {
        String raw = System.getProperty(key, defaultValue).trim().toLowerCase(Locale.ROOT);
        switch (raw) {
            case "openai_standard":
            case "openai":
            case "":
                return HttpEmbeddingProvider.EmbeddingFormat.OPENAI_STANDARD;
            case "volcengine_multimodal":
            case "volcengine":
            case "doubao_multimodal":
                return HttpEmbeddingProvider.EmbeddingFormat.VOLCENGINE_MULTIMODAL;
            default:
                log.warn("Unknown {}={}, falling back to {}", key, raw, defaultValue);
                return HttpEmbeddingProvider.EmbeddingFormat.OPENAI_STANDARD;
        }
    }

    private static int parseInt(String key, int defaultValue) {
        String raw = System.getProperty(key);
        if (raw == null || raw.isBlank()) return defaultValue;
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            log.warn("Invalid integer for {}={}, using default {}", key, raw, defaultValue);
            return defaultValue;
        }
    }

    private static long parseLong(String key, long defaultValue) {
        String raw = System.getProperty(key);
        if (raw == null || raw.isBlank()) return defaultValue;
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            log.warn("Invalid long for {}={}, using default {}", key, raw, defaultValue);
            return defaultValue;
        }
    }
}
