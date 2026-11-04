package io.github.rawwise.axiom.memory;

import java.util.Locale;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory that creates a {@link Summarization Service} from system properties.
 *
 * <p>Configuration keys (system properties):
 *
 * <ul>
 *   <li>{@code rawwise.summarization.provider.type} = {@code http} | {@code noop} (default {@code
 *       noop})
 *   <li>{@code rawwise.summarization.provider.endpoint} = Open AI Chat Completions compatible
 *       endpoint, e.g. {@code http://localhost:8002/v1/chat/completions}
 *   <li>{@code rawwise.summarization.provider.model} = model identifier, e.g. {@code
 *       Qwen/Qwen3-14B}
 *   <li>{@code rawwise.summarization.provider.api-key} = optional Bearer token (may be empty for
 *       self-hosted)
 *   <li>{@code rawwise.summarization.provider.timeout-ms} = HTTP request timeout (default 60000) -
 *       informational (Http Summarization Service currently uses a fixed 60s request timeout)
 *   <li>{@code rawwise.summarization.provider.max-retries} = retries on transient failure (default
 *       2) - informational
 *   <li>{@code rawwise.summarization.provider.l0-max-tokens} = L0 abstract budget (default 100) -
 *       informational (Consolidation Coordinator passes its own values via Summarization Request)
 *   <li>{@code rawwise.summarization.provider.l1-max-tokens} = L1 overview budget (default 2000) -
 *       informational
 * </ul>
 *
 * <p>On configuration error (e.g. type=http but endpoint or model missing), the factory degrades to
 * {@link No Op Summarization Service} and logs a warning rather than throwing. Unknown {@code type}
 * values also degrade to noop.
 */
@Slf4j
public final class SummarizationServiceFactory {

    public static final String KEY_TYPE = "rawwise.summarization.provider.type";
    public static final String KEY_ENDPOINT = "rawwise.summarization.provider.endpoint";
    public static final String KEY_MODEL = "rawwise.summarization.provider.model";
    public static final String KEY_API_KEY = "rawwise.summarization.provider.api-key";
    public static final String KEY_TIMEOUT_MS = "rawwise.summarization.provider.timeout-ms";
    public static final String KEY_MAX_RETRIES = "rawwise.summarization.provider.max-retries";
    public static final String KEY_L0_MAX_TOKENS = "rawwise.summarization.provider.l0-max-tokens";
    public static final String KEY_L1_MAX_TOKENS = "rawwise.summarization.provider.l1-max-tokens";

    public static final String DEFAULT_TYPE = "noop";
    public static final long DEFAULT_TIMEOUT_MS = 60_000L;
    public static final int DEFAULT_MAX_RETRIES = 2;
    public static final int DEFAULT_L0_MAX_TOKENS = 100;
    public static final int DEFAULT_L1_MAX_TOKENS = 2000;

    private SummarizationServiceFactory() {}

    public static SummarizationService createFromSystemProperties() {
        String type = System.getProperty(KEY_TYPE, DEFAULT_TYPE).trim().toLowerCase(Locale.ROOT);

        switch (type) {
            case "http":
                return createHttp();
            case "noop":
                log.info("SummarizationService created: type=noop");
                return new NoOpSummarizationService();
            default:
                log.warn("Unknown {}={}, falling back to noop", KEY_TYPE, type);
                return new NoOpSummarizationService();
        }
    }

    private static SummarizationService createHttp() {
        String endpoint = System.getProperty(KEY_ENDPOINT, "").trim();
        String model = System.getProperty(KEY_MODEL, "").trim();
        if (endpoint.isEmpty() || model.isEmpty()) {
            log.warn(
                    "type=http but {} or {} is empty (endpoint='{}', model='{}'); falling back to noop",
                    KEY_ENDPOINT,
                    KEY_MODEL,
                    endpoint,
                    model);
            return new NoOpSummarizationService();
        }
        String apiKey = System.getProperty(KEY_API_KEY, "");
        long timeoutMs = parseLong(KEY_TIMEOUT_MS, DEFAULT_TIMEOUT_MS);
        int maxRetries = parseInt(KEY_MAX_RETRIES, DEFAULT_MAX_RETRIES);
        int l0MaxTokens = parseInt(KEY_L0_MAX_TOKENS, DEFAULT_L0_MAX_TOKENS);
        int l1MaxTokens = parseInt(KEY_L1_MAX_TOKENS, DEFAULT_L1_MAX_TOKENS);

        try {
            log.info(
                    "SummarizationService created: type=http endpoint={} model={} timeoutMs={} maxRetries={} l0MaxTokens={} l1MaxTokens={}",
                    endpoint,
                    model,
                    timeoutMs,
                    maxRetries,
                    l0MaxTokens,
                    l1MaxTokens);
            return new HttpSummarizationService(endpoint, apiKey, model);
        } catch (Exception e) {
            log.warn(
                    "Failed to construct HttpSummarizationService (endpoint={}, model={}); falling back to noop: {}",
                    endpoint,
                    model,
                    e.getMessage());
            return new NoOpSummarizationService();
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
