package io.github.rawwise.axiom.memory.semantic;

import io.github.rawwise.axiom.memory.HttpSummarizationService;
import io.github.rawwise.axiom.memory.NoOpSummarizationService;
import io.github.rawwise.axiom.memory.SummarizationService;
import io.github.rawwise.axiom.memory.SummarizationServiceFactory;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory that creates a {@link Knowledge Extractor} from system properties.
 *
 * <p>Configuration keys (system properties):
 *
 * <ul>
 *   <li>{@code rawwise.knowledge-extractor.type} = {@code llm} | {@code noop} (default {@code
 *       noop})
 *   <li>{@code rawwise.knowledge-extractor.endpoint} = Open AI Chat Completions compatible
 *       endpoint. If empty, falls back to {@link Summarization Service Factory#KEY_ENDPOINT}.
 *   <li>{@code rawwise.knowledge-extractor.model} = model identifier, e.g. {@code deepseek-chat}.
 *       If empty, falls back to {@link Summarization Service Factory#KEY_MODEL}.
 *   <li>{@code rawwise.knowledge-extractor.api-key} = optional Bearer token. If empty, falls back
 *       to {@link Summarization Service Factory#KEY_API_KEY}.
 * </ul>
 *
 * <p>On configuration error (e.g. type=llm but endpoint or model missing), the factory degrades to
 * {@link No Op Knowledge Extractor} and logs a warning rather than throwing. Unknown {@code type}
 * values also degrade to noop.
 */
@Slf4j
public final class KnowledgeExtractorFactory {

    public static final String KEY_TYPE = "rawwise.knowledge-extractor.type";
    public static final String KEY_ENDPOINT = "rawwise.knowledge-extractor.endpoint";
    public static final String KEY_MODEL = "rawwise.knowledge-extractor.model";
    public static final String KEY_API_KEY = "rawwise.knowledge-extractor.api-key";

    public static final String DEFAULT_TYPE = "noop";

    private KnowledgeExtractorFactory() {}

    /**
     * Create a {@link Knowledge Extractor} from system properties.
     *
     * @param graph Engine required for Llm Knowledge Extractor (write-back nodes/edges)
     * @param summarization Service used as the LLM client when {@code type=llm}; if null and
     *     extractor-specific endpoint+model are missing, falls back to noop
     */
    public static KnowledgeExtractor createFromSystemProperties(
            GraphEngine graphEngine, SummarizationService summarizationService) {
        String type = System.getProperty(KEY_TYPE, DEFAULT_TYPE).trim().toLowerCase(Locale.ROOT);

        switch (type) {
            case "llm":
                return createLlm(graphEngine, summarizationService);
            case "noop":
                log.info("KnowledgeExtractor created: type=noop");
                return new NoOpKnowledgeExtractor();
            default:
                log.warn("Unknown {}={}, falling back to noop", KEY_TYPE, type);
                return new NoOpKnowledgeExtractor();
        }
    }

    private static KnowledgeExtractor createLlm(
            GraphEngine graphEngine, SummarizationService summarizationService) {
        if (graphEngine == null) {
            log.warn("type=llm but graphEngine is null; falling back to noop");
            return new NoOpKnowledgeExtractor();
        }

        String endpoint = System.getProperty(KEY_ENDPOINT, "").trim();
        String model = System.getProperty(KEY_MODEL, "").trim();
        String apiKey = System.getProperty(KEY_API_KEY, "").trim();

        if (endpoint.isEmpty()) {
            endpoint = System.getProperty(SummarizationServiceFactory.KEY_ENDPOINT, "").trim();
        }
        if (model.isEmpty()) {
            model = System.getProperty(SummarizationServiceFactory.KEY_MODEL, "").trim();
        }
        if (apiKey.isEmpty()) {
            apiKey = System.getProperty(SummarizationServiceFactory.KEY_API_KEY, "");
        }

        SummarizationService llmClient = summarizationService;

        boolean dedicatedConfigured = !endpoint.isEmpty() && !model.isEmpty();
        if (dedicatedConfigured) {
            try {
                llmClient = new HttpSummarizationService(endpoint, apiKey, model);
                log.info(
                        "KnowledgeExtractor created: type=llm endpoint={} model={} (dedicated client)",
                        endpoint,
                        model);
            } catch (Exception e) {
                log.warn(
                        "Failed to construct dedicated HttpSummarizationService for KnowledgeExtractor (endpoint={}, model={}); falling back to shared summarizationService: {}",
                        endpoint,
                        model,
                        e.getMessage());
            }
        }

        if (llmClient == null || llmClient instanceof NoOpSummarizationService) {
            log.warn(
                    "type=llm but no usable LLM client (dedicated endpoint+model empty and shared summarizationService is null/noop); falling back to noop");
            return new NoOpKnowledgeExtractor();
        }

        if (!dedicatedConfigured) {
            log.info("KnowledgeExtractor created: type=llm using shared summarizationService");
        }
        return new LlmKnowledgeExtractor(graphEngine, llmClient);
    }
}
