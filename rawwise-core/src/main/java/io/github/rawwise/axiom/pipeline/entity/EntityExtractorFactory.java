package io.github.rawwise.axiom.pipeline.entity;

import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class EntityExtractorFactory {

    private EntityExtractorFactory() {}

    public static HybridEntityExtractor create(EntityExtractionConfig config) {
        EntityExtractionConfig cfg = config == null ? EntityExtractionConfig.defaults() : config;
        if (!cfg.enabled()) {
            log.info("EntityExtraction disabled, using NoOpEntityProvider");
            return new HybridEntityExtractor(new NoOpEntityProvider(), null, false);
        }
        Duration timeout = cfg.timeout() == null ? Duration.ofSeconds(5) : cfg.timeout();
        EntityProvider primary =
                new LlmEntityProvider(cfg.llmEndpoint(), cfg.llmApiKey(), timeout, 2);
        EntityProvider fallback = new SpaCyNerProvider(cfg.spacySidecarPort(), timeout);
        log.info(
                "EntityExtractor created primary={} fallback={} hybrid={}",
                primary.name(),
                fallback.name(),
                cfg.hybridEnabled());
        return new HybridEntityExtractor(primary, fallback, cfg.hybridEnabled());
    }

    public static HybridEntityExtractor createFromSystemProperties() {
        boolean enabled =
                Boolean.parseBoolean(
                        System.getProperty("rawwise.entity.extraction.enabled", "false"));
        boolean hybrid =
                Boolean.parseBoolean(
                        System.getProperty("rawwise.entity.extraction.hybrid.enabled", "true"));
        String llm = System.getProperty("rawwise.entity.extraction.llm.provider", "stub://");
        String llmApiKey = System.getProperty("rawwise.entity.extraction.llm.api-key");
        int port =
                Integer.parseInt(
                        System.getProperty(
                                "rawwise.entity.extraction.spacy.sidecar.port", "17091"));
        EntityExtractionConfig cfg =
                EntityExtractionConfig.builder()
                        .enabled(enabled)
                        .hybridEnabled(hybrid)
                        .llmEndpoint(llm)
                        .llmApiKey(llmApiKey)
                        .spacySidecarPort(port)
                        .timeout(Duration.ofSeconds(5))
                        .asyncThreads(8)
                        .asyncQueueSize(10000)
                        .build();
        return create(cfg);
    }
}
