package io.github.rawwise.axiom.pipeline.entity;

import java.time.Duration;
import lombok.Builder;

@Builder
public record EntityExtractionConfig(
        boolean enabled,
        boolean hybridEnabled,
        String llmEndpoint,
        String llmApiKey,
        int spacySidecarPort,
        Duration timeout,
        int asyncThreads,
        int asyncQueueSize) {

    public static EntityExtractionConfig defaults() {
        return EntityExtractionConfig.builder()
                .enabled(false)
                .hybridEnabled(true)
                .llmEndpoint("stub://")
                .llmApiKey(null)
                .spacySidecarPort(17091)
                .timeout(Duration.ofSeconds(5))
                .asyncThreads(8)
                .asyncQueueSize(10000)
                .build();
    }
}
