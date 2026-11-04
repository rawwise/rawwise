package io.github.rawwise.axiom.pipeline.reformulation;

import java.time.Duration;
import lombok.Builder;

@Builder
public record ReformulationConfig(
        boolean enabled,
        int expandN,
        Duration cacheTtl,
        int cacheSize,
        String modelName,
        int maxTokens) {

    public static ReformulationConfig defaults() {
        return ReformulationConfig.builder()
                .enabled(true)
                .expandN(4)
                .cacheTtl(Duration.ofMinutes(60))
                .cacheSize(10_000)
                .modelName("gpt-4o-mini")
                .maxTokens(200)
                .build();
    }
}
