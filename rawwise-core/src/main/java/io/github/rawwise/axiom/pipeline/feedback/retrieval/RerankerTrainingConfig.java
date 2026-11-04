package io.github.rawwise.axiom.pipeline.feedback.retrieval;

import java.time.Duration;
import lombok.Builder;

@Builder
public record RerankerTrainingConfig(
        boolean enabled,
        int minSamples,
        Duration minInterval,
        String modelOutputPath,
        String currentModelVersion,
        double abTrafficPercent) {

    public static RerankerTrainingConfig defaults() {
        return RerankerTrainingConfig.builder()
                .enabled(
                        Boolean.parseBoolean(
                                System.getProperty("rawwise.reranker.training.enabled", "true")))
                .minSamples(
                        Integer.parseInt(
                                System.getProperty("rawwise.reranker.training.min-samples", "50")))
                .minInterval(
                        Duration.parse(
                                System.getProperty(
                                        "rawwise.reranker.training.min-interval", "PT1M")))
                .modelOutputPath(
                        System.getProperty(
                                "rawwise.reranker.training.output-path",
                                "$WAREHOUSE/models/reranker"))
                .currentModelVersion("v0")
                .abTrafficPercent(
                        Double.parseDouble(
                                System.getProperty("rawwise.reranker.ab-traffic-percent", "1.0")))
                .build();
    }
}
