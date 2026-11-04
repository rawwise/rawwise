package io.github.rawwise.engine.vector.onnx;

import java.time.Duration;
import lombok.Builder;

@Builder
public record OnnxEmbeddingConfig(
        boolean enabled,
        String modelPath,
        int dimension,
        String executionProvider,
        int batchSize,
        Duration warmupTimeout,
        String tokenizerPath,
        int maxSequenceLength) {

    public static OnnxEmbeddingConfig defaults() {
        return OnnxEmbeddingConfig.builder()
                .enabled(false)
                .modelPath("")
                .dimension(1024)
                .executionProvider("CPU")
                .batchSize(8)
                .warmupTimeout(Duration.ofSeconds(30))
                .tokenizerPath("")
                .maxSequenceLength(512)
                .build();
    }
}
