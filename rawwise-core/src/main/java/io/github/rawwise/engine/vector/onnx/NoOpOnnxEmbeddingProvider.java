package io.github.rawwise.engine.vector.onnx;

import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.EmbeddingUnavailableException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NoOpOnnxEmbeddingProvider implements EmbeddingProvider {

    private final int dimension;

    public NoOpOnnxEmbeddingProvider(int dimension) {
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be positive: " + dimension);
        }
        this.dimension = dimension;
    }

    @Override
    public float[] embed(String text) {
        throw new EmbeddingUnavailableException(
                "NoOpOnnxEmbeddingProvider: not configured (sentinel for disabled path)");
    }

    @Override
    public float[][] embedBatch(List<String> texts) {
        throw new EmbeddingUnavailableException(
                "NoOpOnnxEmbeddingProvider: not configured (sentinel for disabled path)");
    }

    public int dimension() {
        return dimension;
    }

    @Override
    public String modelIdentifier() {
        return "noop-onnx";
    }
}
