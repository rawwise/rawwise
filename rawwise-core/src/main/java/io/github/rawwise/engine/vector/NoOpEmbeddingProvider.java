package io.github.rawwise.engine.vector;

import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NoOpEmbeddingProvider implements EmbeddingProvider {

    private final int dimension;

    public NoOpEmbeddingProvider(int dimension) {
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be positive: " + dimension);
        }
        this.dimension = dimension;
    }

    @Override
    public float[] embed(String text) {
        return new float[dimension];
    }

    @Override
    public float[][] embedBatch(List<String> texts) {
        float[][] result = new float[texts.size()][];
        for (int i = 0; i < texts.size(); i++) {
            result[i] = new float[dimension];
        }
        return result;
    }

    @Override
    public String modelIdentifier() {
        return "noop-zero-vector";
    }
}
