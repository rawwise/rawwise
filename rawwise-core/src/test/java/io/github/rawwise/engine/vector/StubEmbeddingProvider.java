package io.github.rawwise.engine.vector;

import java.util.List;
import java.util.Random;

public class StubEmbeddingProvider implements EmbeddingProvider {

    private final int dimension;

    public StubEmbeddingProvider(int dimension) {
        if (dimension <= 0)
            throw new IllegalArgumentException("dimension must be positive: " + dimension);
        this.dimension = dimension;
    }

    @Override
    public float[] embed(String text) {
        Random localRandom = new Random(text.hashCode() + 31L * dimension);
        float[] vector = new float[dimension];
        double sumSq = 0;
        for (int i = 0; i < dimension; i++) {
            vector[i] = (float) (localRandom.nextGaussian() * 0.1);
            sumSq += vector[i] * vector[i];
        }
        float norm = (float) Math.sqrt(sumSq);
        if (norm > 0) {
            for (int i = 0; i < dimension; i++) {
                vector[i] /= norm;
            }
        }
        return vector;
    }

    @Override
    public float[][] embedBatch(List<String> texts) {
        return texts.stream().map(this::embed).toArray(float[][]::new);
    }

    @Override
    public String modelIdentifier() {
        return "stub-gaussian-l2norm";
    }
}
