package io.github.rawwise.engine.vector;

import java.util.List;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeterministicStubEmbeddingProvider implements EmbeddingProvider {

    private final int dimension;

    public DeterministicStubEmbeddingProvider(int dimension) {
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be positive: " + dimension);
        }
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
        float[][] result = new float[texts.size()][];
        for (int i = 0; i < texts.size(); i++) {
            result[i] = embed(texts.get(i));
        }
        return result;
    }

    @Override
    public String modelIdentifier() {
        return "stub-deterministic-l2norm";
    }
}
