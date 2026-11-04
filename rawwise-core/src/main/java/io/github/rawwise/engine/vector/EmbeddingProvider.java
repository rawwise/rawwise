package io.github.rawwise.engine.vector;

import java.util.List;

@FunctionalInterface
public interface EmbeddingProvider {
    float[] embed(String text);

    default float[][] embedBatch(List<String> texts) {
        float[][] results = new float[texts.size()][];
        for (int i = 0; i < texts.size(); i++) {
            results[i] = embed(texts.get(i));
        }
        return results;
    }

    default String modelIdentifier() {
        return "unknown";
    }
}
