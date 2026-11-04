package io.github.rawwise.engine.vector;

import java.nio.charset.StandardCharsets;

public interface MultimodalEmbeddingProvider extends EmbeddingProvider {

    float[] embedImage(byte[] imageData);

    float[] embedAudio(byte[] audioData);

    default float[] embedMultimodal(byte[] data, String modality) {
        if (modality == null) {
            throw new IllegalArgumentException("modality is required");
        }
        return switch (modality) {
            case "text" -> embed(new String(data, StandardCharsets.UTF_8));
            case "image" -> embedImage(data);
            case "audio" -> embedAudio(data);
            default -> throw new IllegalArgumentException("Unknown modality: " + modality);
        };
    }
}
