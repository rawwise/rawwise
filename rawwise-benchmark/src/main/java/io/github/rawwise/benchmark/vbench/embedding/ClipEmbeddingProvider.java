package io.github.rawwise.benchmark.vbench.embedding;

import io.github.rawwise.engine.vector.MultimodalEmbeddingProvider;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import lombok.extern.slf4j.Slf4j;

/**
 * CLIP multimodal embedding provider: consumes RawWise generic MultimodalEmbeddingProvider SPI.
 *
 * <p>Production deployment has two modes: 1. Local inference: via ONNX Runtime / DJL calling
 * clip-vit-large-patch14 model (requires ~1.7GB model file) 2. HTTP call: call local or remote CLIP
 * inference service
 *
 * <p>This implementation provides deterministic stub: generates stable embedding vectors based on
 * content hash. useat: - unit testing and E2E testing (no external model dependency) - placeholder
 * implementation (replaced by subclass in production)
 *
 * <p>Vector dimension: 768 (consistent with CLIP ViT-L/14) Production replacement path: public
 * class OnnxClipEmbeddingProvider extends ClipEmbeddingProvider {... } public class
 * HttpClipEmbeddingProvider extends ClipEmbeddingProvider {... }
 */
@Slf4j
public class ClipEmbeddingProvider implements MultimodalEmbeddingProvider {

    public static final int CLIP_EMBEDDING_DIM = 768;

    private final String modelId;

    public ClipEmbeddingProvider() {
        this("clip-vit-large-patch14-stub");
    }

    public ClipEmbeddingProvider(String modelId) {
        this.modelId = modelId;
    }

    @Override
    public float[] embed(String text) {
        if (text == null || text.isEmpty()) {
            return new float[CLIP_EMBEDDING_DIM];
        }
        return generateDeterministicEmbedding(text.getBytes());
    }

    @Override
    public float[] embedImage(byte[] imageData) {
        if (imageData == null || imageData.length == 0) {
            return new float[CLIP_EMBEDDING_DIM];
        }
        return generateDeterministicEmbedding(imageData);
    }

    @Override
    public float[] embedAudio(byte[] audioData) {
        if (audioData == null || audioData.length == 0) {
            return new float[CLIP_EMBEDDING_DIM];
        }
        return generateDeterministicEmbedding(audioData);
    }

    @Override
    public String modelIdentifier() {
        return modelId;
    }

    /**
     * Generate 768-dimensional deterministic vector based on SHA-256 hash. samesameinnercontent →
     * samesamevector;notsameinnercontent → notsamevector. L2 normalization ensures correct cosine
     * similarity computation.
     */
    protected float[] generateDeterministicEmbedding(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            float[] result = new float[CLIP_EMBEDDING_DIM];
            int offset = 0;

            byte[] currentSeed = data;
            while (offset < CLIP_EMBEDDING_DIM) {
                byte[] hash = md.digest(currentSeed);
                ByteBuffer buffer = ByteBuffer.wrap(hash);
                while (buffer.remaining() >= 4 && offset < CLIP_EMBEDDING_DIM) {
                    int intVal = buffer.getInt();
                    result[offset++] = (intVal / (float) Integer.MAX_VALUE);
                }
                currentSeed = hash;
            }

            return l2Normalize(result);
        } catch (NoSuchAlgorithmException e) {
            log.error("SHA-256 not available", e);
            return new float[CLIP_EMBEDDING_DIM];
        }
    }

    private float[] l2Normalize(float[] vec) {
        double sumSq = 0.0;
        for (float v : vec) {
            sumSq += v * v;
        }
        double norm = Math.sqrt(sumSq);
        if (norm == 0.0) {
            return vec;
        }
        float[] normalized = new float[vec.length];
        for (int i = 0; i < vec.length; i++) {
            normalized[i] = (float) (vec[i] / norm);
        }
        return normalized;
    }
}
