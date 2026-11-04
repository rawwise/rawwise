package io.github.rawwise.engine.vector;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class MultimodalEmbeddingProviderTest {

    @Test
    void embedImageReturnsEmbedding() {
        MultimodalEmbeddingProvider provider = new TestEmbeddingProvider();
        float[] result = provider.embedImage(new byte[] {1, 2, 3});
        assertNotNull(result);
        assertEquals(4, result.length);
        assertArrayEquals(new float[] {0.1f, 0.2f, 0.3f, 0.4f}, result, 0.0001f);
    }

    @Test
    void embedAudioReturnsEmbedding() {
        MultimodalEmbeddingProvider provider = new TestEmbeddingProvider();
        float[] result = provider.embedAudio(new byte[] {4, 5, 6});
        assertNotNull(result);
        assertArrayEquals(new float[] {0.5f, 0.6f, 0.7f, 0.8f}, result, 0.0001f);
    }

    @Test
    void embedMultimodalRoutesByModality() {
        MultimodalEmbeddingProvider provider = new TestEmbeddingProvider();
        float[] imageResult = provider.embedMultimodal(new byte[] {1}, "image");
        float[] audioResult = provider.embedMultimodal(new byte[] {2}, "audio");
        float[] textResult = provider.embedMultimodal("hello".getBytes(), "text");
        assertNotNull(imageResult);
        assertNotNull(audioResult);
        assertNotNull(textResult);
        assertEquals(4, textResult.length);
    }

    @Test
    void embedMultimodalThrowsOnUnknownModality() {
        MultimodalEmbeddingProvider provider = new TestEmbeddingProvider();
        assertThrows(
                IllegalArgumentException.class,
                () -> provider.embedMultimodal(new byte[] {1}, "unknown"));
    }

    @Test
    void embedMultimodalThrowsOnNullModality() {
        MultimodalEmbeddingProvider provider = new TestEmbeddingProvider();
        assertThrows(
                IllegalArgumentException.class,
                () -> provider.embedMultimodal(new byte[] {1}, null));
    }

    @Test
    void backwardCompatibleWithEmbeddingProvider() {
        EmbeddingProvider provider = new TestEmbeddingProvider();
        float[] result = provider.embed("hello");
        assertNotNull(result);
        assertEquals(4, result.length);
    }

    @Test
    void embedBatchInheritedFromBase() {
        MultimodalEmbeddingProvider provider = new TestEmbeddingProvider();
        float[][] results = provider.embedBatch(List.of("a", "b"));
        assertEquals(2, results.length);
        assertEquals(4, results[0].length);
    }

    static class TestEmbeddingProvider implements MultimodalEmbeddingProvider {
        @Override
        public float[] embed(String text) {
            return new float[] {1f, 2f, 3f, 4f};
        }

        @Override
        public float[] embedImage(byte[] imageData) {
            return new float[] {0.1f, 0.2f, 0.3f, 0.4f};
        }

        @Override
        public float[] embedAudio(byte[] audioData) {
            return new float[] {0.5f, 0.6f, 0.7f, 0.8f};
        }

        @Override
        public String modelIdentifier() {
            return "test-provider";
        }
    }
}
