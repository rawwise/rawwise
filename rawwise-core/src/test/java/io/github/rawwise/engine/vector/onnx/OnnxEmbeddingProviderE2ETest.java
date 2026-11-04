package io.github.rawwise.engine.vector.onnx;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.engine.vector.EmbeddingUnavailableException;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@Slf4j
@DisplayName("OnnxEmbeddingProvider E2E - stub model deterministic embedding")
class OnnxEmbeddingProviderE2ETest {

    private OnnxEmbeddingConfig stubConfig(boolean enabled, int dim) {
        return OnnxEmbeddingConfig.builder()
                .enabled(enabled)
                .modelPath("stub://test-model")
                .dimension(dim)
                .executionProvider("CPU")
                .batchSize(8)
                .warmupTimeout(Duration.ofSeconds(5))
                .tokenizerPath("")
                .maxSequenceLength(512)
                .build();
    }

    @Test
    @DisplayName("embed returns float[dimension] vector for stub model")
    void embedReturnsVectorOfCorrectDimension() {
        OnnxEmbeddingProvider provider = new OnnxEmbeddingProvider(stubConfig(true, 128));
        float[] vec = provider.embed("hello");
        assertThat(vec).hasSize(128);
    }

    @Test
    @DisplayName("embed is deterministic - same text produces same vector")
    void embedDeterministic() {
        OnnxEmbeddingProvider provider = new OnnxEmbeddingProvider(stubConfig(true, 128));
        float[] a = provider.embed("hello");
        float[] b = provider.embed("hello");
        assertThat(a).containsExactly(b);
    }

    @Test
    @DisplayName("different texts produce different vectors")
    void differentTextsDifferentVectors() {
        OnnxEmbeddingProvider provider = new OnnxEmbeddingProvider(stubConfig(true, 128));
        float[] a = provider.embed("hello");
        float[] b = provider.embed("world");
        boolean differ = false;
        for (int i = 0; i < a.length; i++) {
            if (Float.compare(a[i], b[i]) != 0) {
                differ = true;
                break;
            }
        }
        assertThat(differ).as("two distinct texts must yield distinct vectors").isTrue();
    }

    @Test
    @DisplayName("output vector is L2-normalized (norm ~= 1.0)")
    void embedIsL2Normalized() {
        OnnxEmbeddingProvider provider = new OnnxEmbeddingProvider(stubConfig(true, 128));
        float[] vec = provider.embed("normalize me");
        double sumSq = 0.0;
        for (float v : vec) sumSq += v * v;
        double norm = Math.sqrt(sumSq);
        assertThat(norm).isCloseTo(1.0, org.assertj.core.data.Offset.offset(1e-4));
    }

    @Test
    @DisplayName("enabled=false -> embed throws EmbeddingUnavailableException")
    void disabledProviderThrows() {
        OnnxEmbeddingProvider provider = new OnnxEmbeddingProvider(stubConfig(false, 128));
        assertThatThrownBy(() -> provider.embed("x"))
                .isInstanceOf(EmbeddingUnavailableException.class)
                .hasMessageContaining("disabled");
    }
}
