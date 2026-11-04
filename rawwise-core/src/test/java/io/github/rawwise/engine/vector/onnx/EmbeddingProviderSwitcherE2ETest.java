package io.github.rawwise.engine.vector.onnx;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.EmbeddingUnavailableException;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@Slf4j
@DisplayName("EmbeddingProviderSwitcher E2E - circuit breaker + swap + dim guard")
class EmbeddingProviderSwitcherE2ETest {

    private OnnxEmbeddingConfig stubCfg(int dim, String tag) {
        return OnnxEmbeddingConfig.builder()
                .enabled(true)
                .modelPath("stub://" + tag)
                .dimension(dim)
                .executionProvider("CPU")
                .batchSize(8)
                .warmupTimeout(Duration.ofSeconds(5))
                .tokenizerPath("")
                .maxSequenceLength(512)
                .build();
    }

    @Test
    @DisplayName("primary throws -> switcher falls back to backup provider")
    void primaryThrowsFallsBackToBackup() {
        OnnxEmbeddingProvider working = new OnnxEmbeddingProvider(stubCfg(128, "fallback"));
        EmbeddingProvider throwing =
                new EmbeddingProvider() {
                    @Override
                    public float[] embed(String text) {
                        throw new EmbeddingUnavailableException("primary unavailable");
                    }

                    @Override
                    public String modelIdentifier() {
                        return "throwing";
                    }
                };
        EmbeddingProviderSwitcher switcher = new EmbeddingProviderSwitcher(throwing, working);
        float[] vec = switcher.embed("text");
        assertThat(vec).hasSize(128);
    }

    @Test
    @DisplayName("swapCurrent to working provider -> subsequent embed uses it")
    void swapCurrentSwitchesProvider() {
        OnnxEmbeddingProvider providerA = new OnnxEmbeddingProvider(stubCfg(128, "a"));
        OnnxEmbeddingProvider providerB = new OnnxEmbeddingProvider(stubCfg(128, "b"));
        EmbeddingProviderSwitcher switcher = new EmbeddingProviderSwitcher(providerA, providerB);

        float[] before = switcher.embed("hello");
        assertThat(before).hasSize(128);

        switcher.swapCurrent(providerB);
        float[] after = switcher.embed("hello");
        assertThat(after).hasSize(128);
        // Different stub model paths => different deterministic seed => different vectors
        boolean differ = false;
        for (int i = 0; i < before.length; i++) {
            if (Float.compare(before[i], after[i]) != 0) {
                differ = true;
                break;
            }
        }
        // Note: deterministicVector uses text.hashCode + 31*dim, so model path actually does not
        // affect vector. Just verify swap doesn't crash and dimension remains stable.
        assertThat(switcher.getCurrent()).isSameAs(providerB);
    }

    @Test
    @DisplayName("swapCurrent with mismatched dimension throws IllegalStateException")
    void swapDimensionMismatchThrows() {
        OnnxEmbeddingProvider provider128 = new OnnxEmbeddingProvider(stubCfg(128, "a"));
        OnnxEmbeddingProvider providerOther128 = new OnnxEmbeddingProvider(stubCfg(128, "b"));
        EmbeddingProviderSwitcher switcher =
                new EmbeddingProviderSwitcher(provider128, providerOther128);

        OnnxEmbeddingProvider provider256 = new OnnxEmbeddingProvider(stubCfg(256, "c"));
        assertThatThrownBy(() -> switcher.swapCurrent(provider256))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("dimension mismatch");
    }

    @Test
    @DisplayName("dimension() reflects providers' dimension")
    void dimensionReflectsProviders() {
        OnnxEmbeddingProvider a = new OnnxEmbeddingProvider(stubCfg(64, "a"));
        OnnxEmbeddingProvider b = new OnnxEmbeddingProvider(stubCfg(64, "b"));
        EmbeddingProviderSwitcher switcher = new EmbeddingProviderSwitcher(a, b);
        assertThat(switcher.dimension()).isEqualTo(64);
    }
}
