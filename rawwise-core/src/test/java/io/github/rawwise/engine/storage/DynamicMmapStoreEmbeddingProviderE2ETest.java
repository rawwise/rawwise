package io.github.rawwise.engine.storage;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.vector.DeterministicStubEmbeddingProvider;
import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.EmbeddingProviderFactory;
import io.github.rawwise.engine.vector.HttpEmbeddingProvider;
import io.github.rawwise.engine.vector.NoOpEmbeddingProvider;
import io.github.rawwise.governance.config.RawWiseConfig;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * E2E test guarding against the regression where {@link DynamicMmapStore#initAxiomEngines} silently
 * constructed a hardcoded Ollama {@code HttpEmbeddingProvider} ({@code
 * http://localhost:11434/v1/embeddings} + {@code nomic-embed-text}) instead of routing through
 * {@link EmbeddingProviderFactory#createFromSystemProperties()}.
 *
 * <p>Bug history (2026-06-21): backend.log showed {@code "EmbeddingProvider initialized:
 * endpoint=http://localhost:11434/v1/embeddings model=nomic-embed-text dimension=768"} even when
 * {@code -Drawwise.embedding.provider.type=http} + {@code
 * -Drawwise.embedding.provider.format=volcengine_multimodal} were set, breaking the entire P1.1
 * wiring chain.
 */
class DynamicMmapStoreEmbeddingProviderE2ETest {

    private static final List<String> KEYS =
            List.of(
                    EmbeddingProviderFactory.KEY_TYPE,
                    EmbeddingProviderFactory.KEY_ENDPOINT,
                    EmbeddingProviderFactory.KEY_MODEL,
                    EmbeddingProviderFactory.KEY_DIMENSION,
                    EmbeddingProviderFactory.KEY_API_KEY,
                    EmbeddingProviderFactory.KEY_TIMEOUT_MS,
                    EmbeddingProviderFactory.KEY_MAX_RETRIES,
                    EmbeddingProviderFactory.KEY_BATCH_SIZE,
                    EmbeddingProviderFactory.KEY_FORMAT);

    @BeforeAll
    static void registerStorageProvider() {
        try {
            StorageRegistry.instance().register(new LocalStorageProvider());
        } catch (Exception e) {
            // Already registered, ignore
        }
    }

    @TempDir Path tempDir;

    @BeforeEach
    void clearProperties() {
        KEYS.forEach(System::clearProperty);
    }

    @AfterEach
    void resetProperties() {
        KEYS.forEach(System::clearProperty);
    }

    @Test
    @DisplayName(
            "initAxiomEngines with no system properties -> NoOp provider, NOT Ollama HTTP default")
    void initAxiomEnginesDefaultsToNoOpNotOllama() {
        DynamicMmapStore store =
                DynamicMmapStore.getInstance(tempDir.toString(), "store-" + UUID.randomUUID());
        store.initAxiomEngines(RawWiseConfig.empty());

        EmbeddingProvider provider = store.getEmbeddingProvider();
        assertThat(provider)
                .as(
                        "Default factory must not produce HttpEmbeddingProvider with hardcoded Ollama endpoint")
                .isNotInstanceOf(HttpEmbeddingProvider.class);
        assertThat(provider).isInstanceOf(NoOpEmbeddingProvider.class);
        assertThat(provider.modelIdentifier()).isEqualTo("noop-zero-vector");
    }

    @Test
    @DisplayName(
            "initAxiomEngines with -Drawwise.embedding.provider.type=stub -> DeterministicStub provider")
    void initAxiomEnginesUsesStubWhenSystemPropertySet() {
        System.setProperty(EmbeddingProviderFactory.KEY_TYPE, "stub");
        System.setProperty(EmbeddingProviderFactory.KEY_DIMENSION, "16");

        DynamicMmapStore store =
                DynamicMmapStore.getInstance(tempDir.toString(), "store-" + UUID.randomUUID());
        store.initAxiomEngines(RawWiseConfig.empty());

        EmbeddingProvider provider = store.getEmbeddingProvider();
        assertThat(provider)
                .as(
                        "type=stub must produce DeterministicStubEmbeddingProvider, not HttpEmbeddingProvider")
                .isInstanceOf(DeterministicStubEmbeddingProvider.class);
        assertThat(provider.modelIdentifier()).isEqualTo("stub-deterministic-l2norm");
        float[] vec = provider.embed("hello");
        assertThat(vec).hasSize(16);
    }

    @Test
    @DisplayName(
            "initAxiomEngines with -Drawwise.embedding.provider.type=http + format=volcengine_multimodal -> HttpEmbeddingProvider")
    void initAxiomEnginesUsesHttpProviderWhenConfigured() {
        System.setProperty(EmbeddingProviderFactory.KEY_TYPE, "http");
        System.setProperty(
                EmbeddingProviderFactory.KEY_ENDPOINT,
                "https://ark.cn-beijing.volces.com/api/v3/embeddings/multimodal");
        System.setProperty(EmbeddingProviderFactory.KEY_MODEL, "doubao-embedding-vision-251215");
        System.setProperty(EmbeddingProviderFactory.KEY_DIMENSION, "2048");
        System.setProperty(EmbeddingProviderFactory.KEY_FORMAT, "volcengine_multimodal");
        System.setProperty(EmbeddingProviderFactory.KEY_TIMEOUT_MS, "5000");

        DynamicMmapStore store =
                DynamicMmapStore.getInstance(tempDir.toString(), "store-" + UUID.randomUUID());
        store.initAxiomEngines(RawWiseConfig.empty());

        EmbeddingProvider provider = store.getEmbeddingProvider();
        assertThat(provider)
                .as(
                        "type=http with valid endpoint+model must construct HttpEmbeddingProvider via factory")
                .isInstanceOf(HttpEmbeddingProvider.class);
        // We don't actually call .embed() here — that would hit the real volcengine endpoint.
        // The factory wiring guarantee is what we verify in this test.
    }

    @Test
    @DisplayName(
            "Hardcoded Ollama default removed: legacy rawwise.axiom.embedding.* keys do NOT silently override factory")
    void legacyAxiomEmbeddingKeysDoNotResurrectOllamaDefault() {
        // Even if user accidentally still sets the OLD key family, the new factory path
        // should ignore them and fall back to NoOp (because new keys are unset).
        System.setProperty(
                "rawwise.axiom.embedding.endpoint", "http://localhost:11434/v1/embeddings");
        System.setProperty("rawwise.axiom.embedding.model", "nomic-embed-text");
        try {
            DynamicMmapStore store =
                    DynamicMmapStore.getInstance(tempDir.toString(), "store-" + UUID.randomUUID());
            store.initAxiomEngines(RawWiseConfig.empty());

            EmbeddingProvider provider = store.getEmbeddingProvider();
            assertThat(provider)
                    .as(
                            "Legacy rawwise.axiom.embedding.* keys must not produce Ollama HttpEmbeddingProvider")
                    .isNotInstanceOf(HttpEmbeddingProvider.class);
            assertThat(provider).isInstanceOf(NoOpEmbeddingProvider.class);
        } finally {
            System.clearProperty("rawwise.axiom.embedding.endpoint");
            System.clearProperty("rawwise.axiom.embedding.model");
        }
    }
}
