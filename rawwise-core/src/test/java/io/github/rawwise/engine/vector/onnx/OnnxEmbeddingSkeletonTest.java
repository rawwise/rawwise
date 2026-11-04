package io.github.rawwise.engine.vector.onnx;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.EmbeddingUnavailableException;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
@DisplayName("OnnxEmbedding Integration E2E - provider + reindex + switcher + registry")
class OnnxEmbeddingSkeletonTest {

    @TempDir Path tempDir;

    private Metastore metastore;
    private EmbeddingProviderRegistry registry;
    private ReindexCoordinator reindexCoordinator;

    @BeforeEach
    void setUp() throws Exception {
        metastore = new InMemoryMetastore();
        registry = new EmbeddingProviderRegistry(metastore);
        reindexCoordinator = new ReindexCoordinator(metastore);
    }

    @AfterEach
    void tearDown() throws Exception {
        metastore.close();
    }

    private OnnxEmbeddingConfig stubConfig(boolean enabled, int dim, String tag) {
        return OnnxEmbeddingConfig.builder()
                .enabled(enabled)
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
    @DisplayName("E2E: provider embed -> registry register -> reindex -> switcher transition")
    void fullLifecycleProviderRegistryReindexSwitcher() {
        OnnxEmbeddingConfig cfgV1 = stubConfig(true, 128, "model-v1");
        OnnxEmbeddingProvider providerV1 = new OnnxEmbeddingProvider(cfgV1);
        assertThat(providerV1.isStubMode()).isTrue();

        float[] vec = providerV1.embed("hello world");
        assertThat(vec).hasSize(128);
        double norm = l2Norm(vec);
        assertThat(norm).isCloseTo(1.0, org.assertj.core.data.Offset.offset(1e-4));

        registry.registerCurrentModel("model-v1", cfgV1);
        Optional<EmbeddingProviderRegistry.RegisteredModel> current = registry.getCurrentModel();
        assertThat(current).isPresent();
        assertThat(current.get().modelId()).isEqualTo("model-v1");
        assertThat(current.get().config().dimension()).isEqualTo(128);
        assertThat(registry.versionCount()).isEqualTo(1);

        OnnxEmbeddingConfig cfgV2 = stubConfig(true, 128, "model-v2");
        registry.registerCurrentModel("model-v2", cfgV2);
        assertThat(registry.versionCount()).isEqualTo(2);
        assertThat(registry.getVersion("model-v1")).isPresent();
        assertThat(registry.getVersion("model-v2")).isPresent();
        assertThat(registry.getCurrentModel().get().modelId()).isEqualTo("model-v2");

        Map<String, String> sourceData = new HashMap<>();
        sourceData.put("row-1", "hello");
        sourceData.put("row-2", "world");
        sourceData.put("row-3", "reindex");

        Map<String, float[]> reindexedData = new HashMap<>();

        String jobId = reindexCoordinator.submitReindex("src_table", "dest_table", 128, 128);
        assertThat(jobId).isNotBlank();

        Optional<ReindexJob> pendingJob = reindexCoordinator.getJobStatus(jobId);
        assertThat(pendingJob).isPresent();
        assertThat(pendingJob.get().status()).isEqualTo(ReindexJob.Status.PENDING);

        ReindexJob.Result result =
                reindexCoordinator.executeReindex(
                        jobId,
                        context -> {
                            context.setTotalRows(sourceData.size());
                            OnnxEmbeddingProvider reindexProvider =
                                    new OnnxEmbeddingProvider(
                                            stubConfig(
                                                    true,
                                                    context.destDimension(),
                                                    "reindex-model"));
                            for (Map.Entry<String, String> entry : sourceData.entrySet()) {
                                float[] embedded = reindexProvider.embed(entry.getValue());
                                reindexedData.put(entry.getKey(), embedded);
                                context.incrementProcessed(1);
                            }
                        });

        assertThat(result.success()).isTrue();
        assertThat(result.processedRows()).isEqualTo(3);
        assertThat(reindexedData).hasSize(3);
        reindexedData.values().forEach(v -> assertThat(v).hasSize(128));

        Optional<ReindexJob> completedJob = reindexCoordinator.getJobStatus(jobId);
        assertThat(completedJob).isPresent();
        assertThat(completedJob.get().status()).isEqualTo(ReindexJob.Status.COMPLETED);
        assertThat(completedJob.get().processedRows()).isEqualTo(3);
        assertThat(completedJob.get().totalRows()).isEqualTo(3);
        assertThat(completedJob.get().completedAtMs()).isGreaterThan(0);

        OnnxEmbeddingProvider providerV2 = new OnnxEmbeddingProvider(cfgV2);
        OnnxEmbeddingProvider fallbackProvider =
                new OnnxEmbeddingProvider(stubConfig(true, 128, "fallback"));
        EmbeddingProviderSwitcher switcher =
                new EmbeddingProviderSwitcher(providerV1, fallbackProvider);

        float[] beforeTransition = switcher.embed("test-text");
        assertThat(beforeTransition).hasSize(128);
        assertThat(switcher.isInTransition()).isFalse();
        assertThat(switcher.getShadow()).isNull();

        switcher.startTransition(providerV2);
        assertThat(switcher.isInTransition()).isTrue();
        assertThat(switcher.getShadow()).isSameAs(providerV2);

        float[] duringTransition1 = switcher.embed("dual-write-1");
        float[] duringTransition2 = switcher.embed("dual-write-2");
        assertThat(duringTransition1).hasSize(128);
        assertThat(duringTransition2).hasSize(128);
        assertThat(switcher.getTransitionDualWriteCount()).isEqualTo(2);
        assertThat(switcher.getCurrent()).isSameAs(providerV1);

        switcher.commitTransition();
        assertThat(switcher.isInTransition()).isFalse();
        assertThat(switcher.getShadow()).isNull();
        assertThat(switcher.getCurrent()).isSameAs(providerV2);

        float[] afterTransition = switcher.embed("post-commit");
        assertThat(afterTransition).hasSize(128);

        assertThat(switcher.modelIdentifier()).contains("model-v2");
    }

    @Test
    @DisplayName("E2E: reindex failure -> job status FAILED + error message recorded")
    void reindexFailureRecordsError() {
        String jobId = reindexCoordinator.submitReindex("src_table", "dest_table", 64, 128);

        ReindexJob.Result result =
                reindexCoordinator.executeReindex(
                        jobId,
                        context -> {
                            context.setTotalRows(10);
                            context.incrementProcessed(3);
                            throw new RuntimeException("disk full during reindex");
                        });

        assertThat(result.success()).isFalse();
        assertThat(result.processedRows()).isEqualTo(3);
        assertThat(result.errorMessage()).contains("disk full during reindex");

        Optional<ReindexJob> failedJob = reindexCoordinator.getJobStatus(jobId);
        assertThat(failedJob).isPresent();
        assertThat(failedJob.get().status()).isEqualTo(ReindexJob.Status.FAILED);
        assertThat(failedJob.get().errorMessage()).contains("disk full during reindex");
        assertThat(failedJob.get().processedRows()).isEqualTo(3);
        assertThat(failedJob.get().completedAtMs()).isGreaterThan(0);
    }

    @Test
    @DisplayName("E2E: reindex cannot execute same job twice")
    void cannotExecuteSameJobTwice() {
        String jobId = reindexCoordinator.submitReindex("src", "dest", 64, 128);
        reindexCoordinator.executeReindex(
                jobId,
                context -> {
                    context.setTotalRows(1);
                    context.incrementProcessed(1);
                });

        assertThatThrownBy(() -> reindexCoordinator.executeReindex(jobId, context -> {}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already started");
    }

    @Test
    @DisplayName("E2E: switcher abortTransition discards shadow and keeps current")
    void abortTransitionKeepsCurrent() {
        OnnxEmbeddingProvider primary = new OnnxEmbeddingProvider(stubConfig(true, 64, "primary"));
        OnnxEmbeddingProvider fallback =
                new OnnxEmbeddingProvider(stubConfig(true, 64, "fallback"));
        OnnxEmbeddingProvider newProvider = new OnnxEmbeddingProvider(stubConfig(true, 64, "new"));
        EmbeddingProviderSwitcher switcher = new EmbeddingProviderSwitcher(primary, fallback);

        switcher.startTransition(newProvider);
        assertThat(switcher.isInTransition()).isTrue();
        switcher.embed("warmup-1");
        switcher.embed("warmup-2");
        assertThat(switcher.getTransitionDualWriteCount()).isEqualTo(2);

        switcher.abortTransition();
        assertThat(switcher.isInTransition()).isFalse();
        assertThat(switcher.getShadow()).isNull();
        assertThat(switcher.getCurrent()).isSameAs(primary);

        float[] vec = switcher.embed("after-abort");
        assertThat(vec).hasSize(64);
    }

    @Test
    @DisplayName("E2E: switcher primary fails during transition -> uses shadow result")
    void primaryFailsDuringTransitionUsesShadow() {
        EmbeddingProvider throwingPrimary =
                new EmbeddingProvider() {
                    @Override
                    public float[] embed(String text) {
                        throw new EmbeddingUnavailableException("primary down");
                    }

                    @Override
                    public String modelIdentifier() {
                        return "throwing-primary";
                    }
                };
        OnnxEmbeddingProvider fallback =
                new OnnxEmbeddingProvider(stubConfig(true, 64, "fallback"));
        OnnxEmbeddingProvider shadow = new OnnxEmbeddingProvider(stubConfig(true, 64, "shadow"));

        EmbeddingProviderSwitcher switcher =
                new EmbeddingProviderSwitcher(throwingPrimary, fallback);
        switcher.startTransition(shadow);

        float[] vec = switcher.embed("failover-test");
        assertThat(vec).hasSize(64);
        assertThat(switcher.getTransitionDualWriteCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("E2E: registry round-trip serialization with Duration field in config")
    void registryRoundTripWithDurationConfig() {
        OnnxEmbeddingConfig cfg =
                OnnxEmbeddingConfig.builder()
                        .enabled(true)
                        .modelPath("stub://duration-test")
                        .dimension(256)
                        .executionProvider("CUDA")
                        .batchSize(16)
                        .warmupTimeout(Duration.ofSeconds(45))
                        .tokenizerPath("/tmp/tokenizer")
                        .maxSequenceLength(1024)
                        .build();

        registry.registerCurrentModel("duration-model", cfg);

        Optional<EmbeddingProviderRegistry.RegisteredModel> retrieved = registry.getCurrentModel();
        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().config().dimension()).isEqualTo(256);
        assertThat(retrieved.get().config().warmupTimeout()).isEqualTo(Duration.ofSeconds(45));
        assertThat(retrieved.get().config().executionProvider()).isEqualTo("CUDA");
        assertThat(retrieved.get().config().batchSize()).isEqualTo(16);
        assertThat(retrieved.get().config().tokenizerPath()).isEqualTo("/tmp/tokenizer");

        Optional<EmbeddingProviderRegistry.RegisteredModel> fromHistory =
                registry.getVersion("duration-model");
        assertThat(fromHistory).isPresent();
        assertThat(fromHistory.get().config().warmupTimeout()).isEqualTo(Duration.ofSeconds(45));
    }

    @Test
    @DisplayName("E2E: reindex job persists to metastore and survives coordinator restart")
    void reindexJobPersistsAcrossCoordinatorRestart() throws Exception {
        String jobId = reindexCoordinator.submitReindex("src", "dest", 64, 128);
        reindexCoordinator.executeReindex(
                jobId,
                context -> {
                    context.setTotalRows(5);
                    for (int i = 0; i < 5; i++) {
                        context.incrementProcessed(1);
                    }
                });

        ReindexCoordinator newCoordinator = new ReindexCoordinator(metastore);
        Optional<ReindexJob> restored = newCoordinator.getJobStatus(jobId);
        assertThat(restored).isPresent();
        assertThat(restored.get().status()).isEqualTo(ReindexJob.Status.COMPLETED);
        assertThat(restored.get().processedRows()).isEqualTo(5);
        assertThat(restored.get().totalRows()).isEqualTo(5);
        assertThat(restored.get().srcTableId()).isEqualTo("src");
        assertThat(restored.get().destTableId()).isEqualTo("dest");
    }

    private static double l2Norm(float[] vec) {
        double sumSq = 0;
        for (float v : vec) sumSq += v * v;
        return Math.sqrt(sumSq);
    }
}
