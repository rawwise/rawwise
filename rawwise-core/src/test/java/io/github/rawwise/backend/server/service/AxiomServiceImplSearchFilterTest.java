package io.github.rawwise.backend.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.github.rawwise.backend.executor.service.ExecutorPointQueryServiceImpl;
import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.EmbeddingProviderFactory;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.proto.axiom.AxiomSearchRequest;
import io.github.rawwise.proto.axiom.AxiomSearchResponse;
import io.github.rawwise.server.AxiomIngestRequest;
import io.github.rawwise.server.AxiomIngestResponse;
import io.github.rawwise.server.ExecutorPointQueryServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Task 1.2 - AxiomServiceImpl.search filter + properties/raw_text")
class AxiomServiceImplSearchFilterTest {

    @TempDir static Path tempDir;

    private static String serverName;
    private static Server grpcServer;
    private static ManagedChannel channel;
    private static ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub executorStub;
    private static Metastore metastore;
    private static AxiomServiceImpl axiomService;

    @BeforeAll
    static void setUpAll() throws Exception {
        try {
            StorageRegistry.instance().register(new LocalStorageProvider());
        } catch (Exception ignored) {
        }

        System.setProperty(EmbeddingProviderFactory.KEY_TYPE, "stub");
        System.setProperty(EmbeddingProviderFactory.KEY_DIMENSION, "16");
        System.setProperty("rawwise.axiom.derivation.async", "false");
        System.setProperty("rawwise.axiom.cognition.stage.enabled", "false");
        System.setProperty("rawwise.warehouse.path", tempDir.toString());

        Map<String, Object> cfg = new HashMap<>();
        cfg.put("rawwise.catalog.warehouse-path", tempDir.toString());
        cfg.put("rawwise.storage.lmdb.initial-map-size-mb", 16L);
        cfg.put("rawwise.storage.lmdb.max-map-size-mb", 256L);
        RawWiseConfig config = RawWiseConfig.fromMap(cfg);

        DynamicMmapStore store = DynamicMmapStore.getInstance(tempDir.toString(), "spark", config);
        metastore = new InMemoryMetastore();
        store.setMetastore(metastore);
        store.initAxiomEngines(config);
        store.start();

        ExecutorPointQueryServiceImpl serviceImpl = new ExecutorPointQueryServiceImpl(config);
        serverName = "axiom-search-filter-" + UUID.randomUUID();
        grpcServer =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(serviceImpl)
                        .build()
                        .start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        executorStub = ExecutorPointQueryServiceGrpc.newBlockingStub(channel);

        EmbeddingProvider ep = EmbeddingProviderFactory.createFromSystemProperties();
        axiomService = new AxiomServiceImpl();
        axiomService.setExecutorStubs(List.of(executorStub));
        axiomService.setEmbeddingProvider(ep);
    }

    @AfterAll
    static void tearDownAll() {
        if (channel != null) channel.shutdownNow();
        if (grpcServer != null) grpcServer.shutdownNow();
        System.clearProperty(EmbeddingProviderFactory.KEY_TYPE);
        System.clearProperty(EmbeddingProviderFactory.KEY_DIMENSION);
        System.clearProperty("rawwise.axiom.derivation.async");
        System.clearProperty("rawwise.axiom.cognition.stage.enabled");
        System.clearProperty("rawwise.warehouse.path");
    }

    private static String uniqTable(String prefix) {
        return "default." + prefix + "_" + UUID.randomUUID().toString().substring(0, 8);
    }

    private void registerTable(String tableId) {
        try {
            String[] parts = tableId.split("\\.");
            byte[] metaKey = (parts[0] + ":" + parts[1] + ":").getBytes(StandardCharsets.UTF_8);
            if (metastore.exists(metaKey)) return;
            io.github.rawwise.meta.PbTableMetadata meta =
                    io.github.rawwise.meta.PbTableMetadata.newBuilder()
                            .putProperties("rawwise.ai-native", "true")
                            .putProperties("derivation.strategy", "auto")
                            .build();
            metastore.put(metaKey, meta.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException("Failed to register table meta for " + tableId, e);
        }
    }

    private AxiomIngestResponse ingest(
            String tableId, String key, String body, Map<String, String> props) {
        registerTable(tableId);
        AxiomIngestRequest req =
                AxiomIngestRequest.newBuilder()
                        .setTableId(tableId)
                        .setKey(ByteString.copyFromUtf8(key))
                        .setBody(ByteString.copyFromUtf8(body))
                        .putAllProperties(props)
                        .setImmutable(false)
                        .build();
        return executorStub.withDeadlineAfter(30, TimeUnit.SECONDS).axiomIngest(req);
    }

    private AxiomSearchResponse search(AxiomSearchRequest request) throws Exception {
        AtomicReference<AxiomSearchResponse> respRef = new AtomicReference<>();
        AtomicReference<Throwable> errRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        axiomService.search(
                request,
                new StreamObserver<>() {
                    @Override
                    public void onNext(AxiomSearchResponse value) {
                        respRef.set(value);
                    }

                    @Override
                    public void onError(Throwable t) {
                        errRef.set(t);
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                });
        assertThat(latch.await(30, TimeUnit.SECONDS))
                .as("search must complete within 30s")
                .isTrue();
        if (errRef.get() != null) {
            throw new RuntimeException("search failed", errRef.get());
        }
        return respRef.get();
    }

    @Test
    @DisplayName("search returns record properties (agent_id/memory_tier) and raw_text body")
    void searchReturnsPropertiesAndRawText() throws Exception {
        String tableId = uniqTable("sf_props");
        String aliceKey = "uuid-alice-001";
        String bobKey = "uuid-bob-001";

        AxiomIngestResponse r1 =
                ingest(
                        tableId,
                        aliceKey,
                        "hello memory world from alice about project omega with enough text",
                        Map.of(
                                "agent_id",
                                "alice",
                                "memory_tier",
                                "episodic",
                                "importance",
                                "0.8"));
        assertThat(r1.getStatus()).isEqualTo("OK");
        AxiomIngestResponse r2 =
                ingest(
                        tableId,
                        bobKey,
                        "hello memory world from bob about project sigma with enough text",
                        Map.of("agent_id", "bob", "memory_tier", "semantic", "importance", "0.6"));
        assertThat(r2.getStatus()).isEqualTo("OK");

        AxiomSearchResponse resp =
                search(
                        AxiomSearchRequest.newBuilder()
                                .setTableId(tableId)
                                .setQuery("hello")
                                .setTopK(10)
                                .build());

        assertThat(resp.getResultsCount())
                .as("both ingested records should be recallable")
                .isGreaterThan(0);

        boolean sawProperties = false;
        boolean sawRawText = false;
        boolean sawAlice = false;
        boolean sawBob = false;
        for (var item : resp.getResultsList()) {
            Map<String, String> props = item.getPropertiesMap();
            if (props.containsKey("agent_id") && props.containsKey("memory_tier")) {
                sawProperties = true;
            }
            if ("alice".equals(props.get("agent_id"))) sawAlice = true;
            if ("bob".equals(props.get("agent_id"))) sawBob = true;
            if (!item.getRawText().isEmpty()) sawRawText = true;
        }
        assertThat(sawProperties)
                .as("results must carry persisted properties (agent_id/memory_tier)")
                .isTrue();
        assertThat(sawAlice).as("alice record recallable").isTrue();
        assertThat(sawBob).as("bob record recallable").isTrue();
        assertThat(sawRawText).as("results must carry raw_text (original body)").isTrue();
    }

    @Test
    @DisplayName("search filter=agent_id:alice returns only alice records, keeps her memory_tier")
    void searchFilterMatchesOnlyRequestedAgent() throws Exception {
        String tableId = uniqTable("sf_filter");
        ingest(
                tableId,
                "uuid-alice-002",
                "hello memory world from alice about project omega with enough text",
                Map.of("agent_id", "alice", "memory_tier", "episodic"));
        ingest(
                tableId,
                "uuid-bob-002",
                "hello memory world from bob about project sigma with enough text",
                Map.of("agent_id", "bob", "memory_tier", "semantic"));

        AxiomSearchResponse resp =
                search(
                        AxiomSearchRequest.newBuilder()
                                .setTableId(tableId)
                                .setQuery("hello")
                                .setTopK(10)
                                .putFilter("agent_id", "alice")
                                .build());

        assertThat(resp.getResultsCount())
                .as("alice-filtered search must recall at least one record")
                .isGreaterThan(0);
        for (var item : resp.getResultsList()) {
            assertThat(item.getPropertiesMap().getOrDefault("agent_id", ""))
                    .as("every filtered result must belong to agent_id=alice")
                    .isEqualTo("alice");
        }
        boolean sawEpisodic =
                resp.getResultsList().stream()
                        .anyMatch(i -> "episodic".equals(i.getPropertiesMap().get("memory_tier")));
        assertThat(sawEpisodic).as("alice filtered result keeps her memory_tier=episodic").isTrue();
    }
}
