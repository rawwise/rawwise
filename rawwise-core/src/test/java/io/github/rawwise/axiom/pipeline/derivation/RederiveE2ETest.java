package io.github.rawwise.axiom.pipeline.derivation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.vector.EmbeddingProviderFactory;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.server.AxiomIngestRequest;
import io.github.rawwise.server.AxiomIngestResponse;
import io.github.rawwise.server.ExecutorPointQueryServiceGrpc;
import io.github.rawwise.server.ExecutorRederiveRequest;
import io.github.rawwise.server.ExecutorRederiveResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Rederive E2E - be derivation API scenariotest")
class RederiveE2ETest {

    @TempDir static Path tempDir;

    private static String serverName;
    private static Server grpcServer;
    private static ManagedChannel channel;
    private static ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub stub;
    private static io.github.rawwise.backend.executor.service.ExecutorPointQueryServiceImpl
            serviceImpl;
    private static Metastore metastore;

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

        serviceImpl =
                new io.github.rawwise.backend.executor.service.ExecutorPointQueryServiceImpl(
                        config);
        serverName = "rederive-e2e-" + UUID.randomUUID();
        grpcServer =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(serviceImpl)
                        .build()
                        .start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        stub = ExecutorPointQueryServiceGrpc.newBlockingStub(channel);
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

    private AxiomIngestResponse ingestRecord(String tableId, String key, String body) {
        registerTable(tableId);
        AxiomIngestRequest req =
                AxiomIngestRequest.newBuilder()
                        .setTableId(tableId)
                        .setKey(ByteString.copyFromUtf8(key))
                        .setBody(ByteString.copyFromUtf8(body))
                        .setImmutable(false)
                        .build();
        return stub.withDeadlineAfter(30, java.util.concurrent.TimeUnit.SECONDS).axiomIngest(req);
    }

    /** 注册表 meta（db:table: TableMeta key），满足 executor ingest 的 fail-fast 前置要求。 */
    private void registerTable(String tableId) {
        try {
            String[] parts = tableId.split("\\.");
            byte[] metaKey =
                    (parts[0] + ":" + parts[1] + ":")
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
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

    private ExecutorRederiveResponse rederive(
            String tableId,
            RederiveService.Scope scope,
            String startKey,
            String endKey,
            String singleKey,
            boolean forceReindex) {
        ExecutorRederiveRequest.Builder reqBuilder =
                ExecutorRederiveRequest.newBuilder()
                        .setTableId(tableId)
                        .setScope(scope.ordinal())
                        .setForceReindex(forceReindex);
        if (startKey != null) {
            reqBuilder.setStartKey(ByteString.copyFromUtf8(startKey));
        }
        if (endKey != null) {
            reqBuilder.setEndKey(ByteString.copyFromUtf8(endKey));
        }
        if (singleKey != null) {
            reqBuilder.setSingleKey(ByteString.copyFromUtf8(singleKey));
        }
        return stub.withDeadlineAfter(120, java.util.concurrent.TimeUnit.SECONDS)
                .axiomRederive(reqBuilder.build());
    }

    @Test
    @DisplayName(
            "ingest into unregistered table fails fast (FAILED_PRECONDITION), no silent degraded write")
    void ingestUnregisteredTableFailsFast() {
        String tableId = uniqTable("unreg");
        AxiomIngestRequest req =
                AxiomIngestRequest.newBuilder()
                        .setTableId(tableId)
                        .setKey(ByteString.copyFromUtf8("doc:unreg-001"))
                        .setBody(ByteString.copyFromUtf8("content for unregistered table"))
                        .setImmutable(false)
                        .build();
        assertThatThrownBy(
                        () ->
                                stub.withDeadlineAfter(30, java.util.concurrent.TimeUnit.SECONDS)
                                        .axiomIngest(req))
                .isInstanceOf(io.grpc.StatusRuntimeException.class)
                .hasMessageContaining("Table not registered");
    }

    @Nested
    @DisplayName("S1-SingleKey - single key derivation")
    class SingleKeyRederive {

        @Test
        @DisplayName("write 1 item -> SINGLE_KEY rederive -> totalKeys=1 rederivedCount=1")
        void rederiveSingleKeyAfterIngest() {
            String tableId = uniqTable("s1_rederiv");
            String key = "doc:single-001";
            String body =
                    "This is test content for single key rederive scenario. It contains enough text for chunking.";

            AxiomIngestResponse ingestResp = ingestRecord(tableId, key, body);
            assertThat(ingestResp.getStatus()).isEqualTo("OK");

            ExecutorRederiveResponse resp =
                    rederive(tableId, RederiveService.Scope.SINGLE_KEY, null, null, key, true);

            assertThat(resp.getTotalKeys()).isEqualTo(1);
            assertThat(resp.getRederivedCount()).isEqualTo(1);
            assertThat(resp.getFailedCount()).isEqualTo(0);
            assertThat(resp.getStatus()).isEqualTo("OK");
        }

        @Test
        @DisplayName("SINGLE_KEY rederive non-existent key -> totalKeys=1 failedCount=1")
        void rederiveNonExistentKey() {
            String tableId = uniqTable("s1_missing");
            ingestRecord(tableId, "doc:exists-001", "Some content here for testing.");

            ExecutorRederiveResponse resp =
                    rederive(
                            tableId,
                            RederiveService.Scope.SINGLE_KEY,
                            null,
                            null,
                            "doc:nonexistent-999",
                            true);

            assertThat(resp.getTotalKeys()).isEqualTo(1);
            assertThat(resp.getFailedCount()).isEqualTo(1);
            assertThat(resp.getStatus()).isEqualTo("PARTIAL");
        }
    }

    @Nested
    @DisplayName("S2-FullTable - table derivation")
    class FullTableRederive {

        @Test
        @DisplayName("write 3 item -> FULL_TABLE rederive -> totalKeys=3")
        void rederiveFullTable() {
            String tableId = uniqTable("s2_full");
            ingestRecord(
                    tableId,
                    "doc:full-001",
                    "Content for document one. Enough text for chunking and embedding.");
            ingestRecord(
                    tableId,
                    "doc:full-002",
                    "Content for document two. Enough text for chunking and embedding.");
            ingestRecord(
                    tableId,
                    "doc:full-003",
                    "Content for document three. Enough text for chunking and embedding.");

            ExecutorRederiveResponse resp =
                    rederive(tableId, RederiveService.Scope.FULL_TABLE, null, null, null, true);

            assertThat(resp.getTotalKeys()).isEqualTo(3);
            assertThat(resp.getRederivedCount()).isEqualTo(3);
            assertThat(resp.getStatus()).isEqualTo("OK");
        }

        @Test
        @DisplayName("table FULL_TABLE rederive -> totalKeys=0 status=OK")
        void rederiveEmptyTable() {
            String tableId = uniqTable("s2_empty");

            ExecutorRederiveResponse resp =
                    rederive(tableId, RederiveService.Scope.FULL_TABLE, null, null, null, false);

            assertThat(resp.getTotalKeys()).isEqualTo(0);
            assertThat(resp.getRederivedCount()).isEqualTo(0);
            assertThat(resp.getStatus()).isEqualTo("OK");
        }
    }

    @Nested
    @DisplayName("S3-KeyRange - derivation")
    class KeyRangeRederive {

        @Test
        @DisplayName("write 5 item -> KEY_RANGE rederive middle 3 item -> totalKeys=3")
        void rederiveKeyRange() {
            String tableId = uniqTable("s3_range");
            ingestRecord(tableId, "key:001", "Document alpha for range testing.");
            ingestRecord(tableId, "key:002", "Document beta for range testing.");
            ingestRecord(tableId, "key:003", "Document gamma for range testing.");
            ingestRecord(tableId, "key:004", "Document delta for range testing.");
            ingestRecord(tableId, "key:005", "Document epsilon for range testing.");

            ExecutorRederiveResponse resp =
                    rederive(
                            tableId,
                            RederiveService.Scope.KEY_RANGE,
                            "key:002",
                            "key:004",
                            null,
                            true);

            assertThat(resp.getTotalKeys()).isEqualTo(3);
            assertThat(resp.getRederivedCount()).isEqualTo(3);
            assertThat(resp.getStatus()).isEqualTo("OK");
        }
    }

    @Nested
    @DisplayName("S4-ForceReindex - CAS intent cleanupand derivation")
    class ForceReindex {

        @Test
        @DisplayName("forceReindex=false CAS intent derivation -> rederivedCount=0 (skipped)")
        void rederiveWithoutForceReindex() {
            String tableId = uniqTable("s4_noforce");
            String key = "doc:noforce-001";
            ingestRecord(
                    tableId, key, "Content that was already derived. Enough text for chunking.");

            ExecutorRederiveResponse resp =
                    rederive(tableId, RederiveService.Scope.SINGLE_KEY, null, null, key, false);

            assertThat(resp.getTotalKeys()).isEqualTo(1);
            assertThat(resp.getRederivedCount()).isEqualTo(0);
            assertThat(resp.getStatus()).isEqualTo("OK");
        }

        @Test
        @DisplayName("forceReindex=true cleanup CAS intent after derivationsuccess")
        void rederiveWithForceReindex() {
            String tableId = uniqTable("s4_force");
            String key = "doc:force-001";
            ingestRecord(
                    tableId, key, "Content that was already derived. Enough text for chunking.");

            ExecutorRederiveResponse resp =
                    rederive(tableId, RederiveService.Scope.SINGLE_KEY, null, null, key, true);

            assertThat(resp.getTotalKeys()).isEqualTo(1);
            assertThat(resp.getRederivedCount()).isEqualTo(1);
            assertThat(resp.getStatus()).isEqualTo("OK");
        }
    }
}
