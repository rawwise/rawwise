package io.github.rawwise.e2e;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.backend.executor.service.ExecutorPointQueryServiceImpl;
import io.github.rawwise.backend.server.auth.QueryAuthorizer;
import io.github.rawwise.backend.server.job.JobEventLog;
import io.github.rawwise.backend.server.job.SparkJobScheduler;
import io.github.rawwise.backend.server.load.BackendLoadInfo;
import io.github.rawwise.backend.server.load.BackpressureManager;
import io.github.rawwise.backend.server.load.LoadCollector;
import io.github.rawwise.backend.server.service.AxiomServiceImpl;
import io.github.rawwise.backend.server.service.QueryServiceImpl;
import io.github.rawwise.backend.server.session.InMemorySessionStore;
import io.github.rawwise.common.catalog.MetastoreCatalog;
import io.github.rawwise.common.catalog.MetastoreCatalogConfig;
import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.vector.EmbeddingProviderFactory;
import io.github.rawwise.frontend.api.FrontendServer;
import io.github.rawwise.frontend.api.routes.AxiomKnowledgeRoutes;
import io.github.rawwise.frontend.api.routes.AxiomRouteGroup;
import io.github.rawwise.frontend.backend.GrpcBackendClient;
import io.github.rawwise.frontend.config.FrontendOptions;
import io.github.rawwise.frontend.evaluator.DefaultQueryEvaluator;
import io.github.rawwise.frontend.ir.IrRuntime;
import io.github.rawwise.frontend.session.FrontendSessionStore;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.proto.axiom.AxiomServiceGrpc;
import io.github.rawwise.server.ExecutorPointQueryServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("AgentMemory Table Bootstrap - agent_memory registration via bootstrap DDL")
class AxiomMemoryTableBootstrapTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    private static SparkSession spark;
    private static Server grpcServer;
    private static SparkJobScheduler scheduler;
    private static InMemorySessionStore sessionStore;
    private static FrontendServer frontendServer;
    private static GrpcBackendClient backendClient;
    private static FrontendSessionStore frontendSessionStore;
    private static AxiomServiceImpl axiomService;
    private static ManagedChannel executorChannel;
    private static ManagedChannel axiomChannel;
    private static int backendPort;
    private static DynamicMmapStore store;
    private static String frontendBaseUrl;

    @TempDir static Path tempDir;

    @BeforeAll
    static void setUpAll() throws Exception {
        try {
            StorageRegistry.instance().register(new LocalStorageProvider());
        } catch (Exception ignored) {
        }

        deleteDirectory(new File("/tmp/spark-warehouse-agent-memory-bootstrap-e2e"));

        String warehousePath = tempDir.resolve("warehouse").toString();
        System.setProperty("rawwise.warehouse.path", warehousePath);
        System.setProperty("rawwise.storage.lmdb.initial-map-size-mb", "16");
        System.setProperty("rawwise.storage.lmdb.max-map-size-mb", "256");
        System.setProperty("rawwise.metastore.type", "inmemory");
        System.setProperty("rawwise.frontend.ir.enabled", "true");
        System.setProperty("rawwise.axiom.enabled", "true");
        System.setProperty("rawwise.context.enabled", "true");
        System.setProperty(EmbeddingProviderFactory.KEY_TYPE, "stub");
        System.setProperty(EmbeddingProviderFactory.KEY_DIMENSION, "16");
        System.setProperty("rawwise.axiom.derivation.async", "false");
        System.setProperty("rawwise.axiom.cognition.stage.enabled", "false");

        Metastore metastore = new InMemoryMetastore();
        io.github.rawwise.backend.shared.BackendSharedRegistry.setMetastore(metastore);

        spark =
                SparkSession.builder()
                        .master("local[2]")
                        .appName("AxiomMemoryTableBootstrapTest")
                        .config("spark.driver.memory", "2g")
                        .config("spark.driver.host", "localhost")
                        .config("spark.driver.bindAddress", "localhost")
                        .config("spark.sql.warehouse.dir", warehousePath)
                        .config("spark.ui.enabled", "false")
                        .config("spark.sql.adaptive.enabled", "false")
                        .config(
                                "spark.sql.catalog.spark_catalog",
                                "io.github.rawwise.backend.spark.RawWiseSparkCatalog")
                        .config("spark.sql.catalog.spark_catalog.warehouse-path", warehousePath)
                        .config(
                                "spark.sql.catalog.spark_catalog.catalog-implementation",
                                "metastore")
                        .config("spark.sql.catalog.spark_catalog.store-kind", "inmemory")
                        .config("spark.rawwise.write.local", "true")
                        .getOrCreate();

        sessionStore = new InMemorySessionStore();
        AtomicInteger queueLength = new AtomicInteger(0);
        scheduler =
                new SparkJobScheduler(spark, sessionStore, queueLength, null, new JobEventLog());
        scheduler.start();

        LoadCollector lowLoadCollector =
                () ->
                        BackendLoadInfo.builder()
                                .queueLength(0)
                                .cpuUsage(0.1)
                                .memoryUsage(0.1)
                                .diskUsage(0.0)
                                .networkUsage(0.0)
                                .build();
        BackpressureManager backpressureManager = new BackpressureManager(lowLoadCollector);
        QueryAuthorizer queryAuthorizer = new QueryAuthorizer();
        QueryServiceImpl queryService =
                new QueryServiceImpl(sessionStore, scheduler, backpressureManager, queryAuthorizer);

        Map<String, Object> cfg = new HashMap<>();
        cfg.put("rawwise.catalog.warehouse-path", warehousePath);
        cfg.put("rawwise.storage.lmdb.initial-map-size-mb", 16L);
        cfg.put("rawwise.storage.lmdb.max-map-size-mb", 256L);
        RawWiseConfig rawWiseConfig = RawWiseConfig.fromMap(cfg);

        store = DynamicMmapStore.getInstance(warehousePath, "spark", rawWiseConfig);
        store.setMetastore(metastore);
        store.initAxiomEngines(rawWiseConfig);
        store.start();

        axiomService = new AxiomServiceImpl();
        ExecutorPointQueryServiceImpl executorService =
                new ExecutorPointQueryServiceImpl(rawWiseConfig);

        grpcServer =
                ServerBuilder.forPort(0)
                        .addService(queryService)
                        .addService(axiomService)
                        .addService(executorService)
                        .build()
                        .start();
        backendPort = grpcServer.getPort();

        executorChannel =
                ManagedChannelBuilder.forAddress("localhost", backendPort)
                        .usePlaintext()
                        .maxInboundMessageSize(64 * 1024 * 1024)
                        .build();
        ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub executorStub =
                ExecutorPointQueryServiceGrpc.newBlockingStub(executorChannel);
        axiomService.setExecutorStubs(List.of(executorStub));

        axiomService.setEmbeddingProvider(EmbeddingProviderFactory.createFromSystemProperties());

        axiomChannel =
                ManagedChannelBuilder.forAddress("localhost", backendPort)
                        .usePlaintext()
                        .maxInboundMessageSize(64 * 1024 * 1024)
                        .build();
        AxiomServiceGrpc.AxiomServiceBlockingStub axiomStub =
                AxiomServiceGrpc.newBlockingStub(axiomChannel);
        AxiomKnowledgeRoutes knowledgeRoutes = new AxiomKnowledgeRoutes(axiomStub, null);
        AxiomRouteGroup axiomRouteGroup =
                new AxiomRouteGroup(null, null, null, null, null, null, knowledgeRoutes);

        backendClient = new GrpcBackendClient("localhost", backendPort, 60);
        frontendSessionStore = new FrontendSessionStore();
        DefaultQueryEvaluator evaluator = new DefaultQueryEvaluator();

        MetastoreCatalogConfig catalogConfig =
                MetastoreCatalogConfig.builder().store(metastore).basePath(warehousePath).build();
        io.github.rawwise.common.catalog.Catalog catalog = new MetastoreCatalog(catalogConfig);
        SnapshotManager snapshotManager = new SnapshotManager(catalog, metastore);

        IrRuntime irRuntime = IrRuntime.create(catalog, snapshotManager);

        Map<String, Object> config = new HashMap<>();
        config.put("frontend.deployment.mode", "sidecar");
        config.put("auth.enabled", false);
        config.put("rawwise.frontend.port", 0);
        config.put("rawwise.orchestrator.enabled", false);
        FrontendOptions options = new FrontendOptions(config);

        frontendServer =
                new FrontendServer(
                        options,
                        frontendSessionStore,
                        backendClient,
                        evaluator,
                        irRuntime,
                        null,
                        null,
                        null,
                        null,
                        axiomRouteGroup,
                        null,
                        null,
                        null,
                        null,
                        null);
        frontendServer.start();

        int actualFrontendPort = frontendServer.getPort();
        frontendBaseUrl = "http://localhost:" + actualFrontendPort;
    }

    @AfterAll
    static void tearDownAll() {
        if (frontendServer != null) frontendServer.stop();
        if (backendClient != null) backendClient.close();
        if (axiomChannel != null) axiomChannel.shutdownNow();
        if (executorChannel != null) executorChannel.shutdownNow();
        if (axiomService != null) axiomService.close();
        if (grpcServer != null) grpcServer.shutdown();
        if (scheduler != null) scheduler.shutdown();
        if (frontendSessionStore != null) frontendSessionStore.close();
        if (spark != null) spark.stop();
        System.clearProperty("rawwise.warehouse.path");
        System.clearProperty("rawwise.storage.lmdb.initial-map-size-mb");
        System.clearProperty("rawwise.storage.lmdb.max-map-size-mb");
        System.clearProperty("rawwise.metastore.type");
        System.clearProperty("rawwise.frontend.ir.enabled");
        System.clearProperty("rawwise.axiom.enabled");
        System.clearProperty("rawwise.context.enabled");
        System.clearProperty(EmbeddingProviderFactory.KEY_TYPE);
        System.clearProperty(EmbeddingProviderFactory.KEY_DIMENSION);
        System.clearProperty("rawwise.axiom.derivation.async");
        System.clearProperty("rawwise.axiom.cognition.stage.enabled");
        io.github.rawwise.backend.shared.BackendSharedRegistry.reset();
    }

    private static void deleteDirectory(File dir) {
        if (!dir.exists()) return;
        try {
            Files.walk(dir.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (Exception ignored) {
        }
    }

    private String postJson(String path, String body) throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(frontendBaseUrl + path))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

    private String executeDdl(String sql) throws Exception {
        String body =
                MAPPER.writeValueAsString(
                        Map.of("sql", sql, "type", "DDL", "options", Map.of("collect", "true")));
        String submitResp = postJson("/api/query", body);
        JsonNode submitJson = MAPPER.readTree(submitResp);
        String jobId = submitJson.get("jobId").asText();
        assertThat(jobId).isNotEmpty();

        if (submitJson.has("state") && !"PENDING".equals(submitJson.get("state").asText())) {
            return submitResp;
        }

        await().atMost(30, SECONDS)
                .untilAsserted(
                        () -> {
                            String statusResp = get("/api/query/" + jobId + "/status");
                            String state = MAPPER.readTree(statusResp).get("state").asText();
                            assertThat(state).isIn("SUCCEEDED", "FAILED");
                        });

        return get("/api/query/" + jobId + "/result");
    }

    private String get(String path) throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder().uri(URI.create(frontendBaseUrl + path)).GET().build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

    /** 模拟 bin/rawwise-bootstrap-knowledge.sh 中的 agent_memory 建表段。 */
    private void bootstrapAgentMemoryTable() throws Exception {
        String createResult =
                executeDdl(
                        "CREATE TABLE IF NOT EXISTS default.agent_memory ("
                                + "  entry_id STRING, agent_id STRING, memory_tier STRING,"
                                + "  importance INT, content STRING"
                                + ") USING rawwise TBLPROPERTIES ("
                                + "  'primary-key'='entry_id',"
                                + "  'rawwise.ai-native'='true',"
                                + "  'derivation.strategy'='auto'"
                                + ")");
        JsonNode createJson = MAPPER.readTree(createResult);
        assertThat(createJson.get("state").asText())
                .as("CREATE TABLE default.agent_memory should succeed")
                .isEqualTo("SUCCEEDED");
    }

    @Test
    @DisplayName("AGENT MEMORY: ingest without registration reports Table not registered")
    void ingestWithoutRegistrationFails() throws Exception {
        String ingestBody =
                MAPPER.writeValueAsString(
                        Map.of(
                                "tableId",
                                "default.agent_memory",
                                "key",
                                "mem:pre:bootstrap",
                                "body",
                                "placeholder before table registration"));
        String ingestResp = postJson("/api/axiom/ingest", ingestBody);
        JsonNode ingestJson = MAPPER.readTree(ingestResp);
        assertThat(ingestJson.has("error"))
                .as("ingest to unregistered agent_memory should fail: %s", ingestResp)
                .isTrue();
        assertThat(ingestJson.get("error").asText())
                .as("error should mention table registration")
                .contains("Table not registered");
    }

    @Test
    @DisplayName("AGENT MEMORY: after bootstrap DDL, ingest to agent_memory succeeds")
    void ingestAfterBootstrapSucceeds() throws Exception {
        bootstrapAgentMemoryTable();

        String ingestBody =
                MAPPER.writeValueAsString(
                        Map.of(
                                "tableId",
                                "default.agent_memory",
                                "key",
                                "mem:agent-1:turn:1",
                                "body",
                                "user asked about transaction commit pipeline and LMDB write performance.",
                                "properties",
                                Map.of(
                                        "agent_id",
                                        "agent-1",
                                        "memory_tier",
                                        "EPISODIC",
                                        "importance",
                                        "5")));
        String ingestResp = postJson("/api/axiom/ingest", ingestBody);
        JsonNode ingestJson = MAPPER.readTree(ingestResp);
        assertThat(ingestJson.has("error"))
                .as("ingest should not return error: %s", ingestResp)
                .isFalse();
        assertThat(ingestJson.get("status").asText()).isEqualTo("OK");
        assertThat(ingestJson.get("recordKey").asText()).isNotEmpty();
    }
}
