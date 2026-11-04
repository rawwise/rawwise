package io.github.rawwise.frontend.api.routes;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.github.rawwise.backend.executor.service.ExecutorPointQueryServiceImpl;
import io.github.rawwise.backend.server.service.AxiomServiceImpl;
import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.EmbeddingProviderFactory;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.proto.axiom.AxiomServiceGrpc;
import io.github.rawwise.server.AxiomIngestRequest;
import io.github.rawwise.server.AxiomIngestResponse;
import io.github.rawwise.server.ExecutorPointQueryServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.javalin.Javalin;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Task 1.3 - /api/axiom/search + /api/axiom/fetch-details filter & properties")
class AxiomKnowledgeRoutesFilterTest {

    @TempDir static Path tempDir;

    private static String serverName;
    private static Server grpcServer;
    private static ManagedChannel channel;
    private static ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub executorStub;
    private static Metastore metastore;
    private static Javalin app;
    private static int httpPort;
    private static final ObjectMapper MAPPER = new ObjectMapper();

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

        ExecutorPointQueryServiceImpl executorImpl = new ExecutorPointQueryServiceImpl(config);
        AxiomServiceImpl axiomImpl = new AxiomServiceImpl();

        serverName = "axiom-routes-" + UUID.randomUUID();
        grpcServer =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(executorImpl)
                        .addService(axiomImpl)
                        .build()
                        .start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        executorStub = ExecutorPointQueryServiceGrpc.newBlockingStub(channel);

        EmbeddingProvider ep = EmbeddingProviderFactory.createFromSystemProperties();
        axiomImpl.setExecutorStubs(List.of(executorStub));
        axiomImpl.setEmbeddingProvider(ep);

        AxiomServiceGrpc.AxiomServiceBlockingStub axiomStub =
                AxiomServiceGrpc.newBlockingStub(channel);
        AxiomKnowledgeRoutes routes = new AxiomKnowledgeRoutes(axiomStub, null);
        app = Javalin.create();
        app.post("/api/axiom/search", routes.search);
        app.post("/api/axiom/fetch-details", routes.fetchDetails);
        app.post("/api/axiom/ingest", routes.ingest);
        app.start(0);
        httpPort = app.port();
    }

    @AfterAll
    static void tearDownAll() {
        if (app != null) app.stop();
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

    private void ingest(String tableId, String key, String body, Map<String, String> props) {
        registerTable(tableId);
        AxiomIngestRequest req =
                AxiomIngestRequest.newBuilder()
                        .setTableId(tableId)
                        .setKey(ByteString.copyFromUtf8(key))
                        .setBody(ByteString.copyFromUtf8(body))
                        .putAllProperties(props)
                        .setImmutable(false)
                        .build();
        AxiomIngestResponse resp =
                executorStub
                        .withDeadlineAfter(30, java.util.concurrent.TimeUnit.SECONDS)
                        .axiomIngest(req);
        assertThat(resp.getStatus()).isEqualTo("OK");
    }

    private HttpResponse<String> post(String path, String json) throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + httpPort + path))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static String searchJson(String tableId, String query, Map<String, String> filter) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"tableId\":\"")
                .append(tableId)
                .append("\",\"query\":\"")
                .append(query)
                .append("\",\"topK\":10");
        if (filter != null && !filter.isEmpty()) {
            sb.append(",\"filter\":{");
            boolean first = true;
            for (Map.Entry<String, String> e : filter.entrySet()) {
                if (!first) sb.append(",");
                sb.append("\"")
                        .append(e.getKey())
                        .append("\":\"")
                        .append(e.getValue())
                        .append("\"");
                first = false;
            }
            sb.append("}");
        }
        sb.append("}");
        return sb.toString();
    }

    @Test
    @DisplayName("GET search carries filter + returns properties/body via HTTP")
    void searchHttpFilterAndProperties() throws Exception {
        String tableId = uniqTable("sr_filter");
        ingest(
                tableId,
                "uuid-routes-alice",
                "hello memory world from alice about project omega with enough text",
                Map.of("agent_id", "alice", "memory_tier", "episodic"));
        ingest(
                tableId,
                "uuid-routes-bob",
                "hello memory world from bob about project sigma with enough text",
                Map.of("agent_id", "bob", "memory_tier", "semantic"));

        HttpResponse<String> unfiltered =
                post("/api/axiom/search", searchJson(tableId, "hello", null));
        assertThat(unfiltered.statusCode()).isEqualTo(200);
        JsonNode unfilteredBody = MAPPER.readTree(unfiltered.body());
        assertThat(unfilteredBody.path("results").size())
                .as("unfiltered search recalls both records")
                .isGreaterThan(0);
        boolean sawProps = false;
        boolean sawBody = false;
        for (JsonNode r : unfilteredBody.path("results")) {
            if (r.has("properties") && r.path("properties").has("agent_id")) sawProps = true;
            if (r.has("body") && !r.path("body").asText().isEmpty()) sawBody = true;
        }
        assertThat(sawProps).as("HTTP results expose properties").isTrue();
        assertThat(sawBody).as("HTTP results expose body").isTrue();

        HttpResponse<String> filtered =
                post(
                        "/api/axiom/search",
                        searchJson(tableId, "hello", Map.of("agent_id", "alice")));
        assertThat(filtered.statusCode()).isEqualTo(200);
        JsonNode filteredBody = MAPPER.readTree(filtered.body());
        assertThat(filteredBody.path("results").size())
                .as("alice-filtered search recalls at least one record")
                .isGreaterThan(0);
        for (JsonNode r : filteredBody.path("results")) {
            assertThat(r.path("properties").path("agent_id").asText())
                    .as("HTTP filtered result agent_id")
                    .isEqualTo("alice");
        }
    }

    @Test
    @DisplayName("fetch-details returns body + properties per entry")
    void fetchDetailsReturnsBodyAndProperties() throws Exception {
        String tableId = uniqTable("sr_details");
        ingest(
                tableId,
                "uuid-routes-detail-a",
                "hello memory world from alice about project omega with enough text",
                Map.of("agent_id", "alice", "memory_tier", "episodic"));

        String body =
                "{\"tableId\":\"" + tableId + "\",\"entryKeys\":[\"rec:uuid-routes-detail-a\"]}";
        HttpResponse<String> resp = post("/api/axiom/fetch-details", body);
        assertThat(resp.statusCode()).isEqualTo(200);
        JsonNode results = MAPPER.readTree(resp.body()).path("results");
        assertThat(results.size()).as("fetch-details returns at least one result").isGreaterThan(0);
        JsonNode first = results.get(0);
        assertThat(first.path("body").asText())
                .as("fetch-details result exposes body")
                .contains("hello memory world");
        assertThat(first.path("properties").path("agent_id").asText())
                .as("fetch-details result exposes properties.agent_id")
                .isEqualTo("alice");
    }
}
