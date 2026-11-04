package io.github.rawwise.e2e;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.backend.server.EmbeddedBackend;
import io.github.rawwise.frontend.api.FrontendServer;
import io.github.rawwise.frontend.backend.GrpcBackendClient;
import io.github.rawwise.frontend.config.FrontendOptions;
import io.github.rawwise.frontend.evaluator.DefaultQueryEvaluator;
import io.github.rawwise.frontend.session.FrontendSessionStore;
import io.github.rawwise.governance.config.RawWiseConfig;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Embedded Mode E2E Test")
class EmbeddedModeE2ETest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private static EmbeddedBackend embeddedBackend;
    private static FrontendServer frontendServer;
    private static GrpcBackendClient backendClient;
    private static FrontendSessionStore frontendSessionStore;
    private static String frontendBaseUrl;

    @TempDir static Path tempDir;

    @BeforeAll
    static void setUpAll() throws Exception {
        String warehousePath = tempDir.resolve("warehouse").toString();
        System.setProperty("rawwise.warehouse.path", warehousePath);
        System.setProperty("rawwise.storage.lmdb.initial-map-size-mb", "16");
        System.setProperty("rawwise.storage.lmdb.max-map-size-mb", "256");
        System.setProperty("rawwise.metastore.type", "inmemory");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("rawwise.metastore.type", "inmemory");
        RawWiseConfig config = RawWiseConfig.fromMap(configMap);

        embeddedBackend = new EmbeddedBackend(config);
        embeddedBackend.start();

        backendClient = GrpcBackendClient.forInProcess(EmbeddedBackend.BACKEND_INPROC_NAME, 60);

        frontendSessionStore = new FrontendSessionStore();
        DefaultQueryEvaluator evaluator = new DefaultQueryEvaluator();

        Map<String, Object> frontendConfig = new HashMap<>();
        frontendConfig.put("frontend.deployment.mode", "sidecar");
        frontendConfig.put("auth.enabled", false);
        frontendConfig.put("rawwise.frontend.port", 0);
        frontendConfig.put("rawwise.orchestrator.enabled", false);
        FrontendOptions options = new FrontendOptions(frontendConfig);

        frontendServer =
                new FrontendServer(
                        options,
                        frontendSessionStore,
                        backendClient,
                        evaluator,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);

        frontendServer.start();
        frontendBaseUrl = "http://localhost:" + frontendServer.getPort();
    }

    @AfterAll
    static void tearDownAll() {
        if (frontendServer != null) frontendServer.stop();
        if (backendClient != null) backendClient.close();
        if (embeddedBackend != null) embeddedBackend.close();
        if (frontendSessionStore != null) frontendSessionStore.close();
        System.clearProperty("rawwise.warehouse.path");
        System.clearProperty("rawwise.storage.lmdb.initial-map-size-mb");
        System.clearProperty("rawwise.storage.lmdb.max-map-size-mb");
        System.clearProperty("rawwise.metastore.type");
    }

    @Test
    @DisplayName("health endpoint returns UP")
    void healthReturnsUp() throws Exception {
        String body = get("/health");
        JsonNode json = MAPPER.readTree(body);
        assertThat(json.get("status").asText()).isEqualTo("UP");
    }

    @Test
    @DisplayName("submit SELECT query via embedded backend (InProcess)")
    void submitSelectQuery_returnsResult() throws Exception {
        String submitResp =
                postJson(
                        "/api/query",
                        "{\"sql\":\"SELECT 1 AS value\",\"options\":{\"collect\":\"true\"}}");
        JsonNode submitJson = MAPPER.readTree(submitResp);
        String jobId = submitJson.get("jobId").asText();
        assertThat(jobId).isNotEmpty();

        await().atMost(15, SECONDS)
                .untilAsserted(
                        () -> {
                            String statusResp = get("/api/query/" + jobId + "/status");
                            JsonNode statusJson = MAPPER.readTree(statusResp);
                            assertThat(statusJson.get("state").asText()).isEqualTo("SUCCEEDED");
                        });

        String resultResp = get("/api/query/" + jobId + "/result");
        JsonNode resultJson = MAPPER.readTree(resultResp);
        assertThat(resultJson.get("state").asText()).isEqualTo("SUCCEEDED");
        assertThat(resultJson.get("data").get("rows").size()).isEqualTo(1);
    }

    @Test
    @DisplayName("CREATE TABLE, INSERT, SELECT roundtrip via embedded backend (InProcess)")
    void createInsertSelectRoundtrip() throws Exception {
        String createResp =
                postJson(
                        "/api/query",
                        "{\"sql\":\"CREATE TABLE emb_test AS SELECT 1 AS id, 'hello' AS msg\",\"type\":\"DDL\"}");
        JsonNode createJson = MAPPER.readTree(createResp);
        String createJobId = createJson.get("jobId").asText();

        await().atMost(15, SECONDS)
                .untilAsserted(
                        () -> {
                            String statusResp = get("/api/query/" + createJobId + "/status");
                            assertThat(MAPPER.readTree(statusResp).get("state").asText())
                                    .isEqualTo("SUCCEEDED");
                        });

        String insertResp =
                postJson(
                        "/api/query",
                        "{\"sql\":\"INSERT INTO emb_test VALUES (2, 'world')\",\"type\":\"DML\"}");
        String insertJobId = MAPPER.readTree(insertResp).get("jobId").asText();

        await().atMost(15, SECONDS)
                .untilAsserted(
                        () -> {
                            String statusResp = get("/api/query/" + insertJobId + "/status");
                            assertThat(MAPPER.readTree(statusResp).get("state").asText())
                                    .isEqualTo("SUCCEEDED");
                        });

        String selectResp =
                postJson(
                        "/api/query",
                        "{\"sql\":\"SELECT * FROM emb_test ORDER BY id\",\"options\":{\"collect\":\"true\"}}");
        String selectJobId = MAPPER.readTree(selectResp).get("jobId").asText();

        await().atMost(15, SECONDS)
                .untilAsserted(
                        () -> {
                            String statusResp = get("/api/query/" + selectJobId + "/status");
                            assertThat(MAPPER.readTree(statusResp).get("state").asText())
                                    .isEqualTo("SUCCEEDED");
                        });

        String resultResp = get("/api/query/" + selectJobId + "/result");
        JsonNode resultJson = MAPPER.readTree(resultResp);
        assertThat(resultJson.get("state").asText()).isEqualTo("SUCCEEDED");
        assertThat(resultJson.get("data").get("rows").size()).isEqualTo(2);
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

    private String get(String path) throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder().uri(URI.create(frontendBaseUrl + path)).GET().build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }
}
