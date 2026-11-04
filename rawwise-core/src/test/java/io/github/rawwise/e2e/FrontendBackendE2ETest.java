package io.github.rawwise.e2e;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.backend.server.auth.QueryAuthorizer;
import io.github.rawwise.backend.server.job.JobEventLog;
import io.github.rawwise.backend.server.job.SparkJobScheduler;
import io.github.rawwise.backend.server.load.BackendLoadInfo;
import io.github.rawwise.backend.server.load.BackpressureManager;
import io.github.rawwise.backend.server.load.LoadCollector;
import io.github.rawwise.backend.server.service.QueryServiceImpl;
import io.github.rawwise.backend.server.session.InMemorySessionStore;
import io.github.rawwise.frontend.api.FrontendServer;
import io.github.rawwise.frontend.backend.GrpcBackendClient;
import io.github.rawwise.frontend.config.FrontendOptions;
import io.github.rawwise.frontend.evaluator.DefaultQueryEvaluator;
import io.github.rawwise.frontend.session.FrontendSessionStore;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Frontend-Backend E2E Test")
class FrontendBackendE2ETest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private static SparkSession spark;
    private static Server grpcServer;
    private static SparkJobScheduler scheduler;
    private static InMemorySessionStore sessionStore;
    private static FrontendServer frontendServer;
    private static GrpcBackendClient backendClient;
    private static FrontendSessionStore frontendSessionStore;
    private static int backendPort;
    private static String frontendBaseUrl;

    @BeforeAll
    static void setUpAll() throws Exception {
        deleteDirectory(new File("/tmp/spark-warehouse-e2e-test"));

        spark =
                SparkSession.builder()
                        .master("local[2]")
                        .appName("FrontendBackendE2ETest")
                        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-e2e-test")
                        .config("spark.ui.enabled", "false")
                        .config("spark.sql.adaptive.enabled", "false")
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

        grpcServer = ServerBuilder.forPort(0).addService(queryService).build().start();
        backendPort = grpcServer.getPort();

        backendClient = new GrpcBackendClient("localhost", backendPort, 60);

        frontendSessionStore = new FrontendSessionStore();
        DefaultQueryEvaluator evaluator = new DefaultQueryEvaluator();

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
        int actualFrontendPort = frontendServer.getPort();
        frontendBaseUrl = "http://localhost:" + actualFrontendPort;
    }

    @AfterAll
    static void tearDownAll() {
        if (frontendServer != null) frontendServer.stop();
        if (backendClient != null) backendClient.close();
        if (grpcServer != null) grpcServer.shutdown();
        if (scheduler != null) scheduler.shutdown();
        if (frontendSessionStore != null) frontendSessionStore.close();
        if (spark != null) spark.stop();
    }

    @Test
    @DisplayName("submit SELECT query and retrieve result")
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
    @DisplayName("submit DDL and DML then SELECT to verify roundtrip")
    void submitDDLAndDML_querySucceeds() throws Exception {
        String createResp =
                postJson(
                        "/api/query",
                        "{\"sql\":\"CREATE TABLE t_e2e_test AS SELECT 1 AS id\",\"type\":\"DDL\"}");
        JsonNode createJson = MAPPER.readTree(createResp);
        String createJobId = createJson.get("jobId").asText();

        await().atMost(15, SECONDS)
                .untilAsserted(
                        () -> {
                            String statusResp = get("/api/query/" + createJobId + "/status");
                            JsonNode statusJson = MAPPER.readTree(statusResp);
                            assertThat(statusJson.get("state").asText()).isEqualTo("SUCCEEDED");
                        });

        String insertResp =
                postJson(
                        "/api/query",
                        "{\"sql\":\"INSERT INTO t_e2e_test VALUES (2)\",\"type\":\"DML\"}");
        JsonNode insertJson = MAPPER.readTree(insertResp);
        String insertJobId = insertJson.get("jobId").asText();

        await().atMost(15, SECONDS)
                .untilAsserted(
                        () -> {
                            String statusResp = get("/api/query/" + insertJobId + "/status");
                            JsonNode statusJson = MAPPER.readTree(statusResp);
                            assertThat(statusJson.get("state").asText()).isEqualTo("SUCCEEDED");
                        });

        String selectResp =
                postJson(
                        "/api/query",
                        "{\"sql\":\"SELECT * FROM t_e2e_test\",\"options\":{\"collect\":\"true\"}}");
        JsonNode selectJson = MAPPER.readTree(selectResp);
        String selectJobId = selectJson.get("jobId").asText();

        await().atMost(15, SECONDS)
                .untilAsserted(
                        () -> {
                            String statusResp = get("/api/query/" + selectJobId + "/status");
                            JsonNode statusJson = MAPPER.readTree(statusResp);
                            assertThat(statusJson.get("state").asText()).isEqualTo("SUCCEEDED");
                        });

        String resultResp = get("/api/query/" + selectJobId + "/result");
        JsonNode resultJson = MAPPER.readTree(resultResp);

        assertThat(resultJson.get("state").asText()).isEqualTo("SUCCEEDED");
        assertThat(resultJson.get("data").get("rows").size()).isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("reject DELETE without WHERE clause")
    void submitSqlNotAllowed_rejected() throws Exception {
        int statusCode = postJsonGetStatus("/api/query", "{\"sql\":\"DELETE FROM some_table\"}");
        assertThat(statusCode).isEqualTo(400);
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

    private int postJsonGetStatus(String path, String body) throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(frontendBaseUrl + path))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        return response.statusCode();
    }

    private String get(String path) throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder().uri(URI.create(frontendBaseUrl + path)).GET().build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

    private static void deleteDirectory(File dir) {
        if (!dir.exists()) return;
        try {
            Files.walk(dir.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException ignored) {
        }
    }
}
