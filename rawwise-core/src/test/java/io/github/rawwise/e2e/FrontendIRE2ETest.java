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
import io.github.rawwise.common.catalog.Catalog;
import io.github.rawwise.common.catalog.MetastoreCatalog;
import io.github.rawwise.common.catalog.MetastoreCatalogConfig;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.frontend.api.FrontendServer;
import io.github.rawwise.frontend.backend.GrpcBackendClient;
import io.github.rawwise.frontend.config.FrontendOptions;
import io.github.rawwise.frontend.evaluator.DefaultQueryEvaluator;
import io.github.rawwise.frontend.ir.IrRuntime;
import io.github.rawwise.frontend.session.FrontendSessionStore;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Frontend IR E2E — HTTP -> Frontend -> Backend -> LMDB")
class FrontendIRE2ETest {

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
    private static int backendPort;
    private static String frontendBaseUrl;

    @TempDir static Path tempDir;

    @BeforeAll
    static void setUpAll() throws Exception {
        try {
            StorageRegistry.instance().register(new LocalStorageProvider());
        } catch (Exception ignored) {
        }

        deleteDirectory(new File("/tmp/spark-warehouse-frontend-e2e"));

        String warehousePath = tempDir.resolve("warehouse").toString();
        System.setProperty("rawwise.warehouse.path", warehousePath);
        System.setProperty("rawwise.storage.lmdb.initial-map-size-mb", "16");
        System.setProperty("rawwise.storage.lmdb.max-map-size-mb", "256");
        System.setProperty("rawwise.metastore.type", "inmemory");

        spark =
                SparkSession.builder()
                        .master("local[2]")
                        .appName("FrontendIRE2ETest")
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

        grpcServer = ServerBuilder.forPort(0).addService(queryService).build().start();
        backendPort = grpcServer.getPort();

        backendClient = new GrpcBackendClient("localhost", backendPort, 60);
        frontendSessionStore = new FrontendSessionStore();
        DefaultQueryEvaluator evaluator = new DefaultQueryEvaluator();

        Metastore metastore = new InMemoryMetastore();
        MetastoreCatalogConfig catalogConfig =
                MetastoreCatalogConfig.builder().store(metastore).basePath(warehousePath).build();
        Catalog catalog = new MetastoreCatalog(catalogConfig);
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
        System.clearProperty("rawwise.warehouse.path");
        System.clearProperty("rawwise.storage.lmdb.initial-map-size-mb");
        System.clearProperty("rawwise.storage.lmdb.max-map-size-mb");
        System.clearProperty("rawwise.metastore.type");
    }

    private String uniqueTable() {
        return "e2e_ft_" + UUID.randomUUID().toString().replace("-", "");
    }

    private static class QueryResult {
        final String state;
        final int rowCount;
        final JsonNode rows;

        QueryResult(String state, int rowCount, JsonNode rows) {
            this.state = state;
            this.rowCount = rowCount;
            this.rows = rows;
        }
    }

    private QueryResult executeQuery(String sql) throws Exception {
        return executeQuery(sql, "DML");
    }

    private QueryResult executeQuery(String sql, String type) throws Exception {
        String body =
                MAPPER.writeValueAsString(
                        Map.of("sql", sql, "type", type, "options", Map.of("collect", "true")));
        String submitResp = postJson("/api/query", body);
        JsonNode submitJson = MAPPER.readTree(submitResp);
        String jobId = submitJson.get("jobId").asText();
        assertThat(jobId).isNotEmpty();

        if (submitJson.has("state") && !"PENDING".equals(submitJson.get("state").asText())) {
            String state = submitJson.get("state").asText();
            JsonNode data = submitJson.get("data");
            int rowCount = data != null && data.has("rowCount") ? data.get("rowCount").asInt() : 0;
            JsonNode rows =
                    data != null && data.has("rows") ? data.get("rows") : MAPPER.createArrayNode();
            return new QueryResult(state, rowCount, rows);
        }

        await().atMost(30, SECONDS)
                .untilAsserted(
                        () -> {
                            String statusResp = get("/api/query/" + jobId + "/status");
                            String state = MAPPER.readTree(statusResp).get("state").asText();
                            assertThat(state).isIn("SUCCEEDED", "FAILED");
                        });

        String resultResp = get("/api/query/" + jobId + "/result");
        JsonNode resultJson = MAPPER.readTree(resultResp);
        String state = resultJson.get("state").asText();
        JsonNode data = resultJson.get("data");
        int rowCount = data != null && data.has("rowCount") ? data.get("rowCount").asInt() : 0;
        JsonNode rows =
                data != null && data.has("rows") ? data.get("rows") : MAPPER.createArrayNode();
        return new QueryResult(state, rowCount, rows);
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

    @Nested
    @DisplayName("scenario group A: CRUD via Frontend HTTP")
    class CrudViaFrontendTests {

        @Test
        @DisplayName("A1: CREATE TABLE + INSERT + SELECT — data correctly read back after write")
        void createInsertSelect() throws Exception {
            String table = uniqueTable();
            executeQuery(
                    "CREATE TABLE "
                            + table
                            + " (id INT, name STRING) USING rawwise TBLPROPERTIES ('primary-key'='id')",
                    "DDL");

            executeQuery(
                    "INSERT INTO " + table + " VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')");

            QueryResult result = executeQuery("SELECT * FROM " + table + " ORDER BY id");

            assertThat(result.state).isEqualTo("SUCCEEDED");
            assertThat(result.rowCount).isEqualTo(3);
            assertThat(result.rows.get(0).get(0).asInt()).isEqualTo(1);
            assertThat(result.rows.get(0).get(1).asText()).isEqualTo("alpha");
            assertThat(result.rows.get(2).get(0).asInt()).isEqualTo(3);
        }

        @Test
        @DisplayName("A2: SELECT columnproject — only returns queried columns")
        void selectWithProjection() throws Exception {
            String table = uniqueTable();
            executeQuery(
                    "CREATE TABLE "
                            + table
                            + " (id INT, name STRING, score DOUBLE) USING rawwise TBLPROPERTIES ('primary-key'='id')",
                    "DDL");
            executeQuery("INSERT INTO " + table + " VALUES (1, 'alpha', 95.5), (2, 'beta', 87.0)");

            QueryResult result = executeQuery("SELECT name FROM " + table + " WHERE id = 2");

            assertThat(result.state).isEqualTo("SUCCEEDED");
            assertThat(result.rows.get(0).size()).isEqualTo(1);
            assertThat(result.rows.get(0).get(0).asText()).isEqualTo("beta");
        }

        @Test
        @DisplayName("A3: WHERE pk = ? point query — primary key point query returns correct data")
        void pointQueryByPk() throws Exception {
            String table = uniqueTable();
            executeQuery(
                    "CREATE TABLE "
                            + table
                            + " (id INT, val STRING) USING rawwise TBLPROPERTIES ('primary-key'='id')",
                    "DDL");
            executeQuery("INSERT INTO " + table + " VALUES (42, 'answer')");

            QueryResult result = executeQuery("SELECT * FROM " + table + " WHERE id = 42");

            assertThat(result.state).isEqualTo("SUCCEEDED");
            assertThat(result.rows.get(0).get(0).asInt()).isEqualTo(42);
            assertThat(result.rows.get(0).get(1).asText()).isEqualTo("answer");
        }

        @Test
        @DisplayName("A4: COUNT(*) — aggregate returns correct row count")
        void countAll() throws Exception {
            String table = uniqueTable();
            executeQuery(
                    "CREATE TABLE "
                            + table
                            + " (id INT, val STRING) USING rawwise TBLPROPERTIES ('primary-key'='id')",
                    "DDL");
            executeQuery(
                    "INSERT INTO "
                            + table
                            + " VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')");

            QueryResult result = executeQuery("SELECT COUNT(*) FROM " + table);

            assertThat(result.state).isEqualTo("SUCCEEDED");
            assertThat(result.rows.get(0).get(0).asLong()).isEqualTo(5);
        }
    }

    @Nested
    @DisplayName("scenario group B: FastPath projectcorrect")
    class FastPathRegressionTests {

        @Test
        @DisplayName("B1: SELECT balance WHERE id=? — balance column non- id column")
        void selectBalanceColumnOnly() throws Exception {
            String table = uniqueTable();
            executeQuery(
                    "CREATE TABLE "
                            + table
                            + " (id INT, balance INT) USING rawwise TBLPROPERTIES ('primary-key'='id')",
                    "DDL");
            executeQuery("INSERT INTO " + table + " VALUES (1000, 0)");

            QueryResult result = executeQuery("SELECT balance FROM " + table + " WHERE id = 1000");

            assertThat(result.state).isEqualTo("SUCCEEDED");
            assertThat(result.rows.get(0).size()).isEqualTo(1);
            assertThat(result.rows.get(0).get(0).asInt()).isEqualTo(0);
        }

        @Test
        @DisplayName("B2: SELECT id, balance WHERE id=? — columnallreturns correct value")
        void selectBothColumns() throws Exception {
            String table = uniqueTable();
            executeQuery(
                    "CREATE TABLE "
                            + table
                            + " (id INT, balance INT) USING rawwise TBLPROPERTIES ('primary-key'='id')",
                    "DDL");
            executeQuery("INSERT INTO " + table + " VALUES (1000, 500)");

            QueryResult result =
                    executeQuery("SELECT id, balance FROM " + table + " WHERE id = 1000");

            assertThat(result.state).isEqualTo("SUCCEEDED");
            assertThat(result.rows.get(0).size()).isEqualTo(2);
            assertThat(result.rows.get(0).get(0).asInt()).isEqualTo(1000);
            assertThat(result.rows.get(0).get(1).asInt()).isEqualTo(500);
        }
    }

    @Nested
    @DisplayName("scenario group C: DDL operations")
    class DdlTests {

        @Test
        @DisplayName("C1: DROP TABLE after SELECT —")
        void dropTableThenSelectFails() throws Exception {
            String table = uniqueTable();
            executeQuery(
                    "CREATE TABLE "
                            + table
                            + " (id INT) USING rawwise TBLPROPERTIES ('primary-key'='id')",
                    "DDL");
            executeQuery("INSERT INTO " + table + " VALUES (1)");
            executeQuery("DROP TABLE " + table, "DDL");

            String body =
                    MAPPER.writeValueAsString(
                            Map.of(
                                    "sql",
                                    "SELECT * FROM " + table,
                                    "type",
                                    "DML",
                                    "options",
                                    Map.of("collect", "true")));
            String submitResp = postJson("/api/query", body);
            JsonNode submitJson = MAPPER.readTree(submitResp);
            String jobId = submitJson.get("jobId").asText();

            await().atMost(30, SECONDS)
                    .untilAsserted(
                            () -> {
                                String statusResp = get("/api/query/" + jobId + "/status");
                                String state = MAPPER.readTree(statusResp).get("state").asText();
                                assertThat(state).isEqualTo("FAILED");
                            });
        }

        @Test
        @DisplayName("C2: multi-tableindependentwrite — no data cross-interference")
        void twoTablesIndependent() throws Exception {
            String table1 = uniqueTable();
            String table2 = uniqueTable();
            executeQuery(
                    "CREATE TABLE "
                            + table1
                            + " (id INT, val STRING) USING rawwise TBLPROPERTIES ('primary-key'='id')",
                    "DDL");
            executeQuery(
                    "CREATE TABLE "
                            + table2
                            + " (id INT, val STRING) USING rawwise TBLPROPERTIES ('primary-key'='id')",
                    "DDL");
            executeQuery("INSERT INTO " + table1 + " VALUES (1, 'hello')");
            executeQuery("INSERT INTO " + table2 + " VALUES (1, 'world')");

            QueryResult r1 = executeQuery("SELECT val FROM " + table1 + " WHERE id = 1");
            QueryResult r2 = executeQuery("SELECT val FROM " + table2 + " WHERE id = 1");

            assertThat(r1.rows.get(0).get(0).asText()).isEqualTo("hello");
            assertThat(r2.rows.get(0).get(0).asText()).isEqualTo("world");
        }
    }
}
