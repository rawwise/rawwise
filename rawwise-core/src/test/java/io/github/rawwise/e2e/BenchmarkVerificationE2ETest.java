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
import io.github.rawwise.frontend.backend.GrpcExecutorStub;
import io.github.rawwise.frontend.config.FrontendOptions;
import io.github.rawwise.frontend.evaluator.DefaultQueryEvaluator;
import io.github.rawwise.frontend.ir.IrRuntime;
import io.github.rawwise.frontend.session.FrontendSessionStore;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.MetastoreFactory;
import io.github.rawwise.metastore.MetastoreRegistry;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * ChBenchmark data accuracyandconcurrent transactionsverify E2E.
 *
 * <p>cover 5 item (and 05-verify-benchmark.sh for):
 *
 * <ol>
 *   <li>data : row consistency + PK uniqueness
 *   <li>transactionatomic : COMMIT persisted + payment tableatomiccommit
 *   <li>concurrentconflict: 8 concurrent UPDATE same PK no deadlock + counter consistency
 *   <li>Read-After-Write: INSERT->SELECT + UPDATE->SELECT + DELETE->SELECT
 *   <li>tableconsistency: payment table ytd/balance
 * </ol>
 *
 * <p>real HTTP -> Frontend (IR) -> FastPath -> ExecutorPointQueryServiceImpl -> LMDB .
 */
@DisplayName("ChBenchmark data accuracyandconcurrent transactionsverify E2E")
class BenchmarkVerificationE2ETest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    private static SparkSession spark;
    private static Server backendGrpcServer;
    private static Server executorInProcessServer;
    private static ManagedChannel executorInProcessChannel;
    private static SparkJobScheduler scheduler;
    private static InMemorySessionStore sessionStore;
    private static FrontendServer frontendServer;
    private static GrpcBackendClient backendClient;
    private static FrontendSessionStore frontendSessionStore;
    private static int backendPort;
    private static String frontendBaseUrl;
    private static MetastoreFactory previousInmemoryFactory;
    private static InMemoryMetastore sharedMetastore;

    @TempDir static Path tempDir;

    @BeforeAll
    static void setUpAll() throws Exception {
        try {
            StorageRegistry.instance().register(new LocalStorageProvider());
        } catch (Exception ignored) {
        }

        deleteDirectory(new File("/tmp/spark-warehouse-bench-verify"));

        String warehousePath = tempDir.resolve("warehouse").toString();
        System.setProperty("rawwise.warehouse.path", warehousePath);
        System.setProperty("rawwise.storage.lmdb.initial-map-size-mb", "16");
        System.setProperty("rawwise.storage.lmdb.max-map-size-mb", "256");
        System.setProperty("rawwise.metastore.type", "inmemory");

        sharedMetastore = new InMemoryMetastore();
        MetastoreRegistry registry = MetastoreRegistry.getInstance();
        previousInmemoryFactory =
                registry.isRegistered("inmemory")
                        ? new MetastoreFactory() {
                            @Override
                            public String kind() {
                                return "inmemory";
                            }

                            @Override
                            public Metastore create(RawWiseConfig cfg) {
                                return new InMemoryMetastore();
                            }
                        }
                        : null;
        registry.register(
                new MetastoreFactory() {
                    @Override
                    public String kind() {
                        return "inmemory";
                    }

                    @Override
                    public Metastore create(RawWiseConfig cfg) {
                        return sharedMetastore;
                    }
                });

        spark =
                SparkSession.builder()
                        .master("local[2]")
                        .appName("BenchmarkVerificationE2ETest")
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

        backendGrpcServer = ServerBuilder.forPort(0).addService(queryService).build().start();
        backendPort = backendGrpcServer.getPort();

        backendClient = new GrpcBackendClient("localhost", backendPort, 60);
        frontendSessionStore = new FrontendSessionStore();
        DefaultQueryEvaluator evaluator = new DefaultQueryEvaluator();

        Metastore metastore = sharedMetastore;
        MetastoreCatalogConfig catalogConfig =
                MetastoreCatalogConfig.builder().store(metastore).basePath(warehousePath).build();
        Catalog catalog = new MetastoreCatalog(catalogConfig);
        SnapshotManager snapshotManager = new SnapshotManager(catalog, metastore);

        IrRuntime irRuntime = IrRuntime.create(catalog, snapshotManager);

        Map<String, Object> execCfg = new HashMap<>();
        execCfg.put("rawwise.catalog.warehouse-path", warehousePath);
        execCfg.put("rawwise.storage.lmdb.initial-map-size-mb", 16L);
        execCfg.put("rawwise.storage.lmdb.max-map-size-mb", 256L);
        RawWiseConfig execConfig = RawWiseConfig.fromMap(execCfg);
        ExecutorPointQueryServiceImpl executorService =
                new ExecutorPointQueryServiceImpl(execConfig);

        String inProcName = "bench-verify-exec-" + UUID.randomUUID();
        executorInProcessServer =
                InProcessServerBuilder.forName(inProcName)
                        .directExecutor()
                        .addService(executorService)
                        .build()
                        .start();
        executorInProcessChannel =
                InProcessChannelBuilder.forName(inProcName).directExecutor().build();
        GrpcExecutorStub execStub = new GrpcExecutorStub(executorInProcessChannel, 30);
        irRuntime.fastPathExecutor().registerExecutor("driver", execStub);
        irRuntime.fastPathExecutor().registerShardExecutor(0, "driver");

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
        if (executorInProcessChannel != null) executorInProcessChannel.shutdownNow();
        if (executorInProcessServer != null) executorInProcessServer.shutdownNow();
        if (backendGrpcServer != null) backendGrpcServer.shutdown();
        if (scheduler != null) scheduler.shutdown();
        if (frontendSessionStore != null) frontendSessionStore.close();
        if (spark != null) spark.stop();
        if (previousInmemoryFactory != null) {
            MetastoreRegistry.getInstance().register(previousInmemoryFactory);
        }
        System.clearProperty("rawwise.warehouse.path");
        System.clearProperty("rawwise.storage.lmdb.initial-map-size-mb");
        System.clearProperty("rawwise.storage.lmdb.max-map-size-mb");
        System.clearProperty("rawwise.metastore.type");
    }

    // === Helpers ===

    private String uniqueTable() {
        return "bench_v_" + UUID.randomUUID().toString().replace("-", "");
    }

    private static void deleteDirectory(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File f : files) {
                    if (f.isDirectory()) deleteDirectory(f);
                    else f.delete();
                }
            }
            dir.delete();
        }
    }

    private static class QueryResult {
        final String state;
        final int rowCount;
        final JsonNode rows;
        final JsonNode columns;

        QueryResult(String state, int rowCount, JsonNode rows, JsonNode columns) {
            this.state = state;
            this.rowCount = rowCount;
            this.rows = rows;
            this.columns = columns;
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

        // BEGIN/COMMIT/ROLLBACK cancan jobId, directly state
        if (!submitJson.has("jobId")) {
            String state = submitJson.has("state") ? submitJson.get("state").asText() : "SUCCEEDED";
            return new QueryResult(state, 0, MAPPER.createArrayNode(), MAPPER.createArrayNode());
        }

        String jobId = submitJson.get("jobId").asText();
        assertThat(jobId).isNotEmpty();

        if (submitJson.has("state") && !"PENDING".equals(submitJson.get("state").asText())) {
            String state = submitJson.get("state").asText();
            JsonNode data = submitJson.get("data");
            int rowCount = data != null && data.has("rowCount") ? data.get("rowCount").asInt() : 0;
            JsonNode rows =
                    data != null && data.has("rows") ? data.get("rows") : MAPPER.createArrayNode();
            JsonNode columns =
                    data != null && data.has("columns")
                            ? data.get("columns")
                            : MAPPER.createArrayNode();
            return new QueryResult(state, rowCount, rows, columns);
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
        JsonNode columns =
                data != null && data.has("columns")
                        ? data.get("columns")
                        : MAPPER.createArrayNode();
        return new QueryResult(state, rowCount, rows, columns);
    }

    private String postJson(String path, String body) throws Exception {
        HttpResponse<String> resp =
                HTTP.send(
                        HttpRequest.newBuilder()
                                .uri(URI.create(frontendBaseUrl + path))
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(body))
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        return resp.body();
    }

    private String get(String path) throws Exception {
        HttpResponse<String> resp =
                HTTP.send(
                        HttpRequest.newBuilder()
                                .uri(URI.create(frontendBaseUrl + path))
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        return resp.body();
    }

    /** from SELECT inextract first row first columnofstring value */
    private String scalar(QueryResult result) {
        if (result.rows == null || result.rows.isEmpty() || !result.rows.isArray()) return "";
        JsonNode firstRow = result.rows.get(0);
        if (firstRow.isArray() && firstRow.size() > 0) return firstRow.get(0).asText();
        if (firstRow.isObject()) {
            var fields = firstRow.fields();
            if (fields.hasNext()) return fields.next().getValue().asText();
        }
        return firstRow.asText();
    }

    // === 1. data verify ===

    @Test
    @DisplayName("V1: data — row consistency + PK uniqueness")
    void testDataIntegrity() throws Exception {
        String table = uniqueTable();
        executeQuery(
                "CREATE TABLE "
                        + table
                        + " (id INT, name STRING) "
                        + "USING rawwise TBLPROPERTIES ('primary-key'='id')",
                "DDL");

        // 10 row
        for (int i = 1; i <= 10; i++) {
            executeQuery("INSERT INTO " + table + " VALUES (" + i + ", 'name-" + i + "')");
        }

        // verifyeachrowallcan SELECT to(COUNT(*), avoid Spark LOGICAL_PLAN instance isolation
        // issue)
        for (int i = 1; i <= 10; i++) {
            QueryResult r = executeQuery("SELECT name FROM " + table + " WHERE id = " + i);
            assertThat(r.state).as("Row " + i + " state").isEqualTo("SUCCEEDED");
            assertThat(r.rowCount).as("Row " + i + " 应存在").isEqualTo(1);
            assertThat(scalar(r)).as("Row " + i + " name").isEqualTo("name-" + i);
        }

        // verify PK uniqueness: duplicate PK shouldcover(duplicaterow)
        executeQuery("INSERT INTO " + table + " VALUES (1, 'updated-1')");

        // id=1 should beupdateafterofvalue
        QueryResult select1 = executeQuery("SELECT name FROM " + table + " WHERE id = 1");
        assertThat(scalar(select1)).as("id=1 的 name 应为 updated-1").isEqualTo("updated-1");

        // id=2 should in valueunchanged(PK coverdoes not affectits row)
        QueryResult select2 = executeQuery("SELECT name FROM " + table + " WHERE id = 2");
        assertThat(scalar(select2)).as("id=2 的 name 应不变").isEqualTo("name-2");
    }

    // === 2. transactionatomic verify ===

    @Test
    @DisplayName("V2: transactionatomic — payment table BEGIN/COMMIT atomiccommit")
    void testTransactionAtomicity() throws Exception {
        // create payment transaction of 3 table
        String wh = uniqueTable() + "_wh";
        String dist = uniqueTable() + "_dist";
        String cust = uniqueTable() + "_cust";

        executeQuery(
                "CREATE TABLE "
                        + wh
                        + " (w_id INT, w_ytd DOUBLE) "
                        + "USING rawwise TBLPROPERTIES ('primary-key'='w_id')",
                "DDL");
        executeQuery(
                "CREATE TABLE "
                        + dist
                        + " (d_w_id INT, d_id INT, d_ytd DOUBLE) "
                        + "USING rawwise TBLPROPERTIES ('primary-key'='d_w_id,d_id')",
                "DDL");
        executeQuery(
                "CREATE TABLE "
                        + cust
                        + " (c_w_id INT, c_d_id INT, c_id INT, c_balance DOUBLE) "
                        + "USING rawwise TBLPROPERTIES ('primary-key'='c_w_id,c_d_id,c_id')",
                "DDL");

        // data
        executeQuery("INSERT INTO " + wh + " VALUES (1, 1000.0)");
        executeQuery("INSERT INTO " + dist + " VALUES (1, 1, 500.0)");
        executeQuery("INSERT INTO " + cust + " VALUES (1, 1, 1, 200.0)");

        // execute payment transaction: BEGIN -> 3xUPDATE -> COMMIT
        executeQuery("BEGIN");
        executeQuery("UPDATE " + wh + " SET w_ytd = 1234.0 WHERE w_id = 1");
        executeQuery("UPDATE " + dist + " SET d_ytd = 1234.0 WHERE d_w_id = 1 AND d_id = 1");
        executeQuery(
                "UPDATE "
                        + cust
                        + " SET c_balance = 1234.0 WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1");
        executeQuery("COMMIT");

        // verify tableallupdate
        QueryResult whResult = executeQuery("SELECT w_ytd FROM " + wh + " WHERE w_id = 1");
        assertThat(scalar(whResult)).as("warehouse.w_ytd 应为 1234.0").isEqualTo("1234.0");

        QueryResult distResult =
                executeQuery("SELECT d_ytd FROM " + dist + " WHERE d_w_id = 1 AND d_id = 1");
        assertThat(scalar(distResult)).as("district.d_ytd 应为 1234.0").isEqualTo("1234.0");

        QueryResult custResult =
                executeQuery(
                        "SELECT c_balance FROM "
                                + cust
                                + " WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1");
        assertThat(scalar(custResult)).as("customer.c_balance 应为 1234.0").isEqualTo("1234.0");
    }

    // === 3. concurrentconflictverify ===

    @Test
    @DisplayName("V3: concurrentconflict — 8 concurrent UPDATE same PK no deadlock")
    void testConcurrentUpdateSamePk() throws Exception {
        String table = uniqueTable();
        executeQuery(
                "CREATE TABLE "
                        + table
                        + " (id INT, counter INT, val STRING) "
                        + "USING rawwise TBLPROPERTIES ('primary-key'='id')",
                "DDL");
        executeQuery("INSERT INTO " + table + " VALUES (1, 0, 'init')");

        // 8 itemconcurrent UPDATE samerow
        int N = 8;
        ExecutorService pool = Executors.newFixedThreadPool(N);
        ConcurrentHashMap<Integer, String> results = new ConcurrentHashMap<>();

        var futures = new java.util.ArrayList<Future<?>>(); // Use wildcard Future<?>
        for (int i = 0; i < N; i++) {
            final int idx = i;
            futures.add(
                    pool.submit(
                            () -> {
                                try {
                                    QueryResult r =
                                            executeQuery(
                                                    "UPDATE "
                                                            + table
                                                            + " SET counter = "
                                                            + idx
                                                            + ", val = 'writer-"
                                                            + idx
                                                            + "' WHERE id = 1");
                                    results.put(idx, r.state);
                                } catch (Exception e) {
                                    results.put(idx, "ERROR: " + e.getMessage());
                                }
                            }));
        }
        for (Future<?> f : futures) f.get(120, SECONDS);
        pool.shutdown();

        // itemsuccess(no deadlock)
        long successCount = results.values().stream().filter("SUCCEEDED"::equals).count();
        assertThat(successCount)
                .as("至少 1 个并发 UPDATE 成功（无死锁），实际成功: " + successCount)
                .isGreaterThanOrEqualTo(1);

        // valueshould be item writer ofvalue(counter 0-7 within range)
        QueryResult finalResult =
                executeQuery("SELECT counter, val FROM " + table + " WHERE id = 1");
        assertThat(finalResult.state).isEqualTo("SUCCEEDED");
        assertThat(finalResult.rowCount).as("最终行应存在").isEqualTo(1);
        JsonNode row = finalResult.rows.get(0);
        String counterStr = row.isArray() ? row.get(0).asText() : row.get("counter").asText();
        String valStr = row.isArray() ? row.get(1).asText() : row.get("val").asText();

        int counter = Integer.parseInt(counterStr);
        assertThat(counter).as("counter 应在 0-" + (N - 1) + " 范围内").isBetween(0, N - 1);
        assertThat(valStr).as("val 应匹配 counter (原子写入)").isEqualTo("writer-" + counter);
    }

    // === 4. Read-After-Write consistency ===

    @Test
    @DisplayName("V4: Read-After-Write — INSERT->SELECT + UPDATE->SELECT + DELETE->SELECT")
    void testReadAfterWrite() throws Exception {
        String table = uniqueTable();
        executeQuery(
                "CREATE TABLE "
                        + table
                        + " (id INT, data STRING) "
                        + "USING rawwise TBLPROPERTIES ('primary-key'='id')",
                "DDL");

        // INSERT -> SELECT
        for (int i = 1; i <= 5; i++) {
            String val = "raw-test-" + i;
            executeQuery("INSERT INTO " + table + " VALUES (" + i + ", '" + val + "')");
            QueryResult r = executeQuery("SELECT data FROM " + table + " WHERE id = " + i);
            assertThat(scalar(r)).as("INSERT→SELECT Row " + i + " 一致").isEqualTo(val);
        }

        // UPDATE -> SELECT
        for (int i = 1; i <= 3; i++) {
            String newVal = "updated-" + i;
            executeQuery("UPDATE " + table + " SET data = '" + newVal + "' WHERE id = " + i);
            QueryResult r = executeQuery("SELECT data FROM " + table + " WHERE id = " + i);
            assertThat(scalar(r)).as("UPDATE→SELECT Row " + i + " 一致").isEqualTo(newVal);
        }

        // DELETE -> SELECT (shouldreturns empty)
        executeQuery("DELETE FROM " + table + " WHERE id = 5");
        QueryResult r = executeQuery("SELECT data FROM " + table + " WHERE id = 5");
        assertThat(r.rowCount).as("DELETE→SELECT Row 5 应返回 0 行").isEqualTo(0);
    }

    // === 5. tableconsistency ===

    @Test
    @DisplayName("V5: tableconsistency — payment transaction table ytd/balance")
    void testCrossTableConsistency() throws Exception {
        String wh = uniqueTable() + "_wh";
        String dist = uniqueTable() + "_dist";
        String cust = uniqueTable() + "_cust";

        executeQuery(
                "CREATE TABLE "
                        + wh
                        + " (w_id INT, w_ytd DOUBLE) "
                        + "USING rawwise TBLPROPERTIES ('primary-key'='w_id')",
                "DDL");
        executeQuery(
                "CREATE TABLE "
                        + dist
                        + " (d_w_id INT, d_id INT, d_ytd DOUBLE) "
                        + "USING rawwise TBLPROPERTIES ('primary-key'='d_w_id,d_id')",
                "DDL");
        executeQuery(
                "CREATE TABLE "
                        + cust
                        + " (c_w_id INT, c_d_id INT, c_id INT, c_balance DOUBLE) "
                        + "USING rawwise TBLPROPERTIES ('primary-key'='c_w_id,c_d_id,c_id')",
                "DDL");

        // data
        executeQuery("INSERT INTO " + wh + " VALUES (2, 300000.0)");
        executeQuery("INSERT INTO " + dist + " VALUES (2, 2, 30000.0)");
        executeQuery("INSERT INTO " + cust + " VALUES (2, 2, 2, 0.0)");

        // payment transaction
        String paymentAmt = "888.88";
        executeQuery("BEGIN");
        executeQuery("UPDATE " + wh + " SET w_ytd = " + paymentAmt + " WHERE w_id = 2");
        executeQuery(
                "UPDATE " + dist + " SET d_ytd = " + paymentAmt + " WHERE d_w_id = 2 AND d_id = 2");
        executeQuery(
                "UPDATE "
                        + cust
                        + " SET c_balance = "
                        + paymentAmt
                        + " WHERE c_w_id = 2 AND c_d_id = 2 AND c_id = 2");
        executeQuery("COMMIT");

        // verify table
        assertThat(scalar(executeQuery("SELECT w_ytd FROM " + wh + " WHERE w_id = 2")))
                .as("warehouse.w_ytd 应联动更新为 " + paymentAmt)
                .isEqualTo(paymentAmt);
        assertThat(
                        scalar(
                                executeQuery(
                                        "SELECT d_ytd FROM "
                                                + dist
                                                + " WHERE d_w_id = 2 AND d_id = 2")))
                .as("district.d_ytd 应联动更新为 " + paymentAmt)
                .isEqualTo(paymentAmt);
        assertThat(
                        scalar(
                                executeQuery(
                                        "SELECT c_balance FROM "
                                                + cust
                                                + " WHERE c_w_id = 2 AND c_d_id = 2 AND c_id = 2")))
                .as("customer.c_balance 应联动更新为 " + paymentAmt)
                .isEqualTo(paymentAmt);
    }
}
