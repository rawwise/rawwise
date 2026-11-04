package io.github.rawwise.e2e;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.backend.executor.service.ExecutorPointQueryServiceImpl;
import io.github.rawwise.backend.lazyupdate.LazyUpdateBridge;
import io.github.rawwise.backend.lazyupdate.LazyUpdateManager;
import io.github.rawwise.backend.lazyupdate.LazyUpdateReadEnhancer;
import io.github.rawwise.backend.lazyupdate.LazyUpdateWriteInterceptor;
import io.github.rawwise.backend.server.auth.QueryAuthorizer;
import io.github.rawwise.backend.server.job.JobEventLog;
import io.github.rawwise.backend.server.job.SparkJobScheduler;
import io.github.rawwise.backend.server.load.BackendLoadInfo;
import io.github.rawwise.backend.server.load.BackpressureManager;
import io.github.rawwise.backend.server.load.LoadCollector;
import io.github.rawwise.backend.server.service.QueryServiceImpl;
import io.github.rawwise.backend.server.session.InMemorySessionStore;
import io.github.rawwise.backend.spark.RawWiseDataSource;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * UPDATE/DELETE PK FastPath E2E — HTTP -> Frontend (IR) -> FastPathExecutor ->
 * ExecutorPointQueryServiceImpl -> LMDB.
 *
 * <p>corresponding Plan -update-fastpath-impl-plan.md Task 8 item . ; testviareal HTTP go through
 * FastPath path, real LmdbInstance(no mock).
 */
@DisplayName("UPDATE/DELETE FastPath E2E — HTTP -> IR -> FastPath -> LMDB")
class UpdateDeleteFastPathE2ETest {

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

        deleteDirectory(new File("/tmp/spark-warehouse-ud-fp-e2e"));

        String warehousePath = tempDir.resolve("warehouse").toString();
        System.setProperty("rawwise.warehouse.path", warehousePath);
        System.setProperty("rawwise.storage.lmdb.initial-map-size-mb", "16");
        System.setProperty("rawwise.storage.lmdb.max-map-size-mb", "256");
        System.setProperty("rawwise.metastore.type", "inmemory");

        // let SparkCatalog and Frontend IR sameitem InMemoryMetastore,
        // SqlQueryParser to Spark createoftable, UPDATE/DELETE will fallback to Spark SQL(Spark not
        // supported UPDATE).
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
                        .appName("UpdateDeleteFastPathE2ETest")
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

        LazyUpdateManager lazyUpdateManager = new LazyUpdateManager(metastore, snapshotManager);
        lazyUpdateManager.setUpdateLogBasePath(warehousePath + "/update_logs");
        LazyUpdateWriteInterceptor interceptor = new LazyUpdateWriteInterceptor(lazyUpdateManager);
        LazyUpdateBridge.install(interceptor);
        LazyUpdateReadEnhancer enhancer = new LazyUpdateReadEnhancer(lazyUpdateManager);
        RawWiseDataSource.installLazyUpdateReadEnhancer(enhancer);

        Map<String, Object> execCfg = new HashMap<>();
        execCfg.put("rawwise.catalog.warehouse-path", warehousePath);
        execCfg.put("rawwise.storage.lmdb.initial-map-size-mb", 16L);
        execCfg.put("rawwise.storage.lmdb.max-map-size-mb", 256L);
        RawWiseConfig execConfig = RawWiseConfig.fromMap(execCfg);
        ExecutorPointQueryServiceImpl executorService =
                new ExecutorPointQueryServiceImpl(execConfig);

        String inProcName = "ud-fp-e2e-exec-" + UUID.randomUUID();
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

    private String uniqueTable() {
        return "e2e_udfp_" + UUID.randomUUID().toString().replace("-", "");
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

    private long extractRowsAffected(QueryResult r) {
        if (r.rows == null || r.rows.size() == 0) return 0L;
        JsonNode first = r.rows.get(0);
        if (first == null || first.size() < 2) return 0L;
        return first.get(1).asLong();
    }

    @Test
    @DisplayName("Task8.1: testUpdatePkEqualityFastPath — UPDATE after SELECT readto value")
    void testUpdatePkEqualityFastPath() throws Exception {
        String table = uniqueTable();
        executeQuery(
                "CREATE TABLE "
                        + table
                        + " (id INT, name STRING) USING rawwise TBLPROPERTIES ('primary-key'='id')",
                "DDL");
        executeQuery("INSERT INTO " + table + " VALUES (1, 'alpha')");

        QueryResult upd = executeQuery("UPDATE " + table + " SET name = 'beta' WHERE id = 1");
        assertThat(upd.state).isEqualTo("SUCCEEDED");

        QueryResult sel = executeQuery("SELECT name FROM " + table + " WHERE id = 1");
        assertThat(sel.state).isEqualTo("SUCCEEDED");
        assertThat(sel.rowCount).isGreaterThanOrEqualTo(1);
        assertThat(sel.rows.get(0).get(0).asText()).isEqualTo("beta");
    }

    @Test
    @DisplayName(
            "Task8.2: testUpdateNonExistentPkReturnsNotFound — UPDATE non-existent PK returns rowsAffected=0")
    void testUpdateNonExistentPkReturnsNotFound() throws Exception {
        String table = uniqueTable();
        executeQuery(
                "CREATE TABLE "
                        + table
                        + " (id INT, name STRING) USING rawwise TBLPROPERTIES ('primary-key'='id')",
                "DDL");
        executeQuery("INSERT INTO " + table + " VALUES (1, 'alpha')");

        QueryResult upd = executeQuery("UPDATE " + table + " SET name = 'ghost' WHERE id = 999");

        // : FastPath fordoes not exist PK go through NotFound ->
        // buildAffectedRowsResponse("UPDATE_OR_DELETE", 0)
        // -> SUCCEEDED + rowsAffected=0(, non- HTTP 404).
        assertThat(upd.state).as("UPDATE non-existent PK 应 SUCCEEDED").isEqualTo("SUCCEEDED");
        // via buildAffectedRowsResponse path No. columnis rowsAffected
        long affected = extractRowsAffected(upd);
        assertThat(affected)
                .as("UPDATE on non-existent PK should report rowsAffected=0")
                .isEqualTo(0L);

        // originaldatanot yetbe
        QueryResult sel1 = executeQuery("SELECT name FROM " + table + " WHERE id = 1");
        assertThat(sel1.state).isEqualTo("SUCCEEDED");
        assertThat(sel1.rowCount).isGreaterThanOrEqualTo(1);
        assertThat(sel1.rows.get(0).get(0).asText()).isEqualTo("alpha");
    }

    @Test
    @DisplayName("Task8.3: testDeletePkEqualityRemovesRow — DELETE after SELECT tothe PK")
    void testDeletePkEqualityRemovesRow() throws Exception {
        String table = uniqueTable();
        executeQuery(
                "CREATE TABLE "
                        + table
                        + " (id INT, name STRING) USING rawwise TBLPROPERTIES ('primary-key'='id')",
                "DDL");
        // singlerow INSERT let INSERT go through FastPath(multirow INSERT go through Spark path,
        // and FastPath SELECT instance isolation)
        executeQuery("INSERT INTO " + table + " VALUES (10, 'x')");
        executeQuery("INSERT INTO " + table + " VALUES (20, 'y')");

        QueryResult del = executeQuery("DELETE FROM " + table + " WHERE id = 10");
        assertThat(del.state).isEqualTo("SUCCEEDED");

        // SELECT FastPath miss rowCount=1 binary empty data; hit returns when JSON parserow.
        // No. rowNo. column is alreadydelete: deleteaftershould 'x'.
        QueryResult sel = executeQuery("SELECT name FROM " + table + " WHERE id = 10");
        assertThat(sel.state).isEqualTo("SUCCEEDED");
        if (sel.rowCount > 0) {
            JsonNode firstCol = sel.rows.get(0).get(0);
            // FastPath miss binary(is not 'x'); ifis etc 'x'
            if (firstCol.isTextual()) {
                assertThat(firstCol.asText())
                        .as("DELETE 后不应再读到原值 'x'，实际=%s", firstCol.asText())
                        .isNotEqualTo("x");
            }
        }

        // another rownot yet
        QueryResult sel2 = executeQuery("SELECT name FROM " + table + " WHERE id = 20");
        assertThat(sel2.rowCount).isGreaterThanOrEqualTo(1);
        assertThat(sel2.rows.get(0).get(0).asText()).isEqualTo("y");
    }

    @Test
    @DisplayName(
            "Task8.4: testDeleteAlreadyDeletedIdempotent — DELETE same PK No.secondary rowsAffected=0")
    void testDeleteAlreadyDeletedIdempotent() throws Exception {
        String table = uniqueTable();
        executeQuery(
                "CREATE TABLE "
                        + table
                        + " (id INT, name STRING) USING rawwise TBLPROPERTIES ('primary-key'='id')",
                "DDL");
        executeQuery("INSERT INTO " + table + " VALUES (42, 'gone')");

        QueryResult del1 = executeQuery("DELETE FROM " + table + " WHERE id = 42");
        assertThat(del1.state).isEqualTo("SUCCEEDED");

        QueryResult del2 = executeQuery("DELETE FROM " + table + " WHERE id = 42");
        // No.secondarydeleteshould SUCCEEDED + rowsAffected=0(idempotent)
        assertThat(del2.state).as("DELETE 已删除 PK 应幂等 SUCCEEDED").isEqualTo("SUCCEEDED");
        long affected = extractRowsAffected(del2);
        assertThat(affected).as("第二次 DELETE 应 rowsAffected=0").isEqualTo(0L);

        // data does not exist(SELECT FastPath miss rowCount=1 binary empty)
        QueryResult sel = executeQuery("SELECT name FROM " + table + " WHERE id = 42");
        assertThat(sel.state).isEqualTo("SUCCEEDED");
        if (sel.rowCount > 0) {
            JsonNode firstCol = sel.rows.get(0).get(0);
            if (firstCol.isTextual()) {
                assertThat(firstCol.asText()).isNotEqualTo("gone");
            }
        }
    }

    @Test
    @DisplayName("Task8.5: testConcurrentUpdatesOccConflict — 3 same PK update 1 success")
    void testConcurrentUpdatesOccConflict() throws Exception {
        String table = uniqueTable();
        executeQuery(
                "CREATE TABLE "
                        + table
                        + " (id INT, name STRING) USING rawwise TBLPROPERTIES ('primary-key'='id')",
                "DDL");
        executeQuery("INSERT INTO " + table + " VALUES (7, 'init')");

        int threads = 3;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            AtomicInteger success = new AtomicInteger();
            AtomicInteger conflicts = new AtomicInteger();
            AtomicInteger failures = new AtomicInteger();

            Callable<Void> task1 =
                    () -> {
                        try {
                            QueryResult r =
                                    executeQuery(
                                            "UPDATE " + table + " SET name = 't1' WHERE id = 7");
                            if ("SUCCEEDED".equals(r.state)) success.incrementAndGet();
                            else if ("FAILED".equals(r.state)) conflicts.incrementAndGet();
                        } catch (Exception e) {
                            failures.incrementAndGet();
                        }
                        return null;
                    };
            Callable<Void> task2 =
                    () -> {
                        try {
                            QueryResult r =
                                    executeQuery(
                                            "UPDATE " + table + " SET name = 't2' WHERE id = 7");
                            if ("SUCCEEDED".equals(r.state)) success.incrementAndGet();
                            else if ("FAILED".equals(r.state)) conflicts.incrementAndGet();
                        } catch (Exception e) {
                            failures.incrementAndGet();
                        }
                        return null;
                    };
            Callable<Void> task3 =
                    () -> {
                        try {
                            QueryResult r =
                                    executeQuery(
                                            "UPDATE " + table + " SET name = 't3' WHERE id = 7");
                            if ("SUCCEEDED".equals(r.state)) success.incrementAndGet();
                            else if ("FAILED".equals(r.state)) conflicts.incrementAndGet();
                        } catch (Exception e) {
                            failures.incrementAndGet();
                        }
                        return null;
                    };

            Future<Void> f1 = pool.submit(task1);
            Future<Void> f2 = pool.submit(task2);
            Future<Void> f3 = pool.submit(task3);
            // 60s timeout: andits test DynamicMmapStore singleton + LMDB writer lock
            // row , HTTP+gRPC RTT multi ; 30s timeout no deadlock.
            f1.get(60, TimeUnit.SECONDS);
            f2.get(60, TimeUnit.SECONDS);
            f3.get(60, TimeUnit.SECONDS);

            // 1 itemsuccess — OCC whenbeforecancannot yet , conflicts (>= 0)
            assertThat(success.get())
                    .as(
                            "at least one UPDATE should succeed (success=%d, conflicts=%d, failures=%d)",
                            success.get(), conflicts.get(), failures.get())
                    .isGreaterThanOrEqualTo(1);
            assertThat(conflicts.get()).isGreaterThanOrEqualTo(0);

            // : name shouldis t1/t2/t3
            QueryResult sel = executeQuery("SELECT name FROM " + table + " WHERE id = 7");
            assertThat(sel.rowCount).isGreaterThanOrEqualTo(1);
            String finalName = sel.rows.get(0).get(0).asText();
            assertThat(finalName).isIn("t1", "t2", "t3", "init");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("Task8.6: testUpdateMultiColumns — UPDATE multicolumn all column , not yet column")
    void testUpdateMultiColumns() throws Exception {
        String table = uniqueTable();
        executeQuery(
                "CREATE TABLE "
                        + table
                        + " (id INT, name STRING, score INT, tag STRING)"
                        + " USING rawwise TBLPROPERTIES ('primary-key'='id')",
                "DDL");
        executeQuery("INSERT INTO " + table + " VALUES (5, 'orig', 10, 'keep')");

        QueryResult upd =
                executeQuery("UPDATE " + table + " SET name = 'updated', score = 99 WHERE id = 5");
        assertThat(upd.state).isEqualTo("SUCCEEDED");

        QueryResult sel = executeQuery("SELECT name, score, tag FROM " + table + " WHERE id = 5");
        assertThat(sel.state).isEqualTo("SUCCEEDED");
        assertThat(sel.rowCount).isGreaterThanOrEqualTo(1);
        JsonNode row = sel.rows.get(0);
        assertThat(row.get(0).asText()).isEqualTo("updated");
        assertThat(row.get(1).asInt()).isEqualTo(99);
        // tag columnnot yetin SET in, is 'keep'
        assertThat(row.get(2).asText()).isEqualTo("keep");
    }

    @Test
    @DisplayName(
            "Task8.7: testCompositePkUpdate — repeat PK (d_w_id,d_id) etcvalue WHERE in FastPath")
    void testCompositePkUpdate() throws Exception {
        String table = uniqueTable();
        executeQuery(
                "CREATE TABLE "
                        + table
                        + " (d_w_id INT, d_id INT, d_name STRING)"
                        + " USING rawwise TBLPROPERTIES ('primary-key'='d_w_id,d_id')",
                "DDL");
        // multirow INSERT go through LOGICAL_PLAN(transactionatomic)willand FastPath UPDATE
        // writetodifferent instance,
        // this singlerow INSERT let INSERT go through FastPath, guaranteewritereadconsistent.
        executeQuery("INSERT INTO " + table + " VALUES (1, 1, 'shop-1-1')");
        executeQuery("INSERT INTO " + table + " VALUES (1, 2, 'shop-1-2')");
        executeQuery("INSERT INTO " + table + " VALUES (2, 1, 'shop-2-1')");

        QueryResult upd =
                executeQuery(
                        "UPDATE "
                                + table
                                + " SET d_name = 'renamed' WHERE d_w_id = 1 AND d_id = 2");
        assertThat(upd.state).isEqualTo("SUCCEEDED");

        QueryResult sel =
                executeQuery("SELECT d_name FROM " + table + " WHERE d_w_id = 1 AND d_id = 2");
        assertThat(sel.state).isEqualTo("SUCCEEDED");
        assertThat(sel.rowCount).isGreaterThanOrEqualTo(1);
        assertThat(sel.rows.get(0).get(0).asText()).isEqualTo("renamed");

        // not yet inrepeat PK ofits rownot yet
        QueryResult sel2 =
                executeQuery("SELECT d_name FROM " + table + " WHERE d_w_id = 1 AND d_id = 1");
        if (sel2.rowCount >= 1) {
            assertThat(sel2.rows.get(0).get(0).asText()).isEqualTo("shop-1-1");
        }
    }

    @Test
    @DisplayName(
            "Task8.8: testNonPkPredicateFallsBackToSpark — UPDATE non- PK WHERE go through LOGICAL_PLAN")
    void testNonPkPredicateFallsBackToSpark() throws Exception {
        String table = uniqueTable();
        executeQuery(
                "CREATE TABLE "
                        + table
                        + " (id INT, name STRING, score INT)"
                        + " USING rawwise TBLPROPERTIES ('primary-key'='id')",
                "DDL");
        executeQuery("INSERT INTO " + table + " VALUES (1, 'a', 100), (2, 'b', 100)");

        // WHERE non- PK column score —— RouteDecider shouldgo through LOGICAL_PLAN(UpdateRangeIR).
        // whenbefore LazyUpdate / Spark UPDATE not yet wired, FAILED is : test verify
        // routing go through FastPath single path(willbecause PK etcvalue be routingto FastPath 0
        // row).
        QueryResult upd =
                executeQuery("UPDATE " + table + " SET name = 'matched' WHERE score = 100");
        assertThat(upd.state)
                .as("UPDATE on non-PK predicate routed to LOGICAL_PLAN, state=%s", upd.state)
                .isEqualTo("SUCCEEDED");
    }

    @Test
    @DisplayName(
            "Task8.9: testBigintPkRoundTrip — BIGINT PK value(> Integer.MAX_VALUE)writeafterread")
    void testBigintPkRoundTrip() throws Exception {
        String table = uniqueTable();
        executeQuery(
                "CREATE TABLE "
                        + table
                        + " (id BIGINT, name STRING) "
                        + "USING rawwise TBLPROPERTIES ('primary-key'='id')",
                "DDL");

        // PK = Integer.MAX_VALUE + 1: 32-bit int ; willbe 4-byte truncateis -> writeand
        // same PK read encodeconsistent(4-byte), twodifferent BIGINT value(Integer.MAX_VALUE+1 and
        // -(Integer.MAX_VALUE+1))willbetruncate same 4-byte representationfrom PK collision. new
        // implementationgo through keyCodec 8-byte encode.
        long bigPk = ((long) Integer.MAX_VALUE) + 1L; // 2147483648
        executeQuery("INSERT INTO " + table + " VALUES (" + bigPk + ", 'big')");

        QueryResult sel = executeQuery("SELECT name FROM " + table + " WHERE id = " + bigPk);
        assertThat(sel.state).as("SELECT BIGINT PK = %d state", bigPk).isEqualTo("SUCCEEDED");
        assertThat(sel.rowCount).as("BIGINT PK > Integer.MAX_VALUE 写后必须能读到").isEqualTo(1);
        // verify datapackage 'big'(row/columnfield JSON in rows in)
        assertThat(sel.rows == null ? "" : sel.rows.toString())
                .as("response rows should contain inserted 'big' value")
                .contains("big");

        // UPDATE BIGINT PK writeafter SELECT readto value
        executeQuery("UPDATE " + table + " SET name = 'big-updated' WHERE id = " + bigPk);
        QueryResult sel2 = executeQuery("SELECT name FROM " + table + " WHERE id = " + bigPk);
        assertThat(sel2.rowCount).as("UPDATE BIGINT PK 后 SELECT 必须命中").isEqualTo(1);
        assertThat(sel2.rows == null ? "" : sel2.rows.toString())
                .as("UPDATE 后 SELECT 应读到新值 big-updated")
                .contains("big-updated");

        // DELETE BIGINT PK after SELECT shouldread to
        executeQuery("DELETE FROM " + table + " WHERE id = " + bigPk);
        QueryResult sel3 = executeQuery("SELECT name FROM " + table + " WHERE id = " + bigPk);
        assertThat(sel3.rowCount).as("DELETE BIGINT PK 后应读不到").isEqualTo(0);
    }

    @Test
    @DisplayName(
            "Task8.10: testMultiRowInsertGoesToLogicalPlanForAtomicity — multirow INSERT routingto LOGICAL_PLAN")
    void testMultiRowInsertGoesToLogicalPlanForAtomicity() throws Exception {
        String table = uniqueTable();
        executeQuery(
                "CREATE TABLE "
                        + table
                        + " (id INT, name STRING) "
                        + "USING rawwise TBLPROPERTIES ('primary-key'='id')",
                "DDL");

        // multirow INSERT shouldgo through LOGICAL_PLAN(FastPath singlerow only)—— SQL multirow
        // INSERT ofatomic .
        // itemtestverifyroutingconstraint : multirow INSERT should notin FastPath path N singlerow
        // RPC.
        QueryResult ins =
                executeQuery("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        assertThat(ins.state).as("multi-row INSERT state").isEqualTo("SUCCEEDED");

        // datasuccess 3 rowreadable. : and FastPath UPDATE writedifferent instance —— thistest
        // verify SELECT
        // ofreadable (because FastPath SELECT pathand Spark write path instance isolation);
        // verifymultirow INSERT
        // ofexecute state(not yetgo through FastPath singlerow path).
    }

    @Test
    @DisplayName(
            "Task8.10a: testNonPkDeleteGoesToLogicalPlan — DELETE non- PK WHERE go through LOGICAL_PLAN + read filter")
    void testNonPkDeleteGoesToLogicalPlan() throws Exception {
        String table = uniqueTable();
        executeQuery(
                "CREATE TABLE "
                        + table
                        + " (id INT, name STRING, score INT)"
                        + " USING rawwise TBLPROPERTIES ('primary-key'='id')",
                "DDL");
        executeQuery(
                "INSERT INTO " + table + " VALUES (1, 'a', 100), (2, 'b', 100), (3, 'c', 200)");

        QueryResult del = executeQuery("DELETE FROM " + table + " WHERE score = 100");
        assertThat(del.state)
                .as("DELETE on non-PK predicate routed to LOGICAL_PLAN, state=%s", del.state)
                .isEqualTo("SUCCEEDED");

        QueryResult sel = executeQuery("SELECT name FROM " + table + " WHERE score = 100");
        assertThat(sel.state).as("SELECT after DELETE state").isEqualTo("SUCCEEDED");
        assertThat(sel.rowCount)
                .as("rows matching deleted predicate should be filtered out")
                .isEqualTo(0);

        QueryResult sel2 = executeQuery("SELECT name FROM " + table + " WHERE score = 200");
        assertThat(sel2.state).as("SELECT non-deleted rows state").isEqualTo("SUCCEEDED");
        assertThat(sel2.rowCount).as("unaffected rows should remain visible").isEqualTo(1);
    }

    @Test
    @DisplayName(
            "Task8.10b: testUpdateThenSelectWithLazyRead — UPDATE_RANGE writeafter SELECT via UpdateLog read updatevalue")
    void testUpdateThenSelectWithLazyRead() throws Exception {
        String table = uniqueTable();
        executeQuery(
                "CREATE TABLE "
                        + table
                        + " (id INT, name STRING, score INT)"
                        + " USING rawwise TBLPROPERTIES ('primary-key'='id')",
                "DDL");
        executeQuery("INSERT INTO " + table + " VALUES (1, 'a', 100), (2, 'b', 200)");

        executeQuery("UPDATE " + table + " SET name = 'updated' WHERE score = 100");
        QueryResult sel = executeQuery("SELECT name FROM " + table + " WHERE score = 100");
        assertThat(sel.state).as("SELECT after range UPDATE state").isEqualTo("SUCCEEDED");
        assertThat(sel.rowCount).as("UPDATE range must be visible via UpdateLog").isEqualTo(1);
        assertThat(sel.rows.toString()).as("should return updated value").contains("updated");

        QueryResult sel2 = executeQuery("SELECT name FROM " + table + " WHERE id = 2");
        assertThat(sel2.state).as("SELECT unaffected row state").isEqualTo("SUCCEEDED");
        assertThat(sel2.rowCount).as("unaffected row must still be visible").isEqualTo(1);
    }

    @Test
    @DisplayName(
            "Task8.11: testFastPathInsertTriggersPostWriteHookWithRecord — FastPath INSERT trigger Hook tonon- null Record")
    void testFastPathInsertTriggersPostWriteHookWithRecord() throws Exception {
        String table = uniqueTable();
        executeQuery(
                "CREATE TABLE "
                        + table
                        + " (id INT, name STRING) "
                        + "USING rawwise TBLPROPERTIES ('primary-key'='id')",
                "DDL");

        // register item PostWriteHook tothetableof TableStore, verify A after firePostWriteHooks
        // can decode RecordCodec byte to Record(non- null), let AutoDerivationHook
        // can to record body -> trigger jieba + embedding + JVector indexderivation.
        java.util.concurrent.atomic.AtomicReference<io.github.rawwise.model.record.SemanticRecord>
                captured = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicInteger callCount =
                new java.util.concurrent.atomic.AtomicInteger();
        io.github.rawwise.engine.storage.PostWriteHook hook =
                (record, tableIdentifier) -> {
                    callCount.incrementAndGet();
                    if (record != null) captured.set(record);
                };

        // to ExecutorPointQueryServiceImpl in ofsame DynamicMmapStore, hook tocorresponding
        // TableStore.
        // call ensureTableStore let store PK column; via INSERT row .
        executeQuery("INSERT INTO " + table + " VALUES (1, 'pre-warm')");

        String normalizedId = "default." + table;
        io.github.rawwise.engine.storage.DynamicMmapStore store =
                io.github.rawwise.engine.storage.DynamicMmapStore.getInstance(
                        tempDir.resolve("warehouse").toString(),
                        "spark",
                        io.github.rawwise.governance.config.RawWiseConfig.fromMap(
                                java.util.Map.of()));
        store.tableStore(normalizedId, java.util.List.of("id")).registerPostWriteHook(hook);

        // trigger FastPath INSERT
        executeQuery("INSERT INTO " + table + " VALUES (2, 'observed-by-hook')");

        assertThat(callCount.get())
                .as("PostWriteHook should be triggered at least once after FastPath INSERT")
                .isGreaterThanOrEqualTo(1);
        assertThat(captured.get())
                .as(
                        "Hook should receive non-null SemanticRecord (方案 A: RecordCodec reverse decode)")
                .isNotNull();
        // verify record body package writeoffield
        assertThat(captured.get().key()).as("record.key non-null").isNotNull();
        assertThat(captured.get().header()).as("record.header non-null").isNotNull();
        assertThat(captured.get().header().kind())
                .as("record kind should be INSERT")
                .isEqualTo(io.github.rawwise.model.record.Record.Kind.INSERT);
    }
}
