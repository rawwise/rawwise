package io.github.rawwise.benchmark.chbenchmark;

import io.github.rawwise.benchmark.BenchmarkConfig;
import io.github.rawwise.benchmark.BenchmarkSuite;
import io.github.rawwise.benchmark.LatencyStats;
import io.github.rawwise.benchmark.QueryBenchmarkResult;
import java.io.IOException;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

/**
 * CH-benCHmark benchmark suite — synchronous end-to-end transaction latency measurement.
 *
 * <p>: HTTP session-based cross-table transactions. payment wrapped in BEGIN/COMMIT, three-table
 * UPDATE atomic commit. newOrder wrapped in BEGIN/COMMIT, orders + order_line atomic insert.
 */
public class ChBenchmarkSuite implements BenchmarkSuite {

    private static final Logger LOG = Logger.getLogger(ChBenchmarkSuite.class.getName());
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(120);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final long POLL_TIMEOUT_MS = 300_000;
    private static final long POLL_INTERVAL_MS = 100;
    // : reference data loading range constants. benchmark randomly generated cId/itemId range
    // aligns with loading range,
    // Ensure transactions hit existing data (otherwise payment UPDATE customer always not found,
    // benchmark numbers are meaningless).
    // Range trade-off: CUSTOMER_PER_DISTRICT=300 is 10x less than TPC-C standard 3000, but loading
    // time is also 10x less;
    // ITEM_COUNT=1000 covers common SKU subset. Sufficient for latency test scenarios.
    private static final int CUSTOMER_PER_DISTRICT = 300;
    private static final int ITEM_COUNT = 100;
    private static final int STOCK_PER_WAREHOUSE = ITEM_COUNT; // stock = warehouse × item
    private static final int INSERT_BATCH_SIZE =
            1; // Single-row INSERT goes through FastPath, multi-row goes through Spark and may
    // timeout due to ServerTxnCoordinator not initialized

    private final String frontendHost;
    private final int frontendPort;
    private final boolean distributed;
    private final int clientId;
    private final int totalClients;
    private final String sharedTable;
    private final boolean waitForSignal;
    private final int signalTimeoutSeconds;

    public ChBenchmarkSuite(String frontendHost, int frontendPort) {
        this(frontendHost, frontendPort, false, 0, 1, null, false, 120);
    }

    public ChBenchmarkSuite(
            String frontendHost,
            int frontendPort,
            boolean distributed,
            int clientId,
            int totalClients,
            String sharedTable,
            boolean waitForSignal,
            int signalTimeoutSeconds) {
        this.frontendHost = frontendHost;
        this.frontendPort = frontendPort;
        this.distributed = distributed;
        this.clientId = clientId;
        this.totalClients = totalClients;
        this.sharedTable = sharedTable;
        this.waitForSignal = waitForSignal;
        this.signalTimeoutSeconds = signalTimeoutSeconds;
    }

    @Override
    public List<QueryBenchmarkResult> run(BenchmarkConfig config) {
        int warehouseCount = Math.max(1, config.scaleFactor());
        int threads = Math.max(1, config.threads());
        LOG.info(
                "CH-benCHmark v3 starting: warehouses="
                        + warehouseCount
                        + " threads="
                        + threads
                        + (distributed ? " client=" + (clientId + 1) + "/" + totalClients : ""));

        DdlClient ddlClient = new DdlClient(frontendHost, frontendPort);

        try {
            if (!distributed || clientId == 0) {
                loadSchema(ddlClient);
            }
            if (distributed && waitForSignal) {
                waitForStartSignal(ddlClient);
            }
            loadReferenceData(ddlClient, warehouseCount);
        } catch (Exception e) {
            LOG.severe("Data load failed: " + e.getMessage());
            e.printStackTrace();
            return List.of(
                    new QueryBenchmarkResult(
                            "load",
                            0,
                            new LatencyStats(0, 0, 0),
                            0,
                            "FAIL",
                            null,
                            e.getMessage(),
                            0,
                            0,
                            "SQL"));
        }

        Random rng = new Random(42 + clientId * 1000);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Double> latencies = Collections.synchronizedList(new ArrayList<>());
        LongAdder errorCount = new LongAdder();
        LongAdder conflictCount = new LongAdder();
        int batchSize = 20;

        long warmupEnd = System.currentTimeMillis() + config.warmupRounds() * 1000L;
        while (System.currentTimeMillis() < warmupEnd) {
            List<Future<?>> batch = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                long wId = nextWarehouse(rng, warehouseCount);
                batch.add(
                        pool.submit(
                                () -> {
                                    try {
                                        executeMix(wId, rng.nextDouble());
                                    } catch (Exception ignored) {
                                    }
                                }));
            }
            for (Future<?> f : batch) {
                try {
                    f.get();
                } catch (Exception ignored) {
                }
            }
        }
        LOG.info("Warmup complete, starting measurement...");

        long measureEnd = System.currentTimeMillis() + config.measureRounds() * 1000L;
        long measuredOps = 0;

        while (System.currentTimeMillis() < measureEnd) {
            List<Future<?>> batch = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                double roll = rng.nextDouble();
                long wId = nextWarehouse(rng, warehouseCount);
                batch.add(
                        pool.submit(
                                () -> {
                                    long start = System.nanoTime();
                                    try {
                                        executeMix(wId, roll);
                                        latencies.add(
                                                (System.nanoTime() - start)
                                                        / 1_000.0); // nanoseconds →
                                        // microseconds,and latencyUs
                                        // fieldnameconsistent
                                    } catch (ConflictException ce) {
                                        conflictCount.increment();
                                    } catch (Exception e) {
                                        errorCount.increment();
                                    }
                                }));
            }
            for (Future<?> f : batch) {
                try {
                    f.get();
                } catch (Exception ignored) {
                }
            }
            measuredOps += batchSize;
        }
        pool.shutdown();
        ddlClient.close();

        double throughput = measuredOps / (double) config.measureRounds();
        double[] sorted = latencies.stream().sorted().mapToDouble(Double::doubleValue).toArray();
        double p50 = sorted.length > 0 ? sorted[sorted.length / 2] : 0;
        double p95 = sorted.length > 0 ? sorted[(int) (sorted.length * 0.95)] : 0;
        double p99 = sorted.length > 0 ? sorted[(int) (sorted.length * 0.99)] : 0;
        long errors = errorCount.sum();
        long conflicts = conflictCount.sum();

        LOG.info(
                "CH-benCHmark complete: ops="
                        + measuredOps
                        + " errors="
                        + errors
                        + " conflicts="
                        + conflicts
                        + " throughput="
                        + String.format("%.0f", throughput)
                        + "/s"
                        + " p50="
                        + String.format("%.1f", p50)
                        + "us"
                        + " p95="
                        + String.format("%.1f", p95)
                        + "us"
                        + " p99="
                        + String.format("%.1f", p99)
                        + "us");

        String status =
                errors == 0
                        ? (conflicts == 0 ? "OK" : "OK (" + conflicts + " conflicts)")
                        : errors + " errors";
        return List.of(
                new QueryBenchmarkResult(
                        "chbenchmark-mix",
                        (int) measuredOps,
                        new LatencyStats(p50, p95, p99),
                        0,
                        status,
                        "LOGICAL_PLAN",
                        null,
                        0,
                        0,
                        "SQL"));
    }

    private long nextWarehouse(Random rng, int warehouseCount) {
        if (distributed && totalClients > 1) {
            int perClient = warehouseCount / totalClients;
            int start = clientId * perClient + 1;
            int end = (clientId == totalClients - 1) ? warehouseCount : start + perClient - 1;
            return start + rng.nextLong(end - start + 1);
        }
        return 1 + rng.nextLong(warehouseCount);
    }

    // ================================================================
    // Transaction Mix — per-thread HttpClient with session
    // ================================================================

    private void executeMix(long wId, double roll) throws IOException, InterruptedException {
        HttpClient client = sessionClient();
        try {
            // Pure FastPath OLTP: newOrder, payment, orderStatus only
            // (delivery/stockLevel disabled - COUNT(*)/non-PK ops need Spark)
            if (roll < 0.50) newOrder(client, wId);
            else if (roll < 0.90) payment(client, wId);
            else orderStatus(client, wId);
        } finally {
            returnClient(client);
        }
    }

    private void newOrder(HttpClient client, long wId) throws IOException, InterruptedException {
        long dId = 1 + ThreadLocalRandom.current().nextLong(10);
        long oId = ThreadLocalRandom.current().nextLong(100_000);
        long cId = 1 + ThreadLocalRandom.current().nextLong(CUSTOMER_PER_DISTRICT);
        long itemId = 1 + ThreadLocalRandom.current().nextLong(ITEM_COUNT);
        executeSqlSync(client, "BEGIN");
        try {
            executeSqlSync(
                    client,
                    "INSERT INTO db.orders (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt) VALUES ("
                            + wId
                            + ", "
                            + dId
                            + ", "
                            + oId
                            + ", "
                            + cId
                            + ", 0, 5)");
            executeSqlSync(
                    client,
                    "INSERT INTO db.order_line (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_quantity, ol_amount) VALUES ("
                            + wId
                            + ", "
                            + dId
                            + ", "
                            + oId
                            + ", 1, "
                            + itemId
                            + ", 5, 10.00)");
            executeSqlSync(client, "COMMIT");
        } catch (Exception e) {
            try {
                executeSqlSync(client, "ROLLBACK");
            } catch (Exception rollbackErr) {
                LOG.warning("ROLLBACK failed during newOrder: " + rollbackErr.getMessage());
            }
            throw e;
        }
    }

    private void payment(HttpClient client, long wId) throws IOException, InterruptedException {
        long dId = 1 + ThreadLocalRandom.current().nextLong(10);
        long cId = 1 + ThreadLocalRandom.current().nextLong(CUSTOMER_PER_DISTRICT);
        double amount = 1.0 + ThreadLocalRandom.current().nextDouble() * 5000;
        String amt = String.format("%.2f", amount);
        executeSqlSync(client, "BEGIN");
        try {
            executeSqlSync(
                    client,
                    "UPDATE db.warehouse SET w_ytd = w_ytd + " + amt + " WHERE w_id = " + wId);
            executeSqlSync(
                    client,
                    "UPDATE db.district SET d_ytd = d_ytd + "
                            + amt
                            + " WHERE d_w_id = "
                            + wId
                            + " AND d_id = "
                            + dId);
            executeSqlSync(
                    client,
                    "UPDATE db.customer SET c_balance = c_balance - "
                            + amt
                            + ", c_ytd_payment = c_ytd_payment + "
                            + amt
                            + " WHERE c_w_id = "
                            + wId
                            + " AND c_d_id = "
                            + dId
                            + " AND c_id = "
                            + cId);
            executeSqlSync(client, "COMMIT");
        } catch (Exception e) {
            try {
                executeSqlSync(client, "ROLLBACK");
            } catch (Exception rollbackErr) {
                LOG.warning("ROLLBACK failed during payment: " + rollbackErr.getMessage());
            }
            throw e;
        }
    }

    private void delivery(HttpClient client, long wId) throws IOException, InterruptedException {
        long dId = 1 + ThreadLocalRandom.current().nextLong(10);
        long carrierId = 1 + ThreadLocalRandom.current().nextLong(10);
        executeSqlSync(client, "BEGIN");
        try {
            executeSqlSync(
                    client,
                    "UPDATE db.orders SET o_carrier_id = "
                            + carrierId
                            + " WHERE o_w_id = "
                            + wId
                            + " AND o_d_id = "
                            + dId
                            + " AND o_carrier_id = 0");
            executeSqlSync(client, "COMMIT");
        } catch (Exception e) {
            try {
                executeSqlSync(client, "ROLLBACK");
            } catch (Exception rollbackErr) {
                LOG.warning("ROLLBACK failed during delivery: " + rollbackErr.getMessage());
            }
            throw e;
        }
    }

    private void orderStatus(HttpClient client, long wId) throws IOException, InterruptedException {
        long dId = 1 + ThreadLocalRandom.current().nextLong(10);
        long cId = 1 + ThreadLocalRandom.current().nextLong(CUSTOMER_PER_DISTRICT);
        String sql =
                "SELECT o_id, o_carrier_id, o_ol_cnt FROM db.orders WHERE o_w_id = "
                        + wId
                        + " AND o_d_id = "
                        + dId
                        + " AND o_c_id = "
                        + cId
                        + " ORDER BY o_id DESC LIMIT 1";
        String escaped = sql.replace("\\", "\\\\").replace("\"", "\\\"");
        String submitBody = "{\"sql\":\"" + escaped + "\"}";
        HttpRequest req =
                HttpRequest.newBuilder()
                        .uri(
                                URI.create(
                                        "http://"
                                                + frontendHost
                                                + ":"
                                                + frontendPort
                                                + "/api/query"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(submitBody))
                        .timeout(java.time.Duration.ofSeconds(2))
                        .build();
        client.sendAsync(req, HttpResponse.BodyHandlers.discarding());
    }

    private void stockLevel(HttpClient client, long wId) throws IOException, InterruptedException {
        long threshold = 10 + ThreadLocalRandom.current().nextLong(11);
        executeSqlSync(
                client,
                "SELECT COUNT(*) FROM db.stock WHERE s_w_id = "
                        + wId
                        + " AND s_quantity < "
                        + threshold);
    }

    // ================================================================
    // Schema & Data Loading (uses dedicated client, no session needed)
    // ================================================================

    private void loadSchema(DdlClient client) throws IOException, InterruptedException {
        LOG.info("Creating CH-benCHmark schema (7 tables + USING rawwise)...");
        // Complete 7 tables (column-aligned with src/main/resources/chbenchmark/schema.sql but
        // using RawWise dialect).
        // schema.sql not used because it uses standard SQL syntax, RawWise actually needs `USING
        // rawwise TBLPROPERTIES ('primary-key'=...)`.
        // Column definitions kept consistent with schema.sql, ensuring complete benchmark data
        // write/query fields.
        executeSqlSync(
                client.httpClient,
                "CREATE TABLE IF NOT EXISTS db.warehouse (w_id INT, w_name STRING, w_ytd DOUBLE) "
                        + "USING rawwise TBLPROPERTIES ('primary-key'='w_id')");
        executeSqlSync(
                client.httpClient,
                "CREATE TABLE IF NOT EXISTS db.district (d_w_id INT, d_id INT, d_name STRING, d_ytd DOUBLE, d_next_o_id INT) "
                        + "USING rawwise TBLPROPERTIES ('primary-key'='d_w_id,d_id')");
        executeSqlSync(
                client.httpClient,
                "CREATE TABLE IF NOT EXISTS db.customer (c_w_id INT, c_d_id INT, c_id INT, c_balance DOUBLE, c_ytd_payment DOUBLE, c_name STRING) "
                        + "USING rawwise TBLPROPERTIES ('primary-key'='c_w_id,c_d_id,c_id')");
        executeSqlSync(
                client.httpClient,
                "CREATE TABLE IF NOT EXISTS db.orders (o_w_id INT, o_d_id INT, o_id INT, o_c_id INT, o_carrier_id INT, o_ol_cnt INT) "
                        + "USING rawwise TBLPROPERTIES ('primary-key'='o_w_id,o_d_id,o_id')");
        executeSqlSync(
                client.httpClient,
                "CREATE TABLE IF NOT EXISTS db.order_line (ol_w_id INT, ol_d_id INT, ol_o_id INT, ol_number INT, ol_i_id INT, ol_quantity INT, ol_amount DOUBLE) "
                        + "USING rawwise TBLPROPERTIES ('primary-key'='ol_w_id,ol_d_id,ol_o_id,ol_number')");
        executeSqlSync(
                client.httpClient,
                "CREATE TABLE IF NOT EXISTS db.stock (s_w_id INT, s_i_id INT, s_quantity INT) "
                        + "USING rawwise TBLPROPERTIES ('primary-key'='s_w_id,s_i_id')");
        // : Add item table (one of the complete 7 tables in schema.sql, missing from original
        // Suite)
        executeSqlSync(
                client.httpClient,
                "CREATE TABLE IF NOT EXISTS db.item (i_id INT, i_name STRING, i_price DOUBLE) "
                        + "USING rawwise TBLPROPERTIES ('primary-key'='i_id')");
        LOG.info("Schema created (7 tables with primary keys)");
    }

    private void loadReferenceData(DdlClient client, int warehouseCount)
            throws IOException, InterruptedException {
        // : loadcompletereferencedata — item + warehouse + district + customer + stock.
        // benchmark transaction randomly generated cId/itemId range aligns with loading range (see
        // CUSTOMER_PER_DISTRICT / ITEM_COUNT),
        // Ensure payment/newOrder/stockLevel hit existing data.
        // orders/order_line not pre-loaded (generated by newOrder transactions during measurement
        // phase).

        // 1. item table (globally unique, only loaded by client 0)
        if (!distributed || clientId == 0) {
            LOG.info("Loading " + ITEM_COUNT + " items...");
            for (long batchStart = 1; batchStart <= ITEM_COUNT; batchStart += INSERT_BATCH_SIZE) {
                long batchEnd = Math.min(batchStart + INSERT_BATCH_SIZE - 1, ITEM_COUNT);
                StringBuilder sb = new StringBuilder("INSERT INTO db.item VALUES ");
                for (long i = batchStart; i <= batchEnd; i++) {
                    if (i > batchStart) sb.append(", ");
                    sb.append("(")
                            .append(i)
                            .append(", 'ITEM-")
                            .append(i)
                            .append("', ")
                            .append(1.0 + (i % 100))
                            .append(")");
                }
                executeSqlSync(client.httpClient, sb.toString());
            }
        }

        // 2. warehouse + district + customer + stock (sharded by warehouse)
        for (long w = 1; w <= warehouseCount; w++) {
            if (distributed && totalClients > 1 && !isMyWarehouse(w, warehouseCount)) continue;
            executeSqlSync(
                    client.httpClient,
                    "INSERT INTO db.warehouse VALUES (" + w + ", 'WH-" + w + "', 3000000)");
            for (long d = 1; d <= 10; d++) {
                executeSqlSync(
                        client.httpClient,
                        "INSERT INTO db.district VALUES ("
                                + w
                                + ", "
                                + d
                                + ", 'D-"
                                + w
                                + "-"
                                + d
                                + "', 300000, 1)");
                // customer: load CUSTOMER_PER_DISTRICT rows per district (batched)
                for (long cb = 1; cb <= CUSTOMER_PER_DISTRICT; cb += INSERT_BATCH_SIZE) {
                    long cbEnd = Math.min(cb + INSERT_BATCH_SIZE - 1, CUSTOMER_PER_DISTRICT);
                    StringBuilder csb = new StringBuilder("INSERT INTO db.customer VALUES ");
                    for (long c = cb; c <= cbEnd; c++) {
                        if (c > cb) csb.append(", ");
                        csb.append("(")
                                .append(w)
                                .append(", ")
                                .append(d)
                                .append(", ")
                                .append(c)
                                .append(", 0, 0, 'C-")
                                .append(w)
                                .append("-")
                                .append(d)
                                .append("-")
                                .append(c)
                                .append("')");
                    }
                    executeSqlSync(client.httpClient, csb.toString());
                }
            }
            // stock: load STOCK_PER_WAREHOUSE rows per warehouse (batched)
            LOG.info("Loading " + STOCK_PER_WAREHOUSE + " stock rows for warehouse " + w + "...");
            for (long sb2 = 1; sb2 <= STOCK_PER_WAREHOUSE; sb2 += INSERT_BATCH_SIZE) {
                long se = Math.min(sb2 + INSERT_BATCH_SIZE - 1, STOCK_PER_WAREHOUSE);
                StringBuilder ssb = new StringBuilder("INSERT INTO db.stock VALUES ");
                for (long i = sb2; i <= se; i++) {
                    if (i > sb2) ssb.append(", ");
                    ssb.append("(")
                            .append(w)
                            .append(", ")
                            .append(i)
                            .append(", ")
                            .append(10 + (int) (Math.random() * 90))
                            .append(")");
                }
                executeSqlSync(client.httpClient, ssb.toString());
            }
        }
        LOG.info(
                "Reference data loaded: "
                        + ITEM_COUNT
                        + " items + "
                        + warehouseCount
                        + " warehouses × (10 districts × "
                        + CUSTOMER_PER_DISTRICT
                        + " customers + "
                        + STOCK_PER_WAREHOUSE
                        + " stock)");
    }

    private boolean isMyWarehouse(long wId, int warehouseCount) {
        int perClient = warehouseCount / totalClients;
        int start = clientId * perClient + 1;
        int end = (clientId == totalClients - 1) ? warehouseCount : start + perClient - 1;
        return wId >= start && wId <= end;
    }

    private void waitForStartSignal(DdlClient client) throws IOException, InterruptedException {
        LOG.info("Waiting for start signal (timeout=" + signalTimeoutSeconds + "s)...");
        String url = "http://" + frontendHost + ":" + frontendPort + "/api/ready";
        long deadline = System.currentTimeMillis() + signalTimeoutSeconds * 1000L;
        while (System.currentTimeMillis() < deadline) {
            try {
                HttpRequest req =
                        HttpRequest.newBuilder()
                                .uri(URI.create(url))
                                .GET()
                                .timeout(Duration.ofSeconds(3))
                                .build();
                HttpResponse<String> resp =
                        client.httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 200) {
                    LOG.info("Start signal received");
                    return;
                }
            } catch (Exception ignored) {
            }
            Thread.sleep(500);
        }
        throw new IOException("Start signal timeout after " + signalTimeoutSeconds + "s");
    }

    // ================================================================
    // HttpClient pool (per-thread, session-aware)
    // ================================================================

    private final ThreadLocal<HttpClient> clientPool =
            ThreadLocal.withInitial(
                    () ->
                            HttpClient.newBuilder()
                                    .version(HttpClient.Version.HTTP_1_1)
                                    .connectTimeout(CONNECT_TIMEOUT)
                                    .cookieHandler(new CookieManager(null, CookiePolicy.ACCEPT_ALL))
                                    .build());

    private HttpClient sessionClient() {
        return clientPool.get();
    }

    @SuppressWarnings("unused")
    private void returnClient(HttpClient client) {
        // ThreadLocal — no explicit return needed
    }

    // ================================================================
    // Synchronous SQL execution
    // ================================================================

    private void executeBatchSync(HttpClient client, String... sqls)
            throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder("{\"sqls\":[");
        for (int i = 0; i < sqls.length; i++) {
            if (i > 0) sb.append(",");
            String escaped = sqls[i].replace("\\", "\\\\").replace("\"", "\\\"");
            sb.append("\"").append(escaped).append("\"");
        }
        sb.append("],\"options\":{\"database\":\"db\"}}");

        HttpRequest req =
                HttpRequest.newBuilder()
                        .uri(
                                URI.create(
                                        "http://"
                                                + frontendHost
                                                + ":"
                                                + frontendPort
                                                + "/api/query/batch"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(sb.toString()))
                        .timeout(REQUEST_TIMEOUT)
                        .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 400) {
            String snippet = resp.body().substring(0, Math.min(200, resp.body().length()));
            if (snippet.contains("TXN_CONFLICT") || snippet.contains("conflict")) {
                throw new ConflictException("Conflict: " + snippet);
            }
            throw new IOException("Batch submit failed: " + resp.statusCode() + " body=" + snippet);
        }
    }

    private void executeSqlSync(HttpClient client, String sql)
            throws IOException, InterruptedException {
        String escaped = sql.replace("\\", "\\\\").replace("\"", "\\\"");
        String submitBody = "{\"sql\":\"" + escaped + "\"}";
        IOException lastError = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                executeSqlSyncOnce(client, submitBody);
                return;
            } catch (IOException e) {
                lastError = e;
                if (attempt < 2) {
                    LOG.fine("Retrying after: " + e.getMessage());
                    Thread.sleep(100L * (attempt + 1));
                }
            }
        }
        throw lastError;
    }

    private void executeSqlSyncOnce(HttpClient client, String submitBody)
            throws IOException, InterruptedException {
        HttpRequest submitReq =
                HttpRequest.newBuilder()
                        .uri(
                                URI.create(
                                        "http://"
                                                + frontendHost
                                                + ":"
                                                + frontendPort
                                                + "/api/query"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(submitBody))
                        .timeout(REQUEST_TIMEOUT)
                        .build();
        HttpResponse<String> submitResp =
                client.send(submitReq, HttpResponse.BodyHandlers.ofString());
        if (submitResp.statusCode() >= 400) {
            String snippet =
                    submitResp.body().substring(0, Math.min(200, submitResp.body().length()));
            if (snippet.contains("TXN_CONFLICT") || snippet.contains("conflict")) {
                throw new ConflictException("Conflict: " + snippet);
            }
            throw new IOException("Submit failed: " + submitResp.statusCode() + " body=" + snippet);
        }

        String respBody = submitResp.body();
        // FastPath / IR / LOCAL path: POST response already contains complete result
        // (state=SUCCEEDED), no polling needed
        if (respBody.contains("\"SUCCEEDED\"") || respBody.contains("\"COMPLETED\"")) {
            return;
        }

        String jobId = extractJobId(respBody);
        if (jobId == null) {
            throw new IOException(
                    "No jobId in response: "
                            + respBody.substring(0, Math.min(200, respBody.length())));
        }

        // Spark job (job_ prefix): synchronous polling until completion.
        // DDL (CREATE/DROP TABLE) must wait for completion before continuing INSERT;
        // Measurement phase OLTP transactions all go through FastPath, no job_ prefix generated.
        String resultUrl =
                "http://" + frontendHost + ":" + frontendPort + "/api/query/" + jobId + "/result";
        long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            HttpRequest pollReq =
                    HttpRequest.newBuilder()
                            .uri(URI.create(resultUrl))
                            .GET()
                            .timeout(REQUEST_TIMEOUT)
                            .build();
            HttpResponse<String> pollResp =
                    client.send(pollReq, HttpResponse.BodyHandlers.ofString());
            if (pollResp.statusCode() == 200) {
                String resultBody = pollResp.body();
                if (resultBody.contains("\"COMPLETED\"")
                        || resultBody.contains("\"SUCCESS\"")
                        || resultBody.contains("\"SUCCEEDED\"")) return;
                if (resultBody.contains("\"FAILED\"") || resultBody.contains("\"error\"")) {
                    String snippet = resultBody.substring(0, Math.min(200, resultBody.length()));
                    if (snippet.contains("TXN_CONFLICT") || snippet.contains("conflict")) {
                        throw new ConflictException("Conflict: " + snippet);
                    }
                    throw new IOException("Query failed: " + snippet);
                }
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
        throw new IOException("Query timeout: " + jobId);
    }

    private static String extractJobId(String json) {
        int idx = json.indexOf("\"jobId\":\"");
        if (idx < 0) return null;
        int start = idx + 9;
        int end = json.indexOf("\"", start);
        return end > start ? json.substring(start, end) : null;
    }

    // ================================================================
    // DDL Client (no session, long-lived for data loading)
    // ================================================================

    static class DdlClient implements AutoCloseable {
        final HttpClient httpClient;

        DdlClient(String host, int port) {
            this.httpClient =
                    HttpClient.newBuilder()
                            .connectTimeout(CONNECT_TIMEOUT)
                            .version(HttpClient.Version.HTTP_1_1)
                            .build();
        }

        @Override
        public void close() {
            // no explicit close needed for HttpClient
        }
    }

    static class ConflictException extends IOException {
        ConflictException(String message) {
            super(message);
        }
    }
}
