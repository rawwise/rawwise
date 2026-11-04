package io.github.rawwise.benchmark.chbenchmark;

import java.io.IOException;
import java.net.CookieManager;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

public class ChHtapSuite {

    private static final Logger LOG = Logger.getLogger(ChHtapSuite.class.getName());
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final int CUSTOMER_PER_DISTRICT = 3000;

    private final String frontendHost;
    private final int frontendPort;
    private final long warehouses;
    private final int oltpThreads;
    private final int olapThreads;
    private final int warmupSeconds;
    private final int measureSeconds;

    public ChHtapSuite(
            String host,
            int port,
            long warehouses,
            int oltpThreads,
            int olapThreads,
            int warmupSeconds,
            int measureSeconds) {
        this.frontendHost = host;
        this.frontendPort = port;
        this.warehouses = warehouses;
        this.oltpThreads = oltpThreads;
        this.olapThreads = olapThreads;
        this.warmupSeconds = warmupSeconds;
        this.measureSeconds = measureSeconds;
    }

    public HtapResult run() throws Exception {
        LOG.info(
                "Starting HTAP suite: warehouses="
                        + warehouses
                        + " oltpThreads="
                        + oltpThreads
                        + " olapThreads="
                        + olapThreads
                        + " warmup="
                        + warmupSeconds
                        + "s measure="
                        + measureSeconds
                        + "s");

        ChOlapQueries olapQueries = new ChOlapQueries(frontendHost, frontendPort);

        runPhase("warmup", warmupSeconds);
        HtapResult result = runPhase("measure", measureSeconds);

        LOG.info("HTAP suite complete: " + result);
        return result;
    }

    private HtapResult runPhase(String phaseName, int durationSeconds) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(oltpThreads + olapThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch stopLatch = new CountDownLatch(1);
        LongAdder oltpOps = new LongAdder();
        LongAdder oltpErrors = new LongAdder();
        LongAdder olapOps = new LongAdder();
        LongAdder olapErrors = new LongAdder();
        List<Long> oltpLatencies = Collections.synchronizedList(new ArrayList<>());
        List<Long> olapLatencies = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < oltpThreads; i++) {
            final long wId = (i % warehouses) + 1;
            executor.submit(
                    () -> {
                        HttpClient client = createClient();
                        try {
                            startLatch.await();
                            while (!Thread.currentThread().isInterrupted()) {
                                long t0 = System.nanoTime();
                                try {
                                    double roll = ThreadLocalRandom.current().nextDouble();
                                    if (roll < 0.45) {
                                        executeSqlSync(client, "BEGIN");
                                        executeSqlSync(
                                                client,
                                                "INSERT INTO db.orders (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt) VALUES ("
                                                        + wId
                                                        + ", 1, "
                                                        + System.nanoTime() % 100000
                                                        + ", 1, 0, 1)");
                                        executeSqlSync(client, "COMMIT");
                                    } else if (roll < 0.88) {
                                        double amt =
                                                1.0
                                                        + ThreadLocalRandom.current().nextDouble()
                                                                * 5000;
                                        String amtStr = String.format("%.2f", amt);
                                        executeSqlSync(client, "BEGIN");
                                        executeSqlSync(
                                                client,
                                                "UPDATE db.warehouse SET w_ytd = w_ytd + "
                                                        + amtStr
                                                        + " WHERE w_id = "
                                                        + wId);
                                        executeSqlSync(client, "COMMIT");
                                    } else if (roll < 0.92) {
                                        executeSqlSync(
                                                client,
                                                "SELECT o_id, o_carrier_id, o_ol_cnt FROM db.orders WHERE o_w_id = "
                                                        + wId
                                                        + " AND o_d_id = 1 AND o_c_id = 1 ORDER BY o_id DESC LIMIT 1");
                                    } else if (roll < 0.96) {
                                        executeSqlSync(
                                                client,
                                                "UPDATE db.orders SET o_carrier_id = 1 WHERE o_w_id = "
                                                        + wId
                                                        + " AND o_d_id = 1 AND o_carrier_id = 0");
                                    } else {
                                        executeSqlSync(
                                                client,
                                                "SELECT COUNT(*) FROM db.stock WHERE s_w_id = "
                                                        + wId
                                                        + " AND s_quantity < 20");
                                    }
                                    oltpLatencies.add((System.nanoTime() - t0) / 1_000_000);
                                    oltpOps.increment();
                                } catch (Exception e) {
                                    oltpErrors.increment();
                                }
                            }
                        } catch (InterruptedException e) {
                            // shutdown
                        } finally {
                            stopLatch.countDown();
                        }
                    });
        }

        for (int i = 0; i < olapThreads; i++) {
            final long wId = (i % warehouses) + 1;
            executor.submit(
                    () -> {
                        HttpClient client = createClient();
                        try {
                            startLatch.await();
                            while (!Thread.currentThread().isInterrupted()) {
                                long t0 = System.nanoTime();
                                try {
                                    List<String> queries =
                                            new ChOlapQueries(frontendHost, frontendPort)
                                                    .getAllQueries(wId);
                                    String sql =
                                            queries.get(
                                                    ThreadLocalRandom.current()
                                                            .nextInt(queries.size()));
                                    new ChOlapQueries(frontendHost, frontendPort)
                                            .executeQuery(client, sql);
                                    olapLatencies.add((System.nanoTime() - t0) / 1_000_000);
                                    olapOps.increment();
                                } catch (Exception e) {
                                    olapErrors.increment();
                                }
                            }
                        } catch (InterruptedException e) {
                            // shutdown
                        } finally {
                            stopLatch.countDown();
                        }
                    });
        }

        startLatch.countDown();
        Thread.sleep(durationSeconds * 1000L);
        executor.shutdownNow();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        return new HtapResult(
                oltpOps.sum(),
                oltpErrors.sum(),
                percentile(oltpLatencies, 50),
                percentile(oltpLatencies, 95),
                percentile(oltpLatencies, 99),
                olapOps.sum(),
                olapErrors.sum(),
                percentile(olapLatencies, 50),
                percentile(olapLatencies, 95),
                percentile(olapLatencies, 99));
    }

    private long percentile(List<Long> latencies, int p) {
        if (latencies.isEmpty()) return 0;
        List<Long> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);
        int idx = (int) Math.ceil((p / 100.0) * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(idx, sorted.size() - 1)));
    }

    private HttpClient createClient() {
        return HttpClient.newBuilder()
                .cookieHandler(new CookieManager())
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    private void executeSqlSync(HttpClient client, String sql)
            throws IOException, InterruptedException {
        String escaped = sql.replace("\\", "\\\\").replace("\"", "\\\"");
        String body = "{\"sql\":\"" + escaped + "\"}";
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
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .timeout(REQUEST_TIMEOUT)
                        .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 400) {
            throw new IOException("Query failed: " + resp.statusCode());
        }
    }

    public record HtapResult(
            long oltpOps,
            long oltpErrors,
            long oltpP50,
            long oltpP95,
            long oltpP99,
            long olapOps,
            long olapErrors,
            long olapP50,
            long olapP95,
            long olapP99) {
        @Override
        public String toString() {
            return String.format(
                    "OLTP: ops=%d errors=%d p50=%dms p95=%dms p99=%dms | OLAP: ops=%d errors=%d p50=%dms p95=%dms p99=%dms",
                    oltpOps,
                    oltpErrors,
                    oltpP50,
                    oltpP95,
                    oltpP99,
                    olapOps,
                    olapErrors,
                    olapP50,
                    olapP95,
                    olapP99);
        }
    }
}
