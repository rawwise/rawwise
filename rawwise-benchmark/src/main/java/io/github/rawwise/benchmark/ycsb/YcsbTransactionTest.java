package io.github.rawwise.benchmark.ycsb;

import io.github.rawwise.client.repl.FrontendHttpClient;
import io.github.rawwise.frontend.api.dto.QueryResponseDto;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * End-to-end write and read performance tests through Frontend HTTP API.
 *
 * <p>Path: Test → HTTP → Frontend → Backend → LMDB
 *
 * <p>Scenarios: 1. Sequential INSERT throughput 2. Sequential SELECT (point query) throughput 3.
 * Read-after-write consistency 4. Concurrent INSERT (2 threads, different key ranges) 5. Mixed
 * sequential read-write
 */
public class YcsbTransactionTest {

    private static final int TIMEOUT_SECONDS = 30;

    private final FrontendHttpClient httpClient;
    private final String database;
    private final String table;

    public YcsbTransactionTest(String host, int port, String database) {
        this.httpClient = new FrontendHttpClient(host, port);
        this.database = database;
        this.table = "perf_test_" + UUID.randomUUID().toString().substring(0, 8);
    }

    public void setup() throws Exception {
        String sql =
                "CREATE TABLE IF NOT EXISTS "
                        + database
                        + "."
                        + table
                        + " (id INT, balance INT) USING rawwise TBLPROPERTIES ('primary-key'='id')";
        executeOrThrow(sql, "create table");
        System.out.println("[SETUP] Table " + database + "." + table + " created.");
    }

    public void teardown() {
        try {
            executeOrThrow("DROP TABLE IF EXISTS " + database + "." + table, "drop table");
            System.out.println("[TEARDOWN] Table dropped.");
        } catch (Exception e) {
            System.out.println("[TEARDOWN] Cleanup failed: " + e.getMessage());
        }
    }

    public boolean testSequentialInsertThroughput() throws Exception {
        System.out.println("\n--- Test 1: Sequential INSERT Throughput ---");

        int numWrites = 100;
        List<Long> latencies = new ArrayList<>();
        int successCount = 0;

        for (int i = 0; i < numWrites; i++) {
            String sql =
                    "INSERT INTO "
                            + database
                            + "."
                            + table
                            + " (id, balance) VALUES ("
                            + i
                            + ", "
                            + (i * 10)
                            + ")";
            long start = System.nanoTime();
            QueryResponseDto resp = httpClient.executeSync(sql, database, TIMEOUT_SECONDS);
            long elapsed = (System.nanoTime() - start) / 1_000;
            if ("SUCCEEDED".equals(resp.getState())) {
                successCount++;
                latencies.add(elapsed);
            }
        }

        printLatencyStats("Sequential INSERT", latencies);
        double elapsedMs = latencies.stream().mapToLong(l -> l).sum() / 1_000.0;
        double throughput = successCount / (elapsedMs / 1_000.0);
        System.out.println("  Successes: " + successCount + "/" + numWrites);
        System.out.println("  Throughput: " + String.format("%.1f", throughput) + " ops/s");
        return successCount == numWrites;
    }

    public boolean testSequentialSelectThroughput() throws Exception {
        System.out.println("\n--- Test 2: Sequential SELECT (point query) Throughput ---");

        int numReads = 50;
        List<Long> latencies = new ArrayList<>();
        int successCount = 0;
        int correctCount = 0;

        for (int i = 0; i < numReads; i++) {
            int id = i * 2;
            String sql = "SELECT balance FROM " + database + "." + table + " WHERE id = " + id;
            long start = System.nanoTime();
            QueryResponseDto resp = httpClient.executeSync(sql, database, TIMEOUT_SECONDS);
            long elapsed = (System.nanoTime() - start) / 1_000;
            if ("SUCCEEDED".equals(resp.getState())) {
                successCount++;
                latencies.add(elapsed);
                if (resp.getData() != null
                        && resp.getData().getRows() != null
                        && !resp.getData().getRows().isEmpty()) {
                    Object val = resp.getData().getRows().get(0).get(0);
                    int expected = id * 10;
                    if (String.valueOf(val).equals(String.valueOf(expected))) {
                        correctCount++;
                    }
                }
            }
        }

        printLatencyStats("Sequential SELECT", latencies);
        double elapsedMs = latencies.stream().mapToLong(l -> l).sum() / 1_000.0;
        double throughput = successCount / (elapsedMs / 1_000.0);
        System.out.println("  Successes: " + successCount + "/" + numReads);
        System.out.println("  Correctness: " + correctCount + "/" + successCount);
        System.out.println("  Throughput: " + String.format("%.1f", throughput) + " ops/s");
        return successCount == numReads && correctCount == successCount;
    }

    public boolean testReadAfterWriteConsistency() throws Exception {
        System.out.println("\n--- Test 3: Read-After-Write Consistency ---");

        int numChecks = 20;
        int passCount = 0;

        for (int i = 0; i < numChecks; i++) {
            int id = 1000 + i;
            int expectedBalance = i * 7;

            String insertSql =
                    "INSERT INTO "
                            + database
                            + "."
                            + table
                            + " (id, balance) VALUES ("
                            + id
                            + ", "
                            + expectedBalance
                            + ")";
            QueryResponseDto insertResp =
                    httpClient.executeSync(insertSql, database, TIMEOUT_SECONDS);
            if (!"SUCCEEDED".equals(insertResp.getState())) {
                System.out.println("  Check " + i + ": INSERT failed");
                continue;
            }

            String selectSql =
                    "SELECT balance FROM " + database + "." + table + " WHERE id = " + id;
            QueryResponseDto selectResp =
                    httpClient.executeSync(selectSql, database, TIMEOUT_SECONDS);
            if (selectResp.getData() != null
                    && selectResp.getData().getRows() != null
                    && !selectResp.getData().getRows().isEmpty()) {
                Object val = selectResp.getData().getRows().get(0).get(0);
                int readBalance = Integer.parseInt(String.valueOf(val));
                if (readBalance == expectedBalance) {
                    passCount++;
                } else {
                    System.out.println(
                            "  Check "
                                    + i
                                    + ": MISMATCH expected="
                                    + expectedBalance
                                    + " got="
                                    + readBalance);
                }
            } else {
                System.out.println("  Check " + i + ": SELECT returned no rows");
            }
        }

        System.out.println("  Consistency checks: " + passCount + "/" + numChecks + " passed");
        return passCount == numChecks;
    }

    public boolean testConcurrentInsert() throws Exception {
        System.out.println("\n--- Test 4: Concurrent INSERT (2 threads, different keys) ---");

        int numThreads = 2;
        int writesPerThread = 20;
        int totalWrites = numThreads * writesPerThread;
        AtomicInteger successCount = new AtomicInteger(0);
        List<Long> allLatencies = Collections.synchronizedList(new ArrayList<>());

        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(numThreads);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(
                    () -> {
                        try {
                            latch.await();
                            for (int i = 0; i < writesPerThread; i++) {
                                int id = 2000 + threadId * writesPerThread + i;
                                String sql =
                                        "INSERT INTO "
                                                + database
                                                + "."
                                                + table
                                                + " (id, balance) VALUES ("
                                                + id
                                                + ", "
                                                + (threadId * 100 + i)
                                                + ")";
                                long start = System.nanoTime();
                                try {
                                    QueryResponseDto resp =
                                            httpClient.executeSync(sql, database, TIMEOUT_SECONDS);
                                    long elapsed = (System.nanoTime() - start) / 1_000;
                                    if ("SUCCEEDED".equals(resp.getState())) {
                                        successCount.incrementAndGet();
                                        allLatencies.add(elapsed);
                                    }
                                } catch (Exception e) {
                                    // ignore
                                }
                            }
                        } catch (InterruptedException ignored) {
                        } finally {
                            done.countDown();
                        }
                    });
        }

        long wallStart = System.nanoTime();
        latch.countDown();
        done.await(120, TimeUnit.SECONDS);
        long wallElapsedUs = (System.nanoTime() - wallStart) / 1_000;
        executor.shutdown();

        double wallMs = wallElapsedUs / 1_000.0;
        int successes = successCount.get();
        double throughput = successes / (wallMs / 1_000.0);

        printLatencyStats("Concurrent INSERT", allLatencies);
        System.out.println("  Threads: " + numThreads + ", writes/thread: " + writesPerThread);
        System.out.println("  Successes: " + successes + "/" + totalWrites);
        System.out.println("  Wall time: " + String.format("%.1f", wallMs) + " ms");
        System.out.println("  Throughput: " + String.format("%.1f", throughput) + " ops/s");

        return successes == totalWrites;
    }

    public boolean testMixedSequentialReadWrite() throws Exception {
        System.out.println("\n--- Test 5: Mixed Sequential Read-Write ---");

        int numOps = 40;
        List<Long> writeLatencies = new ArrayList<>();
        List<Long> readLatencies = new ArrayList<>();
        int writeSuccess = 0;
        int readSuccess = 0;

        for (int i = 0; i < numOps; i++) {
            if (i % 2 == 0) {
                int id = 3000 + i;
                String sql =
                        "INSERT INTO "
                                + database
                                + "."
                                + table
                                + " (id, balance) VALUES ("
                                + id
                                + ", "
                                + i
                                + ")";
                long start = System.nanoTime();
                QueryResponseDto resp = httpClient.executeSync(sql, database, TIMEOUT_SECONDS);
                long elapsed = (System.nanoTime() - start) / 1_000;
                if ("SUCCEEDED".equals(resp.getState())) {
                    writeSuccess++;
                    writeLatencies.add(elapsed);
                }
            } else {
                int id = 3000 + i - 1;
                String sql = "SELECT balance FROM " + database + "." + table + " WHERE id = " + id;
                long start = System.nanoTime();
                QueryResponseDto resp = httpClient.executeSync(sql, database, TIMEOUT_SECONDS);
                long elapsed = (System.nanoTime() - start) / 1_000;
                if ("SUCCEEDED".equals(resp.getState())) {
                    readSuccess++;
                    readLatencies.add(elapsed);
                }
            }
        }

        printLatencyStats("Write (mixed)", writeLatencies);
        printLatencyStats("Read (mixed)", readLatencies);
        System.out.println(
                "  Write successes: " + writeSuccess + ", Read successes: " + readSuccess);
        return writeSuccess + readSuccess >= numOps * 3 / 4;
    }

    private void printLatencyStats(String label, List<Long> latenciesUs) {
        if (latenciesUs.isEmpty()) {
            System.out.println("  " + label + ": no samples");
            return;
        }
        List<Long> sorted = new ArrayList<>(latenciesUs);
        Collections.sort(sorted);
        int n = sorted.size();
        double median = percentile(sorted, 50);
        double p95 = percentile(sorted, 95);
        double p99 = percentile(sorted, 99);
        double avg = sorted.stream().mapToLong(l -> l).average().orElse(0);
        System.out.println(
                "  "
                        + label
                        + ": n="
                        + n
                        + " median="
                        + String.format("%.0f", median)
                        + "us"
                        + " p95="
                        + String.format("%.0f", p95)
                        + "us"
                        + " p99="
                        + String.format("%.0f", p99)
                        + "us"
                        + " avg="
                        + String.format("%.0f", avg)
                        + "us");
    }

    private static double percentile(List<Long> sorted, double pct) {
        int n = sorted.size();
        double index = pct / 100.0 * (n - 1);
        int lower = (int) Math.floor(index);
        int upper = (int) Math.ceil(index);
        if (lower == upper) return sorted.get(lower);
        double w = index - lower;
        return sorted.get(lower) * (1 - w) + sorted.get(upper) * w;
    }

    private void executeOrThrow(String sql, String label) throws Exception {
        QueryResponseDto resp = httpClient.executeSync(sql, database, TIMEOUT_SECONDS);
        if (!"SUCCEEDED".equals(resp.getState())) {
            String err = resp.getError() != null ? resp.getError().getMessage() : resp.getState();
            throw new RuntimeException(label + " failed: " + err);
        }
    }

    public void close() {
        httpClient.close();
    }

    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int port = 8080;
        String database = "default";

        for (String arg : args) {
            if (arg.startsWith("--host=")) host = arg.substring(7);
            else if (arg.startsWith("--port=")) port = Integer.parseInt(arg.substring(7));
            else if (arg.startsWith("--db=")) database = arg.substring(5);
        }

        System.out.println("=== YCSB Write & Read Performance Tests ===");
        System.out.println("Frontend: " + host + ":" + port);
        System.out.println("Database: " + database);

        YcsbTransactionTest test = new YcsbTransactionTest(host, port, database);

        int pass = 0, fail = 0;
        try {
            test.setup();

            if (test.testSequentialInsertThroughput()) pass++;
            else fail++;
            if (test.testSequentialSelectThroughput()) pass++;
            else fail++;
            if (test.testReadAfterWriteConsistency()) pass++;
            else fail++;
            if (test.testConcurrentInsert()) pass++;
            else fail++;
            if (test.testMixedSequentialReadWrite()) pass++;
            else fail++;
        } finally {
            test.teardown();
            test.close();
        }

        System.out.println("\n=== Results: " + pass + " passed, " + fail + " failed ===");
        System.exit(fail > 0 ? 1 : 0);
    }
}
