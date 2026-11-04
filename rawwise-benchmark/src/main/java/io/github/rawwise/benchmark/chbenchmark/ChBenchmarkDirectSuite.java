package io.github.rawwise.benchmark.chbenchmark;

import io.github.rawwise.benchmark.BenchmarkConfig;
import io.github.rawwise.benchmark.BenchmarkSuite;
import io.github.rawwise.benchmark.LatencyStats;
import io.github.rawwise.benchmark.QueryBenchmarkResult;
import io.github.rawwise.engine.storage.*;
import io.github.rawwise.engine.storage.block.*;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import io.github.rawwise.engine.storage.table.read.LmdbTableReader;
import io.github.rawwise.engine.txn.SimpleTableRef;
import io.github.rawwise.model.op.*;
import io.github.rawwise.model.record.PrimaryKey;
import io.github.rawwise.model.record.PrimaryKeyValue;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

/**
 * CH-benCHmark direct LMDB benchmark (bypasses HTTP/gRPC/Spark, tests real transaction latency
 * directly).
 *
 * <p>use {@link DynamicMmapStore} of write/read/delete path,covercompletetransaction: NewOrder
 * (INSERT x2), Payment (UPDATE x3 cross-table), Delivery (UPDATE), OrderStatus (SELECT), StockLevel
 * (SELECT COUNT)
 */
public class ChBenchmarkDirectSuite implements BenchmarkSuite {

    private static final Logger LOG = Logger.getLogger(ChBenchmarkDirectSuite.class.getName());

    @Override
    public List<QueryBenchmarkResult> run(BenchmarkConfig config) {
        int warehouseCount = Math.max(1, config.scaleFactor());
        int threads = Math.max(1, config.threads());
        Path tempDir = null;
        DynamicMmapStore store = null;

        try {
            tempDir = Files.createTempDirectory("ch-bench-direct-");
            String storeId = "ch-" + UUID.randomUUID().toString().substring(0, 8);
            store = DynamicMmapStore.getInstance(tempDir.toAbsolutePath().toString(), storeId);

            ChDirectClient client = new ChDirectClient(store);
            LOG.info(
                    "CH-benCHmark-Direct starting: warehouses="
                            + warehouseCount
                            + " threads="
                            + threads);
            client.init(warehouseCount);

            ExecutorService pool = Executors.newFixedThreadPool(threads);
            Random rng = new Random(42);
            int batchSize = 20;

            // Warmup
            long warmupEnd = System.currentTimeMillis() + config.warmupRounds() * 1000L;
            while (System.currentTimeMillis() < warmupEnd) {
                List<Future<?>> b = new ArrayList<>();
                for (int i = 0; i < batchSize; i++) {
                    long wId = 1 + rng.nextLong(warehouseCount);
                    b.add(
                            pool.submit(
                                    () -> {
                                        try {
                                            client.executeMix(wId, rng.nextDouble());
                                        } catch (Exception ignored) {
                                        }
                                    }));
                }
                for (Future<?> f : b) {
                    try {
                        f.get();
                    } catch (Exception ignored) {
                    }
                }
            }

            // Measurement
            List<Double> latencies = Collections.synchronizedList(new ArrayList<>());
            LongAdder errorCount = new LongAdder();
            long measureEnd = System.currentTimeMillis() + config.measureRounds() * 1000L;
            long measuredOps = 0;

            while (System.currentTimeMillis() < measureEnd) {
                List<Future<?>> b = new ArrayList<>();
                for (int i = 0; i < batchSize; i++) {
                    double roll = rng.nextDouble();
                    long wId = 1 + rng.nextLong(warehouseCount);
                    b.add(
                            pool.submit(
                                    () -> {
                                        long start = System.nanoTime();
                                        try {
                                            client.executeMix(wId, roll);
                                            latencies.add(
                                                    (System.nanoTime() - start)
                                                            / 1_000.0); // nanoseconds →
                                            // microseconds
                                        } catch (Exception e) {
                                            errorCount.increment();
                                        }
                                    }));
                }
                for (Future<?> f : b) {
                    try {
                        f.get();
                    } catch (Exception ignored) {
                    }
                }
                measuredOps += batchSize;
            }
            pool.shutdown();

            double[] sorted =
                    latencies.stream().sorted().mapToDouble(Double::doubleValue).toArray();
            double p50 = sorted.length > 0 ? sorted[sorted.length / 2] : 0;
            double p95 = sorted.length > 0 ? sorted[(int) (sorted.length * 0.95)] : 0;
            double p99 = sorted.length > 0 ? sorted[(int) (sorted.length * 0.99)] : 0;
            double throughput = measuredOps / (double) config.measureRounds();
            long errors = errorCount.sum();

            LOG.info(
                    "CH-benCHmark-Direct: ops="
                            + measuredOps
                            + " errors="
                            + errors
                            + " throughput="
                            + String.format("%.0f", throughput)
                            + "/s"
                            + " p50="
                            + String.format("%.1f", p50)
                            + "us p95="
                            + String.format("%.1f", p95)
                            + "us p99="
                            + String.format("%.1f", p99)
                            + "us");

            return List.of(
                    new QueryBenchmarkResult(
                            "chbenchmark-direct",
                            (int) measuredOps,
                            new LatencyStats(p50, p95, p99),
                            0,
                            errors == 0 ? "OK" : errors + " errors",
                            "LMDB_DIRECT",
                            null,
                            0,
                            0,
                            "LMDB"));

        } catch (Exception e) {
            LOG.severe("CH-benCHmark-Direct failed: " + e.getMessage());
            return List.of(
                    new QueryBenchmarkResult(
                            "chbenchmark-direct",
                            0,
                            new LatencyStats(0, 0, 0),
                            0,
                            "FAIL",
                            null,
                            e.getMessage(),
                            0,
                            0,
                            "LMDB"));
        } finally {
            if (store != null) {
                try {
                    store.close();
                } catch (Exception ignored) {
                }
            }
            if (tempDir != null) {
                try {
                    Files.deleteIfExists(tempDir);
                } catch (Exception ignored) {
                }
            }
        }
    }

    // ================================================================
    // Direct LMDB Client
    // ================================================================

    static class ChDirectClient {
        private static final RoutingOwnerContext OWNER = new RoutingOwnerContext("ch-exec", 1);

        private final DynamicMmapStore store;
        private final Map<String, TableStore> tables = new HashMap<>();
        private final Map<String, KeyCodec> codecs = new HashMap<>();
        private final Map<String, LmdbTableReader> readers = new HashMap<>();
        private final Map<String, List<LmdbInstance>> instances = new HashMap<>();

        ChDirectClient(DynamicMmapStore store) {
            this.store = store;
        }

        void init(int warehouseCount) throws IOException {
            registerTable("warehouse", List.of("w_id"));
            registerTable("district", List.of("d_w_id", "d_id"));
            registerTable("customer", List.of("c_w_id", "c_d_id", "c_id"));
            registerTable("orders", List.of("o_w_id", "o_d_id", "o_id"));
            registerTable("order_line", List.of("ol_w_id", "ol_d_id", "ol_o_id", "ol_number"));
            registerTable("stock", List.of("s_w_id", "s_i_id"));

            for (long w = 1; w <= warehouseCount; w++) {
                write(
                        "warehouse",
                        Map.of("w_id", w),
                        val(Map.of("w_name", "WH-" + w, "w_ytd", 3000000L)));
                for (long d = 1; d <= 10; d++) {
                    write(
                            "district",
                            Map.of("d_w_id", w, "d_id", d),
                            val(
                                    Map.of(
                                            "d_name",
                                            "D-" + w + "-" + d,
                                            "d_ytd",
                                            300000L,
                                            "d_next_o_id",
                                            1L)));
                }
            }
        }

        private void registerTable(String name, List<String> pkCols) {
            TableStore ts = store.tableStore(name, pkCols);
            tables.put(name, ts);
            codecs.put(name, ts.getKeyCodec());
            List<LmdbInstance> li = ts.getAllInstancesDescending();
            instances.put(name, li);
            readers.put(name, new LmdbTableReader(ts));
        }

        // ================================================================
        // Transaction Mix
        // ================================================================

        void executeMix(long wId, double roll) throws IOException {
            if (roll < 0.45) newOrder(wId);
            else if (roll < 0.88) payment(wId);
            else if (roll < 0.92) delivery(wId);
            else if (roll < 0.96) orderStatus(wId);
            else stockLevel(wId);
        }

        void newOrder(long wId) throws IOException {
            long dId = 1 + ThreadLocalRandom.current().nextLong(10);
            long oId = ThreadLocalRandom.current().nextLong(100_000);
            long cId = 1 + ThreadLocalRandom.current().nextLong(3_000);

            write(
                    "orders",
                    Map.of("o_w_id", wId, "o_d_id", dId, "o_id", oId),
                    val(Map.of("o_c_id", cId, "o_carrier_id", 0, "o_ol_cnt", 5)));
            write(
                    "order_line",
                    Map.of("ol_w_id", wId, "ol_d_id", dId, "ol_o_id", oId, "ol_number", 1L),
                    val(Map.of("ol_i_id", 1L, "ol_quantity", 5, "ol_amount", 10.0)));
        }

        void payment(long wId) throws IOException {
            long dId = 1 + ThreadLocalRandom.current().nextLong(10);
            long cId = 1 + ThreadLocalRandom.current().nextLong(3_000);
            double amount = 1 + ThreadLocalRandom.current().nextDouble() * 5000;

            // Cross-table: UPDATE 3 tables (checks postWriteHook firing)
            write("warehouse", Map.of("w_id", wId), val(Map.of("w_ytd", 3000000L + (long) amount)));
            write(
                    "district",
                    Map.of("d_w_id", wId, "d_id", dId),
                    val(Map.of("d_ytd", 300000L + (long) amount)));
            write(
                    "customer",
                    Map.of("c_w_id", wId, "c_d_id", dId, "c_id", cId),
                    val(Map.of("c_balance", -(long) amount, "c_ytd_payment", (long) amount)));
        }

        void delivery(long wId) throws IOException {
            long carrierId = 1 + ThreadLocalRandom.current().nextLong(10);
            write(
                    "orders",
                    Map.of("o_w_id", wId, "o_d_id", 1L, "o_id", 1L),
                    val(Map.of("o_carrier_id", carrierId)));
        }

        void orderStatus(long wId) throws IOException {
            read("orders", Map.of("o_w_id", wId, "o_d_id", 1L, "o_c_id", 1L));
        }

        void stockLevel(long wId) throws IOException {
            read("stock", Map.of("s_w_id", wId, "s_i_id", 1L));
        }

        // ================================================================
        // Write / Read primitives
        // ================================================================

        private void write(String tableName, Map<String, Comparable> pk, byte[] value)
                throws IOException {
            byte[] encKey = codecs.get(tableName).encodePrimaryKey(pk);
            KvWriteBuilder wb =
                    KvWriteBuilder.builder()
                            .table(new SimpleTableRef(tableName))
                            .key(encKey)
                            .value(value)
                            .partitionHint(0);
            store.write(wb, OWNER);
        }

        private void read(String tableName, Map<String, Comparable> pk) throws IOException {
            LmdbTableReader reader = readers.get(tableName);
            List<LmdbInstance> li = instances.get(tableName);
            String bid = li.isEmpty() ? "inst-dummy" : li.getFirst().instanceId();
            DataBlock block =
                    new HotDataBlock(
                            bid,
                            tableName,
                            DataBlock.Kind.MUTABLE_HOT,
                            DataBlock.Format.LMDB,
                            null,
                            null,
                            0,
                            0,
                            0,
                            DataBlock.Visibility.ACTIVE,
                            0,
                            0,
                            0,
                            0,
                            null);
            Find find =
                    Find.builder()
                            .withTable(new SimpleTableRef(tableName))
                            .withPrimaryKey(
                                    PrimaryKeyValue.of(
                                            PrimaryKey.of(pk.keySet().toArray(new String[0])), pk))
                            .withDataBlocks(List.of(block))
                            .withReader(reader)
                            .build();
            store.read(find);
        }

        private static byte[] val(Map<String, Object> fields) {
            StringBuilder sb = new StringBuilder();
            fields.forEach((k, v) -> sb.append(k).append("=").append(v).append(";"));
            return sb.toString().getBytes(StandardCharsets.UTF_8);
        }
    }

    // ================================================================
    // Main
    // ================================================================

    public static void main(String[] args) {
        int wh = args.length > 0 ? Integer.parseInt(args[0]) : 1;
        int th = args.length > 1 ? Integer.parseInt(args[1]) : 2;
        int wu = args.length > 2 ? Integer.parseInt(args[2]) : 5;
        int ms = args.length > 3 ? Integer.parseInt(args[3]) : 15;

        ChBenchmarkDirectSuite suite = new ChBenchmarkDirectSuite();
        BenchmarkConfig config = new BenchmarkConfig(wh, "LMDB", Set.of("all"), wu, ms, 120, th);
        List<QueryBenchmarkResult> results = suite.run(config);
        for (QueryBenchmarkResult r : results) {
            System.out.println(
                    "RESULT: "
                            + r.queryId()
                            + " ops="
                            + r.iterations()
                            + " p50="
                            + r.latencyUs().median()
                            + "us"
                            + " p95="
                            + r.latencyUs().p95()
                            + "us"
                            + " p99="
                            + r.latencyUs().p99()
                            + "us"
                            + " correctness="
                            + r.correctness());
        }
    }
}
