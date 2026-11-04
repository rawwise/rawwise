package io.github.rawwise.benchmark.ycsb;

import io.github.rawwise.benchmark.BenchmarkConfig;
import io.github.rawwise.benchmark.BenchmarkSuite;
import io.github.rawwise.benchmark.LatencyStats;
import io.github.rawwise.benchmark.QueryBenchmarkResult;
import io.github.rawwise.engine.storage.DynamicMmapStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

public class YcsbBenchmarkSuite implements BenchmarkSuite {

    private static final Logger LOG = Logger.getLogger(YcsbBenchmarkSuite.class.getName());

    private final String remoteHost;
    private final int remotePort;
    private final boolean frontend;
    private final String authToken;
    private final int clientId;
    private final int totalClients;
    private final String sharedTable;
    private final boolean waitForSignal;
    private final int signalTimeoutSeconds;

    public YcsbBenchmarkSuite() {
        this.remoteHost = null;
        this.remotePort = 0;
        this.frontend = false;
        this.authToken = null;
        this.clientId = 0;
        this.totalClients = 1;
        this.sharedTable = null;
        this.waitForSignal = false;
        this.signalTimeoutSeconds = 120;
    }

    public YcsbBenchmarkSuite(String host, int port) {
        this.remoteHost = host;
        this.remotePort = port;
        this.frontend = false;
        this.authToken = null;
        this.clientId = 0;
        this.totalClients = 1;
        this.sharedTable = null;
        this.waitForSignal = false;
        this.signalTimeoutSeconds = 120;
    }

    public YcsbBenchmarkSuite(String host, int port, boolean frontend, String authToken) {
        this.remoteHost = host;
        this.remotePort = port;
        this.frontend = frontend;
        this.authToken = authToken;
        this.clientId = 0;
        this.totalClients = 1;
        this.sharedTable = null;
        this.waitForSignal = false;
        this.signalTimeoutSeconds = 120;
    }

    public YcsbBenchmarkSuite(
            String host,
            int port,
            boolean frontend,
            String authToken,
            int clientId,
            int totalClients,
            String sharedTable,
            boolean waitForSignal,
            int signalTimeoutSeconds) {
        this.remoteHost = host;
        this.remotePort = port;
        this.frontend = frontend;
        this.authToken = authToken;
        this.clientId = clientId;
        this.totalClients = totalClients;
        this.sharedTable = sharedTable;
        this.waitForSignal = waitForSignal;
        this.signalTimeoutSeconds = signalTimeoutSeconds;
    }

    private boolean isRemote() {
        return remoteHost != null;
    }

    @Override
    public List<QueryBenchmarkResult> run(BenchmarkConfig config) {
        if (isRemote()) {
            if (waitForSignal
                    && !new DistributedYcsbCoordinator(remoteHost, remotePort, signalTimeoutSeconds)
                            .waitForReady()) {
                return Collections.emptyList();
            }
            return runRemote(config);
        }
        return runEmbedded(config);
    }

    private List<QueryBenchmarkResult> runEmbedded(BenchmarkConfig config) {
        Path tempDir = null;
        try {
            tempDir = Files.createTempDirectory("ycsb-benchmark-");
            String storeId = "ycsb-" + UUID.randomUUID().toString().substring(0, 8);
            DynamicMmapStore store =
                    DynamicMmapStore.getInstance(tempDir.toAbsolutePath().toString(), storeId);

            YcsbRawWiseClient client = new YcsbRawWiseClient(store);
            int recordCount = Math.max(config.scaleFactor() * 1000, 1000);
            client.init(recordCount);

            List<QueryBenchmarkResult> results = new ArrayList<>();
            for (String workloadName : config.queryIds()) {
                YcsbWorkload workload = parseWorkload(workloadName);
                if (workload != null) {
                    results.add(
                            runWorkloadRange(
                                    client, workload, workloadName, 0, recordCount, config));
                }
            }
            return results;
        } catch (Exception e) {
            LOG.severe("YCSB benchmark failed: " + e.getMessage());
            return List.of();
        } finally {
            if (tempDir != null) {
                try {
                    deleteRecursively(tempDir);
                } catch (IOException e) {
                    LOG.warning("Failed to cleanup temp dir: " + e.getMessage());
                }
            }
        }
    }

    private List<QueryBenchmarkResult> runRemote(BenchmarkConfig config) {
        String tableId;
        if (sharedTable != null && !sharedTable.isBlank()) {
            tableId = sharedTable;
        } else {
            tableId = "ycsb_usertable_" + UUID.randomUUID().toString().substring(0, 8);
        }
        YcsbClient client;
        if (frontend) {
            client = new FrontendYcsbClient(remoteHost, remotePort, "default", tableId, authToken);
        } else {
            client = new GrpcYcsbClient(remoteHost, remotePort, tableId);
        }
        try {
            int recordCount = Math.max(config.scaleFactor() * 1000, 1000);
            int keyStart, keyEnd;
            if (totalClients > 1) {
                int perClient = recordCount / totalClients;
                keyStart = clientId * perClient;
                keyEnd = (clientId + 1) * perClient;
                client.initRange(keyStart, keyEnd);
            } else {
                keyStart = 0;
                keyEnd = recordCount;
                client.init(recordCount);
            }

            List<QueryBenchmarkResult> results = new ArrayList<>();
            for (String workloadName : config.queryIds()) {
                YcsbWorkload workload = parseWorkload(workloadName);
                if (workload != null) {
                    results.add(
                            runWorkloadRange(
                                    client, workload, workloadName, keyStart, keyEnd, config));
                }
            }
            return results;
        } catch (Exception e) {
            LOG.severe("YCSB remote benchmark failed: " + e.getMessage());
            return List.of();
        } finally {
            try {
                client.close();
            } catch (Exception e) {
                // best-effort close
            }
        }
    }

    private QueryBenchmarkResult runWorkloadRange(
            YcsbClient client,
            YcsbWorkload workload,
            String workloadName,
            int keyStart,
            int keyEnd,
            BenchmarkConfig config) {
        int rangeSize = keyEnd - keyStart;
        int opsPerRound = Math.max(config.scaleFactor() * 100, 100);
        int threads = config.threads();
        Random rng = new Random(42 + clientId);

        ExecutorService executor = null;
        if (threads > 1) {
            executor = Executors.newFixedThreadPool(threads);
        }

        try {
            for (int w = 0; w < config.warmupRounds(); w++) {
                runOpsInRange(
                        client, workload, opsPerRound, keyStart, rangeSize, rng, executor, threads);
            }

            List<Long> latencies = new ArrayList<>();
            long totalRows = 0;

            for (int round = 0; round < config.measureRounds(); round++) {
                List<Long> roundLatencies =
                        runOpsInRange(
                                client,
                                workload,
                                opsPerRound,
                                keyStart,
                                rangeSize,
                                rng,
                                executor,
                                threads);
                latencies.addAll(roundLatencies);
                totalRows += roundLatencies.size();
            }

            LatencyStats stats = computeStats(latencies);
            LOG.info(
                    "YCSB "
                            + workloadName
                            + ": median="
                            + stats.median()
                            + "us p95="
                            + stats.p95()
                            + "us samples="
                            + latencies.size()
                            + " threads="
                            + threads);

            return new QueryBenchmarkResult(
                    workloadName,
                    config.measureRounds(),
                    stats,
                    totalRows,
                    "true",
                    "N/A",
                    null,
                    0,
                    latencies.isEmpty()
                            ? 0L
                            : (long) latencies.stream().mapToLong(l -> l).average().orElse(0),
                    "ycsb");
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }
    }

    private List<Long> runOpsInRange(
            YcsbClient client,
            YcsbWorkload workload,
            int opsPerRound,
            int keyStart,
            int rangeSize,
            Random rng,
            ExecutorService executor,
            int threads) {
        if (threads <= 1 || executor == null) {
            return runOpsSerial(client, workload, opsPerRound, keyStart, rangeSize, rng);
        }

        int opsPerThread = opsPerRound / threads;
        int remainder = opsPerRound % threads;
        List<Callable<List<Long>>> tasks = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int ops = opsPerThread + (t < remainder ? 1 : 0);
            tasks.add(
                    () ->
                            runOpsSerial(
                                    client,
                                    workload,
                                    ops,
                                    keyStart,
                                    rangeSize,
                                    ThreadLocalRandom.current()));
        }

        try {
            List<Future<List<Long>>> futures = executor.invokeAll(tasks);
            List<Long> latencies = new ArrayList<>();
            for (Future<List<Long>> f : futures) {
                latencies.addAll(f.get());
            }
            return latencies;
        } catch (Exception e) {
            LOG.severe("Concurrent YCSB execution failed: " + e.getMessage());
            return List.of();
        }
    }

    private List<Long> runOpsSerial(
            YcsbClient client,
            YcsbWorkload workload,
            int ops,
            int keyStart,
            int rangeSize,
            Random rng) {
        List<Long> latencies = new ArrayList<>(ops);
        double r = workload.readRatio();
        double w = workload.writeRatio();
        double s = workload.scanRatio();
        double readThreshold = r;
        double writeThreshold = r + w;
        double scanThreshold = r + w + s;

        for (int i = 0; i < ops; i++) {
            double dice = rng.nextDouble();
            int key = keyStart + rng.nextInt(rangeSize);

            long start = System.nanoTime();
            try {
                if (dice < readThreshold) {
                    client.read(key);
                } else if (dice < writeThreshold) {
                    client.write(key);
                } else if (dice < scanThreshold) {
                    client.scan(key);
                } else {
                    client.insert(keyStart + rangeSize + i);
                }
            } catch (IOException e) {
                LOG.fine("YCSB op failed: " + e.getMessage());
            }
            long elapsedNs = System.nanoTime() - start;
            latencies.add(elapsedNs / 1_000);
        }
        return latencies;
    }

    private static YcsbWorkload parseWorkload(String name) {
        try {
            return YcsbWorkload.valueOf(name.toUpperCase());
        } catch (IllegalArgumentException e) {
            LOG.warning("Unknown YCSB workload: " + name);
            return null;
        }
    }

    private static LatencyStats computeStats(List<Long> latencies) {
        if (latencies.isEmpty()) {
            return new LatencyStats(0, 0, 0);
        }
        List<Long> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);
        int n = sorted.size();
        double median = percentile(sorted, 50);
        double p95 = percentile(sorted, 95);
        double p99 = percentile(sorted, 99);
        return new LatencyStats(median, p95, p99);
    }

    private static double percentile(List<Long> sorted, double percentile) {
        int n = sorted.size();
        double index = percentile / 100.0 * (n - 1);
        int lower = (int) Math.floor(index);
        int upper = (int) Math.ceil(index);
        if (lower == upper) {
            return sorted.get(lower);
        }
        double weight = index - lower;
        return sorted.get(lower) * (1 - weight) + sorted.get(upper) * weight;
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (var entries = Files.list(path)) {
                for (Path entry : entries.toList()) {
                    deleteRecursively(entry);
                }
            }
        }
        Files.deleteIfExists(path);
    }
}
