package io.github.rawwise.benchmark;

// import io.github.rawwise.benchmark.storage.LsmTreeBenchmarkSuite;
import io.github.rawwise.benchmark.chbenchmark.ChBenchmarkSuite;
import io.github.rawwise.benchmark.tpcds.TpcdsBenchmarkSuite;
import io.github.rawwise.benchmark.tpcds.TpcdsCorrectnessChecker;
import io.github.rawwise.benchmark.tpcds.TpcdsQuerySet;
import io.github.rawwise.benchmark.tpch.TpchBenchmarkSuite;
import io.github.rawwise.benchmark.tpch.TpchQuerySet;
import io.github.rawwise.benchmark.ycsb.YcsbBenchmarkSuite;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.SparkSession;

public final class BenchmarkRunner {

    private static final String DEFAULT_FRONTEND_HOST = "127.0.0.1";
    private static final int DEFAULT_FRONTEND_PORT = 8080;
    private static final int DEFAULT_TIMEOUT_SECONDS = 300;
    private static final String DEFAULT_TPCDS_DATA_DIR = "./benchmark/data/tpcds";

    private BenchmarkRunner() {}

    public static void main(String[] args) {
        Map<String, String> opts = parseArgs(args);

        String suiteName = opts.getOrDefault("suite", "tpch").toLowerCase();
        int sf = Integer.parseInt(opts.getOrDefault("sf", "10"));
        int warmup = Integer.parseInt(opts.getOrDefault("warmup", "3"));
        int measure = Integer.parseInt(opts.getOrDefault("measure", "5"));
        String host =
                opts.getOrDefault(
                        "host",
                        System.getenv()
                                .getOrDefault("RAWWISE_FRONTEND_HOST", DEFAULT_FRONTEND_HOST));
        int port =
                Integer.parseInt(
                        opts.getOrDefault(
                                "port",
                                System.getenv()
                                        .getOrDefault(
                                                "RAWWISE_FRONTEND_PORT",
                                                String.valueOf(DEFAULT_FRONTEND_PORT))));
        String tpcdsDataDir =
                opts.getOrDefault(
                        "data-dir",
                        System.getenv().getOrDefault("RAWWISE_TPCDS_DATA", DEFAULT_TPCDS_DATA_DIR));
        int timeout =
                Integer.parseInt(
                        opts.getOrDefault("timeout", String.valueOf(DEFAULT_TIMEOUT_SECONDS)));

        Set<String> queryIds;
        String queryIdsArg = opts.get("queries");
        if (queryIdsArg != null && !queryIdsArg.isBlank()) {
            queryIds = Set.of(queryIdsArg.split(","));
        } else {
            queryIds = defaultQueryIds(suiteName);
        }

        String engineType = resolveEngineType(suiteName);
        boolean remote = Boolean.parseBoolean(opts.getOrDefault("remote", "false"));
        boolean frontend = Boolean.parseBoolean(opts.getOrDefault("frontend", "false"));
        int clientId = Integer.parseInt(opts.getOrDefault("client-id", "0"));
        int totalClients = Integer.parseInt(opts.getOrDefault("total-clients", "1"));
        String sharedTable = opts.getOrDefault("shared-table", null);
        boolean waitForSignal = Boolean.parseBoolean(opts.getOrDefault("wait-for-signal", "false"));
        int signalTimeout = Integer.parseInt(opts.getOrDefault("signal-timeout-seconds", "120"));
        int threads = Integer.parseInt(opts.getOrDefault("threads", "1"));
        BenchmarkConfig config =
                new BenchmarkConfig(sf, engineType, queryIds, warmup, measure, timeout, threads);

        String authToken = readBootstrapToken();
        if (authToken != null) {
            System.out.println("  Auth: using bootstrap token");
        } else {
            System.out.println("  Auth: no bootstrap token found, requests may be rejected");
        }

        SparkSession spark = null;
        if ("tpcds".equals(suiteName)) {
            spark = createLocalSparkSession();
            System.out.println(
                    "  Correctness: Spark ground truth enabled (data-dir=" + tpcdsDataDir + ")");
        }

        try {
            BenchmarkSuite suite =
                    createSuite(
                            suiteName,
                            host,
                            port,
                            authToken,
                            spark,
                            tpcdsDataDir,
                            remote,
                            frontend,
                            clientId,
                            totalClients,
                            sharedTable,
                            waitForSignal,
                            signalTimeout);
            System.out.println("Running benchmark suite: " + suiteName);
            System.out.println(
                    "  scaleFactor="
                            + sf
                            + " warmup="
                            + warmup
                            + " measure="
                            + measure
                            + " engineType="
                            + engineType
                            + " threads="
                            + threads);
            if (totalClients > 1) {
                System.out.println("  distributed: client " + clientId + "/" + totalClients);
            }

            List<QueryBenchmarkResult> results = suite.run(config);
            BenchmarkReport report = new BenchmarkReport(config, results);

            String json = report.toJson();
            System.out.println(json);

            String timestamp =
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
            Path resultsDir = Paths.get("benchmark", suiteName, "results");
            try {
                Files.createDirectories(resultsDir);
            } catch (IOException ignored) {
            }
            Path outputFile =
                    resultsDir.resolve("benchmark-" + suiteName + "-" + timestamp + ".json");
            try {
                Files.writeString(outputFile, json);
                System.out.println("Report written to: " + outputFile.toAbsolutePath());
            } catch (IOException e) {
                System.err.println("Failed to write report file: " + e.getMessage());
            }
        } finally {
            if (spark != null) {
                spark.stop();
            }
        }
    }

    private static BenchmarkSuite createSuite(
            String suiteName,
            String host,
            int port,
            String authToken,
            SparkSession spark,
            String tpcdsDataDir,
            boolean remote,
            boolean frontend,
            int clientId,
            int totalClients,
            String sharedTable,
            boolean waitForSignal,
            int signalTimeout) {
        return switch (suiteName) {
            case "tpch" -> new TpchBenchmarkSuite(host, port, authToken, new TpchQuerySet());
            case "tpcds" -> {
                TpcdsCorrectnessChecker checker = null;
                if (spark != null && Files.isDirectory(Path.of(tpcdsDataDir))) {
                    checker = new TpcdsCorrectnessChecker(spark, tpcdsDataDir);
                }
                yield new TpcdsBenchmarkSuite(host, port, authToken, new TpcdsQuerySet(), checker);
            }
            case "ycsb" ->
                    remote
                            ? new YcsbBenchmarkSuite(
                                    host,
                                    port,
                                    frontend,
                                    authToken,
                                    clientId,
                                    totalClients,
                                    sharedTable,
                                    waitForSignal,
                                    signalTimeout)
                            : new YcsbBenchmarkSuite();
            case "chbenchmark" ->
                    new ChBenchmarkSuite(
                            host,
                            port,
                            remote,
                            clientId,
                            totalClients,
                            sharedTable,
                            waitForSignal,
                            signalTimeout);
            case "lsm-tree" ->
                    throw new UnsupportedOperationException("lsm-tree suite unavailable");
            default ->
                    throw new IllegalArgumentException(
                            "Unknown suite: "
                                    + suiteName
                                    + ". Supported: tpch, tpcds, ycsb, chbenchmark, lsm-tree");
        };
    }

    private static SparkSession createLocalSparkSession() {
        return SparkSession.builder()
                .appName("TpcdsCorrectnessChecker")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "true")
                .config(
                        "spark.driver.extraJavaOptions",
                        "--add-opens java.base/java.nio=ALL-UNNAMED "
                                + "--add-opens java.base/sun.nio.ch=ALL-UNNAMED "
                                + "--add-opens java.base/java.lang=ALL-UNNAMED "
                                + "--add-opens java.base/java.lang.invoke=ALL-UNNAMED")
                .getOrCreate();
    }

    private static Set<String> defaultQueryIds(String suiteName) {
        return switch (suiteName) {
            case "tpch" ->
                    Set.of(
                            "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
                            "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21",
                            "q22");
            case "tpcds" ->
                    Set.of(
                            "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
                            "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21",
                            "q22", "q23", "q24", "q25");
            case "ycsb" -> Set.of("A", "B", "C");
            case "chbenchmark" -> Set.of("all");
            case "lsm-tree" -> Set.of("WALWriteLatency", "FlushLatency", "CompactionThroughput");
            default -> Set.of();
        };
    }

    private static String resolveEngineType(String suiteName) {
        return switch (suiteName) {
            case "ycsb" -> "ycsb";
            case "chbenchmark" -> "chbenchmark";
            case "lsm-tree" -> "lsm-tree";
            default -> "rawwise";
        };
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> opts = new HashMap<>();
        for (String arg : args) {
            if (arg.startsWith("--")) {
                int eqIdx = arg.indexOf('=');
                if (eqIdx > 2) {
                    String key = arg.substring(2, eqIdx);
                    String value = arg.substring(eqIdx + 1);
                    opts.put(key, value);
                }
            }
        }
        return opts;
    }

    private static String readBootstrapToken() {
        for (String dir :
                List.of(
                        System.getProperty("rawwise.auth.token-dir", "/run/rawwise"),
                        System.getProperty("java.io.tmpdir") + "/rawwise")) {
            try {
                java.nio.file.Path tokenFile = java.nio.file.Path.of(dir, "bootstrap-token");
                if (java.nio.file.Files.exists(tokenFile)) {
                    return java.nio.file.Files.readString(tokenFile).trim();
                }
            } catch (Exception ignored) {
            }
        }
        return null;
    }
}
