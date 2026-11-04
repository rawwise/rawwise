package io.github.rawwise.benchmark.loader;

import io.github.rawwise.client.repl.FrontendHttpClient;
import io.github.rawwise.frontend.api.dto.QueryResponseDto;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class TpcdsDataLoader {

    private static final Set<String> TPCDS_TABLES =
            Set.of(
                    "call_center",
                    "catalog_page",
                    "catalog_returns",
                    "catalog_sales",
                    "customer",
                    "customer_address",
                    "customer_demographics",
                    "date_dim",
                    "household_demographics",
                    "income_band",
                    "inventory",
                    "item",
                    "promotion",
                    "reason",
                    "ship_mode",
                    "store",
                    "store_returns",
                    "store_sales",
                    "time_dim",
                    "warehouse",
                    "web_page",
                    "web_returns",
                    "web_sales",
                    "web_site");

    private static final int LOAD_TIMEOUT_SECONDS = 600;

    private final String dataDir;
    private final FrontendHttpClient client;

    public TpcdsDataLoader(String dataDir, String host, int port) {
        this(dataDir, host, port, null);
    }

    public TpcdsDataLoader(String dataDir, String host, int port, String authToken) {
        this.dataDir = dataDir;
        this.client = new FrontendHttpClient(host, port);
    }

    public void load() {
        if (!client.healthCheck()) {
            throw new IllegalStateException("RawWise Frontend is not available");
        }

        log.info("Creating database tpcds ...");
        QueryResponseDto dbResult =
                client.executeSync("CREATE DATABASE IF NOT EXISTS tpcds", "default", 60);
        if (isFailed(dbResult)) {
            log.warn("Failed to create database tpcds: {}", errorOf(dbResult));
        }

        List<String> loaded = new ArrayList<>();
        List<String> skipped = new ArrayList<>();
        List<String> failed = new ArrayList<>();

        File dir = Path.of(dataDir).toFile();
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("Data directory does not exist: " + dataDir);
        }

        for (String table : TPCDS_TABLES) {
            File tableDir = Path.of(dataDir, table).toFile();
            if (!tableDir.isDirectory()
                    || tableDir.listFiles(f -> f.getName().endsWith(".parquet")) == null) {
                skipped.add(table);
                log.info("  SKIP {} (no parquet data)", table);
                continue;
            }

            log.info("  Loading {} ...", table);
            String sql =
                    String.format(
                            "CREATE TABLE IF NOT EXISTS tpcds.%s USING rawwise AS SELECT * FROM parquet.`%s`",
                            table, tableDir.getAbsolutePath());

            QueryResponseDto result = client.executeSync(sql, "tpcds", LOAD_TIMEOUT_SECONDS);
            if (isSucceeded(result)) {
                loaded.add(table);
                log.info("  OK   {}", table);
            } else {
                failed.add(table);
                log.warn("  FAIL {} : {}", table, errorOf(result));
            }
        }

        log.info("=== TPC-DS Data Loading Summary ===");
        log.info("Loaded:  {} tables {}", loaded.size(), loaded);
        log.info("Skipped: {} tables {}", skipped.size(), skipped);
        log.info("Failed:  {} tables {}", failed.size(), failed);

        if (!failed.isEmpty()) {
            log.warn("Some tables failed to load: {}", failed);
        }
    }

    public static void loadViaSpark(String dataDir, String warehousePath) {
        loadViaSpark(dataDir, warehousePath, TPCDS_TABLES);
    }

    public static void loadViaSpark(String dataDir, String warehousePath, Set<String> tables) {
        SparkSession spark =
                SparkSession.builder()
                        .appName("TpcdsDataLoader")
                        .master("local[1]")
                        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-tpcds")
                        .config(
                                "spark.sql.catalog.rawwise",
                                "io.github.rawwise.backend.spark.RawWiseSparkCatalog")
                        .config("spark.sql.catalog.rawwise.warehouse-path", warehousePath)
                        .config("spark.sql.catalog.rawwise.catalog-implementation", "metastore")
                        .config("spark.sql.catalog.rawwise.store-kind", "rocksdb")
                        .config("spark.rawwise.write.local", "true")
                        .config("spark.sql.adaptive.enabled", "true")
                        .config(
                                "spark.driver.extraJavaOptions",
                                "--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/sun.security.action=ALL-UNNAMED")
                        .config(
                                "spark.executor.extraJavaOptions",
                                "--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/sun.security.action=ALL-UNNAMED")
                        .getOrCreate();

        try {
            log.info("Creating database tpcds in rawwise catalog ...");
            spark.sql("CREATE DATABASE IF NOT EXISTS rawwise.tpcds");

            List<String> loaded = new ArrayList<>();
            List<String> skipped = new ArrayList<>();
            List<String> failed = new ArrayList<>();

            for (String table : tables) {
                File tableDir = Path.of(dataDir, table).toFile();
                if (!tableDir.isDirectory()) {
                    skipped.add(table);
                    log.info("  SKIP {} (no parquet data)", table);
                    continue;
                }

                log.info("  Loading {} ...", table);
                try {
                    Dataset<Row> df = spark.read().parquet(tableDir.getAbsolutePath());
                    df.writeTo("rawwise.tpcds." + table).using("rawwise").create();
                    loaded.add(table);
                    log.info("  OK   {}", table);
                } catch (Exception e) {
                    failed.add(table);
                    log.warn("  FAIL {} : {}", table, e.getMessage(), e);
                }
            }

            log.info("=== TPC-DS Data Loading Summary ===");
            log.info("Loaded:  {} tables {}", loaded.size(), loaded);
            log.info("Skipped: {} tables {}", skipped.size(), skipped);
            log.info("Failed:  {} tables {}", failed.size(), failed);

            if (!failed.isEmpty()) {
                log.warn("Some tables failed to load: {}", failed);
            }

            log.info("");
            log.info("=== Verifying loaded tables (same JVM) ===");
            int verified = 0;
            for (String table : loaded) {
                try {
                    Dataset<Row> df =
                            spark.sql("SELECT COUNT(*) AS cnt FROM rawwise.tpcds." + table);
                    long count = df.collectAsList().get(0).getLong(0);
                    verified++;
                    log.info("  VERIFY {} ({} rows)", table, count);
                } catch (Exception e) {
                    log.warn("  VERIFY FAIL {} : {}", table, e.getMessage());
                }
            }
            log.info("Verified: {}/{} tables readable", verified, loaded.size());
        } finally {
            spark.stop();
        }
    }

    private static boolean isSucceeded(QueryResponseDto result) {
        return result != null && "SUCCEEDED".equals(result.getState());
    }

    private static boolean isFailed(QueryResponseDto result) {
        return result == null
                || "FAILED".equals(result.getState())
                || "TIMED_OUT".equals(result.getState());
    }

    private static String errorOf(QueryResponseDto result) {
        if (result == null) return "null response";
        if (result.getError() != null) return result.getError().getMessage();
        return result.getState();
    }

    public static void main(String[] args) {
        String dataDir = "./benchmark/data/tpcds";
        String host = "127.0.0.1";
        int port = 8080;
        boolean sparkDirect = false;
        String warehousePath = "/tmp/rawwise/warehouse";
        Set<String> tables = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--data-dir" -> dataDir = args[++i];
                case "--host" -> host = args[++i];
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--spark-direct" -> sparkDirect = true;
                case "--warehouse-path" -> warehousePath = args[++i];
                case "--tables" -> tables = Set.of(args[++i].split(","));
                case "--help" -> {
                    System.out.println("Usage: TpcdsDataLoader [options]");
                    System.out.println(
                            "  --data-dir        Path to TPC-DS Parquet data (default: ./benchmark/data/tpcds)");
                    System.out.println(
                            "  --host            RawWise Frontend host (default: 127.0.0.1)");
                    System.out.println("  --port            RawWise Frontend port (default: 8080)");
                    System.out.println(
                            "  --spark-direct    Register tables directly via Spark (bypass Frontend)");
                    System.out.println(
                            "  --warehouse-path  RawWise warehouse path (default: /tmp/rawwise/warehouse)");
                    System.out.println(
                            "  --tables          Comma-separated list of tables to load (default: all)");
                    return;
                }
            }
        }

        log.info("TPC-DS Data Loader");
        log.info("  Data dir: {}", dataDir);

        if (sparkDirect) {
            log.info("  Mode: Spark Direct");
            log.info("  Warehouse: {}", warehousePath);
            loadViaSpark(dataDir, warehousePath, tables != null ? tables : TPCDS_TABLES);
        } else {
            log.info("  Mode: Frontend API");
            log.info("  Frontend: {}:{}", host, port);
            String token = readBootstrapToken();
            if (token != null) {
                log.info("  Auth: using bootstrap token");
            } else {
                log.warn("  Auth: no bootstrap token found, requests may be rejected");
            }
            TpcdsDataLoader loader = new TpcdsDataLoader(dataDir, host, port, token);
            loader.load();
        }
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
