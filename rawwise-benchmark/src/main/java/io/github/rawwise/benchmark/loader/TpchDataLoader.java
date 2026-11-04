package io.github.rawwise.benchmark.loader;

import io.github.rawwise.client.repl.FrontendHttpClient;
import io.github.rawwise.frontend.api.dto.QueryResponseDto;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

@Slf4j
public class TpchDataLoader {

    private static final Map<String, StructType> TABLE_SCHEMAS = new LinkedHashMap<>();

    static {
        TABLE_SCHEMAS.put(
                "nation",
                DataTypes.createStructType(
                        new StructField[] {
                            field("n_nationkey", DataTypes.IntegerType),
                            field("n_name", DataTypes.StringType),
                            field("n_regionkey", DataTypes.IntegerType),
                            field("n_comment", DataTypes.StringType)
                        }));

        TABLE_SCHEMAS.put(
                "region",
                DataTypes.createStructType(
                        new StructField[] {
                            field("r_regionkey", DataTypes.IntegerType),
                            field("r_name", DataTypes.StringType),
                            field("r_comment", DataTypes.StringType)
                        }));

        TABLE_SCHEMAS.put(
                "part",
                DataTypes.createStructType(
                        new StructField[] {
                            field("p_partkey", DataTypes.IntegerType),
                            field("p_name", DataTypes.StringType),
                            field("p_mfgr", DataTypes.StringType),
                            field("p_brand", DataTypes.StringType),
                            field("p_type", DataTypes.StringType),
                            field("p_size", DataTypes.IntegerType),
                            field("p_container", DataTypes.StringType),
                            field("p_retailprice", DataTypes.createDecimalType(15, 2)),
                            field("p_comment", DataTypes.StringType)
                        }));

        TABLE_SCHEMAS.put(
                "supplier",
                DataTypes.createStructType(
                        new StructField[] {
                            field("s_suppkey", DataTypes.IntegerType),
                            field("s_name", DataTypes.StringType),
                            field("s_address", DataTypes.StringType),
                            field("s_nationkey", DataTypes.IntegerType),
                            field("s_phone", DataTypes.StringType),
                            field("s_acctbal", DataTypes.createDecimalType(15, 2)),
                            field("s_comment", DataTypes.StringType)
                        }));

        TABLE_SCHEMAS.put(
                "partsupp",
                DataTypes.createStructType(
                        new StructField[] {
                            field("ps_partkey", DataTypes.IntegerType),
                            field("ps_suppkey", DataTypes.IntegerType),
                            field("ps_availqty", DataTypes.IntegerType),
                            field("ps_supplycost", DataTypes.createDecimalType(15, 2)),
                            field("ps_comment", DataTypes.StringType)
                        }));

        TABLE_SCHEMAS.put(
                "customer",
                DataTypes.createStructType(
                        new StructField[] {
                            field("c_custkey", DataTypes.IntegerType),
                            field("c_name", DataTypes.StringType),
                            field("c_address", DataTypes.StringType),
                            field("c_nationkey", DataTypes.IntegerType),
                            field("c_phone", DataTypes.StringType),
                            field("c_acctbal", DataTypes.createDecimalType(15, 2)),
                            field("c_mktsegment", DataTypes.StringType),
                            field("c_comment", DataTypes.StringType)
                        }));

        TABLE_SCHEMAS.put(
                "orders",
                DataTypes.createStructType(
                        new StructField[] {
                            field("o_orderkey", DataTypes.IntegerType),
                            field("o_custkey", DataTypes.IntegerType),
                            field("o_orderstatus", DataTypes.StringType),
                            field("o_totalprice", DataTypes.createDecimalType(15, 2)),
                            field("o_orderdate", DataTypes.DateType),
                            field("o_orderpriority", DataTypes.StringType),
                            field("o_clerk", DataTypes.StringType),
                            field("o_shippriority", DataTypes.IntegerType),
                            field("o_comment", DataTypes.StringType)
                        }));

        TABLE_SCHEMAS.put(
                "lineitem",
                DataTypes.createStructType(
                        new StructField[] {
                            field("l_orderkey", DataTypes.IntegerType),
                            field("l_partkey", DataTypes.IntegerType),
                            field("l_suppkey", DataTypes.IntegerType),
                            field("l_linenumber", DataTypes.IntegerType),
                            field("l_quantity", DataTypes.createDecimalType(15, 2)),
                            field("l_extendedprice", DataTypes.createDecimalType(15, 2)),
                            field("l_discount", DataTypes.createDecimalType(15, 2)),
                            field("l_tax", DataTypes.createDecimalType(15, 2)),
                            field("l_returnflag", DataTypes.StringType),
                            field("l_linestatus", DataTypes.StringType),
                            field("l_shipdate", DataTypes.DateType),
                            field("l_commitdate", DataTypes.DateType),
                            field("l_receiptdate", DataTypes.DateType),
                            field("l_shipinstruct", DataTypes.StringType),
                            field("l_shipmode", DataTypes.StringType),
                            field("l_comment", DataTypes.StringType)
                        }));
    }

    private static StructField field(String name, org.apache.spark.sql.types.DataType type) {
        return new StructField(name, type, true, Metadata.empty());
    }

    private static final int LOAD_TIMEOUT_SECONDS = 600;

    private static final Map<String, String> TABLE_DDL = new LinkedHashMap<>();

    static {
        TABLE_DDL.put(
                "nation", "n_nationkey INT, n_name STRING, n_regionkey INT, n_comment STRING");
        TABLE_DDL.put("region", "r_regionkey INT, r_name STRING, r_comment STRING");
        TABLE_DDL.put(
                "part",
                "p_partkey INT, p_name STRING, p_mfgr STRING, p_brand STRING, "
                        + "p_type STRING, p_size INT, p_container STRING, p_retailprice DECIMAL(15,2), p_comment STRING");
        TABLE_DDL.put(
                "supplier",
                "s_suppkey INT, s_name STRING, s_address STRING, s_nationkey INT, "
                        + "s_phone STRING, s_acctbal DECIMAL(15,2), s_comment STRING");
        TABLE_DDL.put(
                "partsupp",
                "ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost DECIMAL(15,2), ps_comment STRING");
        TABLE_DDL.put(
                "customer",
                "c_custkey INT, c_name STRING, c_address STRING, c_nationkey INT, "
                        + "c_phone STRING, c_acctbal DECIMAL(15,2), c_mktsegment STRING, c_comment STRING");
        TABLE_DDL.put(
                "orders",
                "o_orderkey INT, o_custkey INT, o_orderstatus STRING, o_totalprice DECIMAL(15,2), "
                        + "o_orderdate DATE, o_orderpriority STRING, o_clerk STRING, o_shippriority INT, o_comment STRING");
        TABLE_DDL.put(
                "lineitem",
                "l_orderkey INT, l_partkey INT, l_suppkey INT, l_linenumber INT, "
                        + "l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2), "
                        + "l_tax DECIMAL(15,2), l_returnflag STRING, l_linestatus STRING, "
                        + "l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, "
                        + "l_shipinstruct STRING, l_shipmode STRING, l_comment STRING");
    }

    public void convertTblToParquet(String tblDir, String outputDir) {
        SparkSession spark =
                SparkSession.builder()
                        .appName("TpchTblToParquet")
                        .master("local[*]")
                        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-tmp")
                        .getOrCreate();

        try {
            for (Map.Entry<String, StructType> entry : TABLE_SCHEMAS.entrySet()) {
                String table = entry.getKey();
                StructType schema = entry.getValue();

                File tblFile = Path.of(tblDir, table + ".tbl").toFile();
                File outputTableDir = Path.of(outputDir, table).toFile();

                if (!tblFile.exists()) {
                    log.info("  SKIP {} (no .tbl file)", table);
                    continue;
                }

                if (outputTableDir.exists()
                        && outputTableDir.listFiles(f -> f.getName().endsWith(".parquet"))
                                != null) {
                    log.info("  SKIP {} (parquet already exists)", table);
                    continue;
                }

                log.info("  Converting {} ...", table);
                spark.read()
                        .option("delimiter", "|")
                        .option("header", "false")
                        .schema(schema)
                        .csv(tblFile.getAbsolutePath())
                        .write()
                        .mode("overwrite")
                        .parquet(outputTableDir.getAbsolutePath());

                log.info("  OK   {}", table);
            }
        } finally {
            spark.stop();
        }
    }

    public void registerTables(String dataDir, String host, int port) {
        this.registerTables(dataDir, host, port, null);
    }

    public void registerTables(String dataDir, String host, int port, String authToken) {
        FrontendHttpClient client = new FrontendHttpClient(host, port);

        if (!client.healthCheck()) {
            throw new IllegalStateException("RawWise Frontend is not available");
        }

        log.info("Creating database tpch ...");
        QueryResponseDto dbResult =
                client.executeSync("CREATE DATABASE IF NOT EXISTS tpch", "default", 60);
        if (isFailed(dbResult)) {
            log.warn("Failed to create database tpch: {}", errorOf(dbResult));
        }

        List<String> loaded = new ArrayList<>();
        List<String> skipped = new ArrayList<>();
        List<String> failed = new ArrayList<>();

        for (String table : TABLE_SCHEMAS.keySet()) {
            File tableDir = Path.of(dataDir, table).toFile();
            if (!tableDir.isDirectory()) {
                skipped.add(table);
                log.info("  SKIP {} (no parquet data)", table);
                continue;
            }

            log.info("  Loading {} ...", table);

            String ddl = TABLE_DDL.get(table);
            if (ddl == null) {
                log.warn("  SKIP {} (no DDL definition)", table);
                skipped.add(table);
                continue;
            }

            String createSql =
                    String.format(
                            "CREATE TABLE IF NOT EXISTS rawwise.tpch.%s (%s) USING rawwise",
                            table, ddl);
            QueryResponseDto createResult =
                    client.executeSync(createSql, "tpch", LOAD_TIMEOUT_SECONDS);
            if (isFailed(createResult)) {
                failed.add(table);
                log.warn("  FAIL {} (create): {}", table, errorOf(createResult));
                continue;
            }

            String insertSql =
                    String.format(
                            "INSERT INTO rawwise.tpch.%s SELECT * FROM parquet.`%s`",
                            table, tableDir.getAbsolutePath());
            QueryResponseDto insertResult =
                    client.executeSync(insertSql, "tpch", LOAD_TIMEOUT_SECONDS);
            if (isSucceeded(insertResult)) {
                loaded.add(table);
                log.info("  OK   {}", table);
            } else {
                failed.add(table);
                log.warn("  FAIL {} (insert): {}", table, errorOf(insertResult));
            }
        }

        log.info("=== TPC-H Data Loading Summary ===");
        log.info("Loaded:  {} tables {}", loaded.size(), loaded);
        log.info("Skipped: {} tables {}", skipped.size(), skipped);
        log.info("Failed:  {} tables {}", failed.size(), failed);

        if (!failed.isEmpty()) {
            throw new RuntimeException("Failed to load tables: " + failed);
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
        String tblDir = null;
        String outputDir = "./benchmark/data/tpch";
        String dataDir = "./benchmark/data/tpch";
        String host = "127.0.0.1";
        int port = 8080;
        boolean registerOnly = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--tbl-dir" -> tblDir = args[++i];
                case "--output-dir" -> outputDir = args[++i];
                case "--data-dir" -> dataDir = args[++i];
                case "--host" -> host = args[++i];
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--register-only" -> registerOnly = true;
                case "--help" -> {
                    System.out.println("Usage: TpchDataLoader [options]");
                    System.out.println("  --tbl-dir       Path to dbgen .tbl files");
                    System.out.println(
                            "  --output-dir    Output path for Parquet (default: ./benchmark/data/tpch)");
                    System.out.println(
                            "  --data-dir      Path to TPC-H Parquet data for registration (default: ./benchmark/data/tpch)");
                    System.out.println(
                            "  --host          RawWise Frontend host (default: 127.0.0.1)");
                    System.out.println("  --port          RawWise Frontend port (default: 8080)");
                    System.out.println(
                            "  --register-only Skip .tbl conversion, only register tables");
                    return;
                }
            }
        }

        TpchDataLoader loader = new TpchDataLoader();

        if (!registerOnly && tblDir != null) {
            log.info("Converting .tbl to Parquet ...");
            loader.convertTblToParquet(tblDir, outputDir);
        }

        log.info("Registering TPC-H tables in RawWise ...");
        String token = readBootstrapToken();
        if (token != null) {
            log.info("  Auth: using bootstrap token");
        } else {
            log.warn("  Auth: no bootstrap token found, requests may be rejected");
        }
        loader.registerTables(dataDir, host, port, token);
    }

    private static String readBootstrapToken() {
        for (String dir :
                java.util.List.of(
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
