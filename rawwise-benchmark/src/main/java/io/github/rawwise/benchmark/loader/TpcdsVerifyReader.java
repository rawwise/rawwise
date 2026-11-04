package io.github.rawwise.benchmark.loader;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class TpcdsVerifyReader {

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
                    "web_sales");

    public static void main(String[] args) {
        String warehousePath = args.length > 0 ? args[0] : "/tmp/rawwise/warehouse";

        SparkSession spark =
                SparkSession.builder()
                        .appName("TpcdsVerifyReader")
                        .master("local[4]")
                        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-tpcds-verify")
                        .config(
                                "spark.sql.catalog.rawwise",
                                "io.github.rawwise.backend.spark.RawWiseSparkCatalog")
                        .config("spark.sql.catalog.rawwise.warehouse-path", warehousePath)
                        .config("spark.sql.catalog.rawwise.catalog-implementation", "metastore")
                        .config("spark.sql.catalog.rawwise.store-kind", "rocksdb")
                        .config("spark.sql.adaptive.enabled", "true")
                        .config(
                                "spark.driver.extraJavaOptions",
                                "--add-opens java.base/java.nio=ALL-UNNAMED")
                        .config(
                                "spark.executor.extraJavaOptions",
                                "--add-opens java.base/java.nio=ALL-UNNAMED")
                        .getOrCreate();

        try {
            List<String> ok = new ArrayList<>();
            List<String> fail = new ArrayList<>();

            for (String table : TPCDS_TABLES) {
                try {
                    Dataset<Row> df =
                            spark.sql("SELECT COUNT(*) AS cnt FROM rawwise.tpcds." + table);
                    long count = df.collectAsList().get(0).getLong(0);
                    ok.add(table);
                    log.info("  OK   {} ({} rows)", table, count);
                } catch (Exception e) {
                    fail.add(table);
                    log.warn("  FAIL {} : {}", table, e.getMessage());
                }
            }

            log.info("=== TPC-DS Verify Summary ===");
            log.info("Readable: {} tables {}", ok.size(), ok);
            log.info("Failed:   {} tables {}", fail.size(), fail);
        } finally {
            spark.stop();
        }
    }
}
