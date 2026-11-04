package io.github.rawwise.backend.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

@Slf4j
public final class ParquetMigrationTool {
    private ParquetMigrationTool() {}

    public static String migrateTable(SparkSession spark, String catalogTable, String parquetDir) {
        log.info("Migrating {} to {}", catalogTable, parquetDir);
        Dataset<Row> df = spark.sql("SELECT * FROM " + catalogTable);
        long count = df.count();
        df.write().mode(SaveMode.Overwrite).parquet(parquetDir);
        log.info("Migrated {} rows from {} to {}", count, catalogTable, parquetDir);
        return "Migrated " + count + " rows from " + catalogTable + " to " + parquetDir;
    }

    public static String migrateTableWithLimit(
            SparkSession spark, String catalogTable, String parquetDir, long limit) {
        log.info("Migrating {} (limit {}) to {}", catalogTable, limit, parquetDir);
        Dataset<Row> df = spark.sql("SELECT * FROM " + catalogTable + " LIMIT " + limit);
        long count = df.count();
        df.write().mode(SaveMode.Overwrite).parquet(parquetDir);
        log.info("Migrated {} rows from {} to {}", count, catalogTable, parquetDir);
        return "Migrated " + count + " rows from " + catalogTable + " to " + parquetDir;
    }
}
