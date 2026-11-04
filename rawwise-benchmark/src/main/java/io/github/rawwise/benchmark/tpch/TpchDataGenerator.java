package io.github.rawwise.benchmark.tpch;

import io.github.rawwise.benchmark.DataGenerator;
import java.nio.file.Path;

public class TpchDataGenerator implements DataGenerator {

    @Override
    public void generate(int scaleFactor, Path outputDir, Object spark) {
        try {
            Class<?> sparkSessionClass = spark.getClass();
            var sqlMethod = sparkSessionClass.getMethod("sql", String.class);

            long lineitemRows = scaleFactor * 10000L;
            long ordersRows = scaleFactor * 1500L;

            sqlMethod.invoke(
                    spark,
                    "CREATE TABLE IF NOT EXISTS lineitem USING parquet LOCATION '"
                            + outputDir.resolve("lineitem").toUri()
                            + "' AS "
                            + "SELECT id AS l_orderkey, "
                            + "CAST(id % 7 AS INT) AS l_linenumber, "
                            + "id * 10.0 AS l_quantity, "
                            + "id * 100.0 AS l_extendedprice, "
                            + "CAST(CASE WHEN id % 2 = 0 THEN 'A' ELSE 'R' END AS STRING) AS l_returnflag, "
                            + "CAST(CASE WHEN id % 3 = 0 THEN 'F' WHEN id % 3 = 1 THEN 'O' ELSE 'P' END AS STRING) AS l_linestatus, "
                            + "DATE_ADD(DATE '1998-01-01', CAST(id % 365 AS INT)) AS l_shipdate "
                            + "FROM (SELECT id FROM RANGE("
                            + lineitemRows
                            + "))");

            sqlMethod.invoke(
                    spark,
                    "CREATE TABLE IF NOT EXISTS orders USING parquet LOCATION '"
                            + outputDir.resolve("orders").toUri()
                            + "' AS "
                            + "SELECT id AS o_orderkey, "
                            + "CAST(CASE WHEN id % 2 = 0 THEN 'F' ELSE 'O' END AS STRING) AS o_orderstatus "
                            + "FROM (SELECT id FROM RANGE("
                            + ordersRows
                            + "))");
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate TPC-H data", e);
        }
    }
}
