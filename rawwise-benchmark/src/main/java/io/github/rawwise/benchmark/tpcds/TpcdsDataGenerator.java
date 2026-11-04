package io.github.rawwise.benchmark.tpcds;

import io.github.rawwise.benchmark.DataGenerator;
import java.nio.file.Path;

public class TpcdsDataGenerator implements DataGenerator {

    @Override
    public void generate(int scaleFactor, Path outputDir, Object spark) {
        try {
            Class<?> sparkSessionClass = spark.getClass();
            var sqlMethod = sparkSessionClass.getMethod("sql", String.class);

            long storeSalesRows = scaleFactor * 28800L;
            long dateDimRows = 2556L;
            long itemRows = scaleFactor * 2000L;
            long storeRows = scaleFactor * 12L;
            long customerRows = scaleFactor * 10000L;

            sqlMethod.invoke(
                    spark,
                    "CREATE TABLE IF NOT EXISTS store_sales USING parquet LOCATION '"
                            + outputDir.resolve("store_sales").toUri()
                            + "' AS "
                            + "SELECT id AS ss_item_sk, "
                            + "CAST(id % "
                            + dateDimRows
                            + " AS BIGINT) AS ss_sold_date_sk, "
                            + "CAST(id % "
                            + storeRows
                            + " AS BIGINT) AS ss_store_sk, "
                            + "CAST(id % "
                            + customerRows
                            + " AS BIGINT) AS ss_customer_sk, "
                            + "id * 10.0 AS ss_quantity, "
                            + "id * 100.0 AS ss_sales_price, "
                            + "id * 50.0 AS ss_net_profit, "
                            + "CAST(CASE WHEN id % 2 = 0 THEN 'Y' ELSE 'N' END AS STRING) AS ss_wholesale_cost_flag "
                            + "FROM (SELECT id FROM RANGE("
                            + storeSalesRows
                            + "))");

            sqlMethod.invoke(
                    spark,
                    "CREATE TABLE IF NOT EXISTS date_dim USING parquet LOCATION '"
                            + outputDir.resolve("date_dim").toUri()
                            + "' AS "
                            + "SELECT id AS d_date_sk, "
                            + "DATE_ADD(DATE '1998-01-01', CAST(id AS INT)) AS d_date, "
                            + "CAST(id % 7 + 1 AS INT) AS d_day_of_week, "
                            + "CAST((id / 30) % 12 + 1 AS INT) AS d_month_seq, "
                            + "CAST(id % 4 + 1998 AS INT) AS d_year "
                            + "FROM (SELECT id FROM RANGE("
                            + dateDimRows
                            + "))");

            sqlMethod.invoke(
                    spark,
                    "CREATE TABLE IF NOT EXISTS item USING parquet LOCATION '"
                            + outputDir.resolve("item").toUri()
                            + "' AS "
                            + "SELECT id AS i_item_sk, "
                            + "CONCAT('item_', CAST(id AS STRING)) AS i_item_id, "
                            + "CAST(id % 1000 + 1.0 AS DOUBLE) AS i_current_price "
                            + "FROM (SELECT id FROM RANGE("
                            + itemRows
                            + "))");

            sqlMethod.invoke(
                    spark,
                    "CREATE TABLE IF NOT EXISTS store USING parquet LOCATION '"
                            + outputDir.resolve("store").toUri()
                            + "' AS "
                            + "SELECT id AS s_store_sk, "
                            + "CONCAT('store_', CAST(id AS STRING)) AS s_store_id, "
                            + "CAST(CASE WHEN id % 4 = 0 THEN 'A' WHEN id % 4 = 1 THEN 'B' WHEN id % 4 = 2 THEN 'C' ELSE 'D' END AS STRING) AS s_state "
                            + "FROM (SELECT id FROM RANGE("
                            + storeRows
                            + "))");

            sqlMethod.invoke(
                    spark,
                    "CREATE TABLE IF NOT EXISTS customer USING parquet LOCATION '"
                            + outputDir.resolve("customer").toUri()
                            + "' AS "
                            + "SELECT id AS c_customer_sk, "
                            + "CONCAT('customer_', CAST(id AS STRING)) AS c_customer_id, "
                            + "CAST(CASE WHEN id % 2 = 0 THEN 'M' ELSE 'F' END AS STRING) AS c_gender, "
                            + "CAST(id % 100 + 1 AS INT) AS c_birth_month "
                            + "FROM (SELECT id FROM RANGE("
                            + customerRows
                            + "))");

        } catch (Exception e) {
            throw new RuntimeException("Failed to generate TPC-DS data", e);
        }
    }
}
