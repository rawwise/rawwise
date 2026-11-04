package io.github.rawwise.benchmark.tpcds;

import io.github.rawwise.benchmark.CorrectnessChecker;
import io.github.rawwise.frontend.api.dto.ColumnInfoDto;
import io.github.rawwise.frontend.api.dto.QueryResponseDto;
import io.github.rawwise.frontend.api.dto.QueryResultDto;
import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TpcdsCorrectnessChecker implements CorrectnessChecker {

    private static final Logger LOG = Logger.getLogger(TpcdsCorrectnessChecker.class.getName());
    private static final double EPSILON = 1e-6;

    private final SparkSession spark;
    private final String dataDir;
    private volatile boolean tablesRegistered;

    public TpcdsCorrectnessChecker(SparkSession spark, String dataDir) {
        this.spark = spark;
        this.dataDir = dataDir;
    }

    private synchronized void ensureTablesRegistered() {
        if (tablesRegistered) {
            return;
        }
        File dir = new File(dataDir);
        if (!dir.isDirectory()) {
            throw new IllegalStateException("TPC-DS data directory not found: " + dataDir);
        }
        File[] tableDirs = dir.listFiles(File::isDirectory);
        if (tableDirs == null) {
            throw new IllegalStateException("No table directories found in: " + dataDir);
        }
        for (File tableDir : tableDirs) {
            String tableName = tableDir.getName();
            try {
                spark.read().parquet(tableDir.getAbsolutePath()).createOrReplaceTempView(tableName);
            } catch (Exception e) {
                LOG.fine("Failed to register table " + tableName + ": " + e.getMessage());
            }
        }
        tablesRegistered = true;
    }

    @Override
    public String check(String queryId, Object actual) {
        if (!(actual instanceof QueryResponseDto response)) {
            return "UNSUPPORTED_RESULT_TYPE: "
                    + (actual != null ? actual.getClass().getName() : "null");
        }
        if (!"SUCCEEDED".equals(response.getState())) {
            return "QUERY_FAILED: " + response.getState();
        }
        QueryResultDto data = response.getData();
        if (data == null) {
            return "NO_RESULT_DATA";
        }
        try {
            ensureTablesRegistered();
            return compareWithGroundTruth(queryId, data);
        } catch (Exception e) {
            LOG.warning("Correctness check for " + queryId + " failed: " + e.getMessage());
            return "CHECK_ERROR: " + e.getMessage();
        }
    }

    public String checkWithSql(String queryId, String sql, QueryResultDto actual) {
        try {
            ensureTablesRegistered();
            return compareWithGroundTruth(queryId, sql, actual);
        } catch (Exception e) {
            LOG.warning("Correctness check for " + queryId + " failed: " + e.getMessage());
            return "CHECK_ERROR: " + e.getMessage();
        }
    }

    private String compareWithGroundTruth(String queryId, QueryResultDto actual) {
        String sql = loadQuerySql(queryId);
        if (sql == null) {
            return "NO_QUERY_SQL";
        }
        return compareWithGroundTruth(queryId, sql, actual);
    }

    private String compareWithGroundTruth(String queryId, String sql, QueryResultDto actual) {
        Dataset<Row> df = spark.sql(sql);
        List<Row> groundTruthRows = df.collectAsList();

        String schemaCheck = compareSchemas(queryId, df, actual);
        if (schemaCheck != null) {
            return schemaCheck;
        }

        String rowCountCheck = compareRowCounts(queryId, groundTruthRows, actual);
        if (rowCountCheck != null) {
            return rowCountCheck;
        }

        return compareValues(queryId, sql, df, groundTruthRows, actual);
    }

    private String loadQuerySql(String queryId) {
        try {
            String resourcePath = "tpcds/queries/" + queryId + ".sql";
            java.io.InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath);
            if (is == null) {
                return null;
            }
            return new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            return null;
        }
    }

    private String compareSchemas(String queryId, Dataset<Row> df, QueryResultDto actual) {
        org.apache.spark.sql.types.StructType sparkSchema = df.schema();
        List<ColumnInfoDto> rawwiseColumns = actual.getColumns();

        if (rawwiseColumns == null || rawwiseColumns.isEmpty()) {
            return "SCHEMA_MISMATCH: no columns in RawWise result";
        }

        if (sparkSchema.size() != rawwiseColumns.size()) {
            return "SCHEMA_COLUMN_COUNT_MISMATCH: expected="
                    + sparkSchema.size()
                    + ", actual="
                    + rawwiseColumns.size();
        }

        for (int i = 0; i < sparkSchema.size(); i++) {
            String expectedName = sparkSchema.apply(i).name();
            String actualName = rawwiseColumns.get(i).getName();
            if (!expectedName.equalsIgnoreCase(actualName)) {
                return "SCHEMA_COLUMN_NAME_MISMATCH at col "
                        + i
                        + ": expected="
                        + expectedName
                        + ", actual="
                        + actualName;
            }
        }

        return null;
    }

    private String compareRowCounts(String queryId, List<Row> groundTruth, QueryResultDto actual) {
        int expected = groundTruth.size();
        int actualCount = actual.getRowCount() != null ? actual.getRowCount() : 0;
        if (expected != actualCount) {
            return "ROW_COUNT_MISMATCH: expected=" + expected + ", actual=" + actualCount;
        }
        return null;
    }

    private String compareValues(
            String queryId,
            String sql,
            Dataset<Row> df,
            List<Row> groundTruth,
            QueryResultDto actual) {
        List<List<Object>> actualRows = actual.getRows();
        if (actualRows == null || actualRows.isEmpty()) {
            if (groundTruth.isEmpty()) {
                return "MATCH";
            }
            return "VALUE_MISMATCH: expected " + groundTruth.size() + " rows, got null/empty rows";
        }
        if (groundTruth.isEmpty()) {
            return "VALUE_MISMATCH: expected 0 rows, got " + actualRows.size() + " rows";
        }

        int numCols = df.schema().size();

        List<Row> sortedGroundTruth = sortGroundTruth(groundTruth, numCols);
        List<List<Object>> sortedActual = sortActual(actualRows, numCols);

        for (int rowIdx = 0; rowIdx < sortedGroundTruth.size(); rowIdx++) {
            Row expectedRow = sortedGroundTruth.get(rowIdx);
            List<Object> actualRow = sortedActual.get(rowIdx);

            for (int colIdx = 0; colIdx < numCols; colIdx++) {
                Object expectedVal = expectedRow.isNullAt(colIdx) ? null : expectedRow.get(colIdx);
                Object actualVal = colIdx < actualRow.size() ? actualRow.get(colIdx) : null;

                if (!valuesEqual(expectedVal, actualVal)) {
                    String colName = df.schema().apply(colIdx).name();
                    return "VALUE_MISMATCH at row "
                            + rowIdx
                            + ", col "
                            + colIdx
                            + " ("
                            + colName
                            + "): expected="
                            + expectedVal
                            + ", actual="
                            + actualVal;
                }
            }
        }

        return "MATCH";
    }

    private List<Row> sortGroundTruth(List<Row> rows, int numCols) {
        List<Row> sorted = new ArrayList<>(rows);
        sorted.sort(createRowComparator(numCols));
        return sorted;
    }

    private List<List<Object>> sortActual(List<List<Object>> rows, int numCols) {
        List<List<Object>> sorted = new ArrayList<>(rows);
        sorted.sort(createListComparator(numCols));
        return sorted;
    }

    private Comparator<Row> createRowComparator(int numCols) {
        return (a, b) -> {
            for (int i = 0; i < numCols; i++) {
                int cmp =
                        compareValues(
                                a.isNullAt(i) ? null : a.get(i), b.isNullAt(i) ? null : b.get(i));
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        };
    }

    private Comparator<List<Object>> createListComparator(int numCols) {
        return (a, b) -> {
            for (int i = 0; i < numCols && i < a.size() && i < b.size(); i++) {
                int cmp = compareValues(a.get(i), b.get(i));
                if (cmp != 0) {
                    return cmp;
                }
            }
            return Integer.compare(a.size(), b.size());
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compareValues(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;

        if (a instanceof Comparable
                && b instanceof Comparable
                && a.getClass().isAssignableFrom(b.getClass())) {
            return ((Comparable) a).compareTo(b);
        }
        if (a instanceof Comparable
                && b instanceof Comparable
                && b.getClass().isAssignableFrom(a.getClass())) {
            return ((Comparable) b).compareTo(a) * -1;
        }

        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
        }

        return a.toString().compareTo(b.toString());
    }

    private static boolean valuesEqual(Object expected, Object actual) {
        if (expected == null && actual == null) return true;
        if (expected == null || actual == null) return false;

        if (expected instanceof Number && actual instanceof Number) {
            double diff =
                    Math.abs(((Number) expected).doubleValue() - ((Number) actual).doubleValue());
            return diff <= EPSILON;
        }

        if (expected instanceof String && actual instanceof String) {
            return expected.equals(actual);
        }

        if (expected instanceof Boolean && actual instanceof Boolean) {
            return expected.equals(actual);
        }

        return expected.toString().equals(actual.toString());
    }
}
