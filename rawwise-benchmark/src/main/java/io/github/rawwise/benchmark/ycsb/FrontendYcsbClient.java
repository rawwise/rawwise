package io.github.rawwise.benchmark.ycsb;

import io.github.rawwise.client.repl.FrontendHttpClient;
import io.github.rawwise.frontend.api.dto.QueryResponseDto;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * YCSB client that goes through the Frontend HTTP API using SQL. Full path: Benchmark → HTTP →
 * Frontend → Spark → Backend → DynamicMmapStore → LMDB.
 */
public class FrontendYcsbClient implements YcsbClient {

    private static final Logger LOG = Logger.getLogger(FrontendYcsbClient.class.getName());
    private static final int SCAN_LENGTH = 10;
    private static final int TIMEOUT_SECONDS = 30;

    private final FrontendHttpClient httpClient;
    private final String database;
    private final String table;

    public FrontendYcsbClient(
            String host, int port, String database, String table, String authToken) {
        this.httpClient = new FrontendHttpClient(host, port);
        this.database = database;
        this.table = table;
    }

    @Override
    public void init(int recordCount) throws IOException {
        createTableIfNeeded();
        for (int i = 0; i < recordCount; i++) {
            insertRow(i);
        }
        LOG.info(
                "FrontendYcsbClient init: "
                        + recordCount
                        + " records written to "
                        + database
                        + "."
                        + table);
    }

    @Override
    public void initRange(int keyStart, int keyEnd) throws IOException {
        createTableIfNeeded();
        for (int i = keyStart; i < keyEnd; i++) {
            insertRow(i);
        }
        LOG.info(
                "FrontendYcsbClient initRange: ["
                        + keyStart
                        + ", "
                        + keyEnd
                        + ") to "
                        + database
                        + "."
                        + table);
    }

    private void createTableIfNeeded() throws IOException {
        String sql =
                "CREATE TABLE IF NOT EXISTS "
                        + database
                        + "."
                        + table
                        + " (id INT, data STRING) USING rawwise TBLPROPERTIES ('primary-key'='id')";
        QueryResponseDto resp = httpClient.executeSync(sql, database, TIMEOUT_SECONDS);
        if (!"SUCCEEDED".equals(resp.getState())) {
            throw new IOException("Create table failed: " + getErrorMessage(resp));
        }
    }

    private void insertRow(int i) throws IOException {
        String value = generateValue(i);
        String sql =
                "INSERT INTO "
                        + database
                        + "."
                        + table
                        + " (id, data) VALUES ("
                        + i
                        + ", '"
                        + escapeSql(value)
                        + "')";
        QueryResponseDto resp = httpClient.executeSync(sql, database, TIMEOUT_SECONDS);
        if (!"SUCCEEDED".equals(resp.getState())) {
            String errMsg = getErrorMessage(resp);
            if (errMsg != null && errMsg.contains("429")) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
                resp = httpClient.executeSync(sql, database, TIMEOUT_SECONDS);
            }
            if (!"SUCCEEDED".equals(resp.getState())) {
                throw new IOException(
                        "Init insert failed for key=" + i + ": " + getErrorMessage(resp));
            }
        }
    }

    private QueryResponseDto executeWithRetry(String sql) throws IOException {
        QueryResponseDto resp = httpClient.executeSync(sql, database, TIMEOUT_SECONDS);
        if (!"SUCCEEDED".equals(resp.getState())) {
            String errMsg = getErrorMessage(resp);
            if (errMsg != null && errMsg.contains("429")) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ignored) {
                }
                resp = httpClient.executeSync(sql, database, TIMEOUT_SECONDS);
            }
        }
        return resp;
    }

    @Override
    public String read(int key) throws IOException {
        String sql = "SELECT data FROM " + database + "." + table + " WHERE id = " + key;
        QueryResponseDto resp = executeWithRetry(sql);
        if (!"SUCCEEDED".equals(resp.getState())) {
            throw new IOException("Read failed for key=" + key + ": " + getErrorMessage(resp));
        }
        if (resp.getData() == null
                || resp.getData().getRows() == null
                || resp.getData().getRows().isEmpty()) {
            return null;
        }
        Object val = resp.getData().getRows().get(0).get(0);
        return val != null ? val.toString() : null;
    }

    @Override
    public void write(int key) throws IOException {
        String value = generateValue(key);
        String sql =
                "UPDATE "
                        + database
                        + "."
                        + table
                        + " SET data = '"
                        + escapeSql(value)
                        + "' WHERE id = "
                        + key;
        QueryResponseDto resp = executeWithRetry(sql);
        if (!"SUCCEEDED".equals(resp.getState())) {
            throw new IOException("Write failed for key=" + key + ": " + getErrorMessage(resp));
        }
    }

    @Override
    public void insert(int key) throws IOException {
        String value = generateValue(key);
        String sql =
                "INSERT INTO "
                        + database
                        + "."
                        + table
                        + " (id, data) VALUES ("
                        + key
                        + ", '"
                        + escapeSql(value)
                        + "')";
        QueryResponseDto resp = executeWithRetry(sql);
        if (!"SUCCEEDED".equals(resp.getState())) {
            throw new IOException("Insert failed for key=" + key + ": " + getErrorMessage(resp));
        }
    }

    @Override
    public void delete(int key) throws IOException {
        String sql = "DELETE FROM " + database + "." + table + " WHERE id = " + key;
        QueryResponseDto resp = executeWithRetry(sql);
        if (!"SUCCEEDED".equals(resp.getState())) {
            throw new IOException("Delete failed for key=" + key + ": " + getErrorMessage(resp));
        }
    }

    @Override
    public int scan(int startKey) throws IOException {
        int count = 0;
        for (int i = 0; i < SCAN_LENGTH; i++) {
            String val = read(startKey + i);
            if (val != null) {
                count++;
            }
        }
        return count;
    }

    @Override
    public void close() {
        httpClient.close();
    }

    private static String generateValue(int i) {
        return "field0-"
                + i
                + ",field1-"
                + (i * 2)
                + ",field2-"
                + (i * 3)
                + ",field3-"
                + (i * 4)
                + ",field4-"
                + (i * 5);
    }

    private static String escapeSql(String value) {
        return value.replace("'", "''");
    }

    private static String getErrorMessage(QueryResponseDto resp) {
        if (resp.getError() != null && resp.getError().getMessage() != null) {
            return resp.getError().getMessage();
        }
        return resp.getState();
    }
}
