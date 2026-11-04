package io.github.rawwise.benchmark.chbenchmark;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class ChOlapQueries {

    private static final Logger LOG = Logger.getLogger(ChOlapQueries.class.getName());
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private final String frontendHost;
    private final int frontendPort;

    public ChOlapQueries(String host, int port) {
        this.frontendHost = host;
        this.frontendPort = port;
    }

    public List<String> getAllQueries(long wId) {
        List<String> queries = new ArrayList<>();
        queries.add("SELECT COUNT(*) AS order_count FROM db.orders WHERE o_w_id = " + wId);
        queries.add("SELECT AVG(c_balance) AS avg_balance FROM db.customer WHERE c_w_id = " + wId);
        queries.add(
                "SELECT COUNT(*) AS delivery_count FROM db.orders WHERE o_w_id = "
                        + wId
                        + " AND o_carrier_id != 0");
        queries.add(
                "SELECT c_d_id, COUNT(*) AS cust_per_district, AVG(c_balance) AS avg_bal FROM db.customer WHERE c_w_id = "
                        + wId
                        + " GROUP BY c_d_id ORDER BY c_d_id");
        queries.add(
                "SELECT o_d_id, COUNT(*) AS orders_per_district FROM db.orders WHERE o_w_id = "
                        + wId
                        + " GROUP BY o_d_id ORDER BY o_d_id");
        queries.add(
                "SELECT s_i_id, s_quantity FROM db.stock WHERE s_w_id = "
                        + wId
                        + " AND s_quantity < 20 ORDER BY s_quantity ASC");
        queries.add(
                "SELECT c_id, c_balance FROM db.customer WHERE c_w_id = "
                        + wId
                        + " ORDER BY c_balance ASC LIMIT 10");
        queries.add(
                "SELECT c_d_id, SUM(c_ytd_payment) AS total_ytd FROM db.customer WHERE c_w_id = "
                        + wId
                        + " GROUP BY c_d_id ORDER BY total_ytd DESC");
        queries.add(
                "SELECT o_carrier_id, COUNT(*) AS cnt FROM db.orders WHERE o_w_id = "
                        + wId
                        + " GROUP BY o_carrier_id ORDER BY cnt DESC");
        queries.add(
                "SELECT COUNT(*) AS low_stock FROM db.stock WHERE s_w_id = "
                        + wId
                        + " AND s_quantity < 10");
        queries.add(
                "SELECT d_id, d_ytd FROM db.district WHERE d_w_id = "
                        + wId
                        + " ORDER BY d_ytd DESC");
        queries.add(
                "SELECT AVG(ol_amount) AS avg_amount FROM db.order_line WHERE ol_w_id = " + wId);
        queries.add(
                "SELECT ol_i_id, SUM(ol_quantity) AS total_qty FROM db.order_line WHERE ol_w_id = "
                        + wId
                        + " GROUP BY ol_i_id ORDER BY total_qty DESC LIMIT 10");
        queries.add(
                "SELECT COUNT(DISTINCT o_c_id) AS unique_customers FROM db.orders WHERE o_w_id = "
                        + wId);
        queries.add(
                "SELECT c_id, c_ytd_payment FROM db.customer WHERE c_w_id = "
                        + wId
                        + " ORDER BY c_ytd_payment DESC LIMIT 10");
        queries.add(
                "SELECT d_id, COUNT(*) AS district_orders FROM db.orders WHERE o_w_id = "
                        + wId
                        + " AND o_d_id IN (1,2,3) GROUP BY d_id");
        queries.add("SELECT SUM(s_quantity) AS total_stock FROM db.stock WHERE s_w_id = " + wId);
        queries.add(
                "SELECT o_d_id, MIN(o_id) AS min_order, MAX(o_id) AS max_order FROM db.orders WHERE o_w_id = "
                        + wId
                        + " GROUP BY o_d_id");
        queries.add(
                "SELECT c_d_id, COUNT(*) AS cnt FROM db.customer WHERE c_w_id = "
                        + wId
                        + " AND c_balance > 5000 GROUP BY c_d_id");
        queries.add(
                "SELECT ol_number, AVG(ol_amount) AS avg_amt FROM db.order_line WHERE ol_w_id = "
                        + wId
                        + " GROUP BY ol_number ORDER BY ol_number");
        queries.add(
                "SELECT COUNT(*) AS new_orders FROM db.orders WHERE o_w_id = "
                        + wId
                        + " AND o_carrier_id = 0");
        queries.add("SELECT w_id, w_ytd FROM db.warehouse WHERE w_id = " + wId);
        return queries;
    }

    public QueryResult executeQuery(HttpClient client, String sql)
            throws IOException, InterruptedException {
        long start = System.nanoTime();
        String escaped = sql.replace("\\", "\\\\").replace("\"", "\\\"");
        String body = "{\"sql\":\"" + escaped + "\"}";
        HttpRequest req =
                HttpRequest.newBuilder()
                        .uri(
                                URI.create(
                                        "http://"
                                                + frontendHost
                                                + ":"
                                                + frontendPort
                                                + "/api/query"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .timeout(REQUEST_TIMEOUT)
                        .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        boolean success = resp.statusCode() < 400;
        if (!success) {
            String snippet = resp.body().substring(0, Math.min(200, resp.body().length()));
            LOG.warning("OLAP query failed: " + resp.statusCode() + " body=" + snippet);
        }
        return new QueryResult(success, elapsedMs, sql);
    }

    public record QueryResult(boolean success, long latencyMs, String sql) {}
}
