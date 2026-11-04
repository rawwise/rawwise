package io.github.rawwise.client.repl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.frontend.api.dto.QueryRequestDto;
import io.github.rawwise.frontend.api.dto.QueryResponseDto;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FrontendHttpClient implements AutoCloseable {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int CONNECT_TIMEOUT_SECONDS = 10;
    private static final int HTTP_MAX_RETRIES = 3;
    // 轮询策略：先线性短间隔（2ms × 25 次 = 50ms）快速捕获 Spark SQL 快速完成的作业，
    // 再指数退避到最大 500ms。
    // 旧策略 1→2→4→8→16→32=63ms 导致 7-63ms 完成的作业被延迟到 63ms 才检测到（p99=63ms 尾部）。
    // 新策略：作业 35ms 完成时，线性 2ms 间隔在 36ms 检测到（1ms overhead），旧策略要等到 63ms。
    private static final long POLL_LINEAR_INTERVAL_MS = 2;
    private static final int POLL_LINEAR_COUNT = 25;
    private static final long POLL_EXPONENTIAL_INITIAL_MS = 4;
    private static final long POLL_MAX_MS = 500;

    public static final String STATE_PENDING = "PENDING";
    public static final String STATE_SUBMITTED = "SUBMITTED";
    public static final String STATE_RUNNING = "RUNNING";
    public static final String STATE_SUCCEEDED = "SUCCEEDED";
    public static final String STATE_FAILED = "FAILED";
    public static final String STATE_CANCELLED = "CANCELLED";
    public static final String STATE_TIMED_OUT = "TIMED_OUT";

    private final String baseUrl;
    private final HttpClient httpClient;

    public FrontendHttpClient(String host, int port) {
        this.baseUrl = "http://" + host + ":" + port;
        // HTTP/1.1 显式：Javalin/Jetty 默认 HTTP/1.1，避免 HTTP/2 协商升级开销（1 RTT）。
        // 虚拟线程 executor：多线程 YCSB client 并发 HTTP 请求不被 single-thread executor 阻塞。
        this.httpClient =
                HttpClient.newBuilder()
                        .version(HttpClient.Version.HTTP_1_1)
                        .connectTimeout(Duration.ofSeconds(CONNECT_TIMEOUT_SECONDS))
                        .executor(java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor())
                        .build();
    }

    private HttpResponse<String> sendWithRetry(HttpRequest request) throws Exception {
        IOException lastException = null;
        for (int i = 0; i < HTTP_MAX_RETRIES; i++) {
            try {
                return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (IOException e) {
                lastException = e;
                String msg = e.getMessage() != null ? e.getMessage() : "";
                if (msg.contains("EOF reached")
                        || msg.contains("header parser received no bytes")
                        || msg.contains("connection reset")
                        || msg.contains("closed")) {
                    log.debug(
                            "HTTP retry {}/{} after stale connection: {}",
                            i + 1,
                            HTTP_MAX_RETRIES,
                            msg);
                    continue;
                }
                throw e;
            }
        }
        throw lastException;
    }

    public boolean healthCheck() {
        try {
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(buildUrl("/health")))
                            .GET()
                            .timeout(Duration.ofSeconds(5))
                            .build();
            HttpResponse<String> response = sendWithRetry(request);
            return response.statusCode() == 200;
        } catch (Exception e) {
            log.warn("Health check failed", e);
            return false;
        }
    }

    public boolean readyCheck() {
        try {
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(buildUrl("/ready")))
                            .GET()
                            .timeout(Duration.ofSeconds(5))
                            .build();
            HttpResponse<String> response = sendWithRetry(request);
            return response.statusCode() == 200;
        } catch (Exception e) {
            log.warn("Ready check failed", e);
            return false;
        }
    }

    public QueryResponseDto fetchResult(String jobId) {
        try {
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(buildUrl("/api/query/" + jobId + "/result")))
                            .GET()
                            .timeout(Duration.ofSeconds(30))
                            .build();

            HttpResponse<String> response = sendWithRetry(request);

            if (response.statusCode() == 200) {
                return MAPPER.readValue(response.body(), QueryResponseDto.class);
            }
            // Jobs without results (e.g., DDL) return 404 — treat as success
            if (response.statusCode() == 404 && response.body().contains("QUERY_NOT_FOUND")) {
                return QueryResponseDto.builder().state(STATE_SUCCEEDED).build();
            }
            return QueryResponseDto.builder()
                    .state(STATE_FAILED)
                    .error(
                            io.github.rawwise.frontend.api.dto.ErrorResponseDto.builder()
                                    .code("HTTP_ERROR")
                                    .message("HTTP " + response.statusCode())
                                    .build())
                    .build();
        } catch (Exception e) {
            log.error("Failed to fetch result", e);
            return QueryResponseDto.builder()
                    .state(STATE_FAILED)
                    .error(
                            io.github.rawwise.frontend.api.dto.ErrorResponseDto.builder()
                                    .code("CONNECTION_ERROR")
                                    .message(e.getMessage())
                                    .build())
                    .build();
        }
    }

    public String fetchJobStatus(String jobId) {
        try {
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(buildUrl("/api/query/" + jobId + "/status")))
                            .GET()
                            .timeout(Duration.ofSeconds(10))
                            .build();
            HttpResponse<String> response = sendWithRetry(request);
            if (response.statusCode() == 200) {
                QueryResponseDto dto = MAPPER.readValue(response.body(), QueryResponseDto.class);
                return dto.getState() != null ? dto.getState() : STATE_PENDING;
            }
            return STATE_FAILED;
        } catch (Exception e) {
            log.warn("Failed to fetch job status for {}", jobId, e);
            return STATE_FAILED;
        }
    }

    public QueryResponseDto executeSync(String sql, String database, int timeoutSeconds) {
        try {
            QueryRequestDto requestDto = new QueryRequestDto();
            requestDto.setSql(sql);
            requestDto.setType("SQL");
            requestDto.setOptions(Map.of("database", database != null ? database : "default"));

            String body = MAPPER.writeValueAsString(requestDto);

            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(buildUrl("/api/query")))
                            .POST(HttpRequest.BodyPublishers.ofString(body))
                            .header("Content-Type", "application/json")
                            .timeout(Duration.ofMinutes(Math.max(10, timeoutSeconds / 60 + 1)))
                            .build();

            HttpResponse<String> response = sendWithRetry(request);

            if (response.statusCode() != 200) {
                return QueryResponseDto.builder()
                        .state(STATE_FAILED)
                        .error(
                                io.github.rawwise.frontend.api.dto.ErrorResponseDto.builder()
                                        .code("HTTP_ERROR")
                                        .message(
                                                "HTTP "
                                                        + response.statusCode()
                                                        + ": "
                                                        + response.body())
                                        .build())
                        .build();
            }

            QueryResponseDto currentResponse =
                    MAPPER.readValue(response.body(), QueryResponseDto.class);
            String state =
                    currentResponse.getState() != null ? currentResponse.getState() : STATE_PENDING;
            String jobId = currentResponse.getJobId();

            if (STATE_SUCCEEDED.equals(state)
                    || STATE_FAILED.equals(state)
                    || STATE_CANCELLED.equals(state)
                    || STATE_TIMED_OUT.equals(state)) {
                return currentResponse;
            }

            if (jobId == null) {
                return QueryResponseDto.builder()
                        .state(STATE_FAILED)
                        .error(
                                io.github.rawwise.frontend.api.dto.ErrorResponseDto.builder()
                                        .code("MISSING_JOB_ID")
                                        .message("Server returned no job ID for async query")
                                        .build())
                        .build();
            }

            long startTime = System.currentTimeMillis();
            long timeoutMs = timeoutSeconds * 1000L;
            // 轮询策略：先线性短间隔（2ms × 25 = 50ms）快速捕获 Spark SQL 快速完成的作业，
            // 再切换到指数退避（4→8→16→...→500ms）避免 busy loop。
            // 修复 p99=63ms 尾部：旧策略 1→2→4→8→16→32=63ms 导致 7-63ms 完成的作业被延迟。
            long pollInterval = POLL_LINEAR_INTERVAL_MS;
            int linearPollsRemaining = POLL_LINEAR_COUNT;

            while (System.currentTimeMillis() - startTime < timeoutMs) {

                String currentState = fetchJobStatus(jobId);

                if (STATE_SUCCEEDED.equals(currentState)) {
                    return fetchResult(jobId);
                }
                if (STATE_FAILED.equals(currentState)
                        || STATE_CANCELLED.equals(currentState)
                        || STATE_TIMED_OUT.equals(currentState)) {
                    return QueryResponseDto.builder().jobId(jobId).state(currentState).build();
                }

                try {
                    Thread.sleep(pollInterval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return QueryResponseDto.builder()
                            .jobId(jobId)
                            .state(STATE_CANCELLED)
                            .error(
                                    io.github.rawwise.frontend.api.dto.ErrorResponseDto.builder()
                                            .code("INTERRUPTED")
                                            .message("Query was interrupted")
                                            .build())
                            .build();
                }
                // 阶段 1：线性 2ms 间隔（前 50ms），快速捕获短作业
                // 阶段 2：指数退避 4→8→16→...→500ms，避免长时间 busy loop
                if (linearPollsRemaining > 0) {
                    linearPollsRemaining--;
                    if (linearPollsRemaining == 0) {
                        // 切换到指数退避，从 4ms 开始
                        pollInterval = POLL_EXPONENTIAL_INITIAL_MS;
                    }
                    // 线性阶段保持 2ms
                } else {
                    pollInterval = Math.min(pollInterval * 2, POLL_MAX_MS);
                }
            }

            return QueryResponseDto.builder()
                    .state(STATE_RUNNING)
                    .error(
                            io.github.rawwise.frontend.api.dto.ErrorResponseDto.builder()
                                    .code("TIMEOUT")
                                    .message("Query timed out after " + timeoutSeconds + " seconds")
                                    .build())
                    .build();
        } catch (Exception e) {
            log.error("Failed to execute sync query", e);
            return QueryResponseDto.builder()
                    .state(STATE_FAILED)
                    .error(
                            io.github.rawwise.frontend.api.dto.ErrorResponseDto.builder()
                                    .code("CONNECTION_ERROR")
                                    .message(e.getMessage())
                                    .build())
                    .build();
        }
    }

    @SuppressWarnings("unchecked")
    public List<String> listDatabases() {
        try {
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(buildUrl("/api/databases")))
                            .GET()
                            .timeout(Duration.ofSeconds(30))
                            .build();

            HttpResponse<String> response = sendWithRetry(request);

            if (response.statusCode() == 200) {
                Map<String, Object> result = MAPPER.readValue(response.body(), Map.class);
                Object databases = result.get("databases");
                if (databases instanceof List) {
                    return ((List<?>) databases).stream().map(Object::toString).toList();
                }
            }
            return List.of("default");
        } catch (Exception e) {
            log.warn("Failed to list databases", e);
            return List.of("default");
        }
    }

    @SuppressWarnings("unchecked")
    public List<String> listTables(String database) {
        try {
            String url = buildUrl("/api/tables");
            if (database != null && !database.equals("default")) {
                url += "?database=" + URLEncoder.encode(database, StandardCharsets.UTF_8);
            }

            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(url))
                            .GET()
                            .timeout(Duration.ofSeconds(30))
                            .build();

            HttpResponse<String> response = sendWithRetry(request);

            if (response.statusCode() == 200) {
                Map<String, Object> result = MAPPER.readValue(response.body(), Map.class);
                Object tables = result.get("tables");
                if (tables instanceof List) {
                    return ((List<?>) tables)
                            .stream()
                                    .map(
                                            obj -> {
                                                if (obj instanceof Map) {
                                                    return String.valueOf(
                                                            ((Map<?, ?>) obj).get("name"));
                                                }
                                                return obj.toString();
                                            })
                                    .toList();
                }
            }
            return List.of();
        } catch (Exception e) {
            log.warn("Failed to list tables", e);
            return List.of();
        }
    }

    String buildUrl(String path) {
        return baseUrl + path;
    }

    @Override
    public void close() {
        try {
            if (httpClient instanceof AutoCloseable ac) {
                ac.close();
            }
        } catch (Exception e) {
            log.warn("Failed to close HttpClient", e);
        }
    }
}
