package io.github.rawwise.e2e;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.engine.vector.EmbeddingProviderFactory;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 2 Task 2.1 failing E2E.
 *
 * <p>Starts the real production entry {@code io.github.rawwise.frontend.server.Main} as a child
 * process in {@code --embedded} mode (Frontend + in-process Backend + in-process Metastore), then
 * walks the {@code /api/v1/axiom/memory/*} production endpoints through {@code store -> recall
 * (depth=DETAIL) -> decay}.
 *
 * <p>Semantic-stack assertions (green target, AxiomMemoryFacade backend):
 *
 * <ul>
 *   <li>recall entry {@code content} no longer contains {@code l0Abstract}/{@code l1Overview} keys
 *   <li>{@code depth=DETAIL} content carries the original body text
 *   <li>decay leaves entries/bodies unchanged (1-arg AxiomMemoryFacade decay is a no-op)
 * </ul>
 *
 * <p>{@code Main.java} wires the {@code MemoryFacade} slot to {@code AxiomMemoryFacade}
 * (HTTP-backed AxiomClient over {@code /api/axiom/*}), so recall {@code content} must NOT contain
 * the legacy {@code l0Abstract}/{@code l1Overview} keys — this is the green assertion this test
 * locks in.
 */
@DisplayName(
        "MemoryFacade Rewire E2E - store/recall/decay through production /api/v1/axiom/memory/*")
class MemoryFacadeRewireE2ETest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    private static Process serverProcess;
    private static File serverLog;
    private static String frontendBaseUrl;
    private static int frontendPort;

    @TempDir static Path tempDir;

    @BeforeAll
    static void setUpAll() throws Exception {
        frontendPort = findFreePort();
        int metastoreGrpcPort = findFreePort();
        int prometheusPort = findFreePort();
        String warehousePath = tempDir.resolve("warehouse").toString();
        String tokenDir = tempDir.resolve("tokens").toString();
        String classpath = System.getProperty("java.class.path");
        String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        serverLog =
                Path.of(System.getProperty("java.io.tmpdir"), "rawwise-e2e-server.log").toFile();
        if (serverLog.exists()) {
            serverLog.delete();
        }

        List<String> cmd = new ArrayList<>();
        cmd.add(javaBin);
        cmd.add("--add-opens");
        cmd.add("java.base/java.nio=ALL-UNNAMED");
        cmd.add("--add-opens");
        cmd.add("java.base/sun.nio.ch=ALL-UNNAMED");
        cmd.add("--add-opens");
        cmd.add("java.base/java.lang=ALL-UNNAMED");
        cmd.add("--add-opens");
        cmd.add("java.base/java.lang.invoke=ALL-UNNAMED");
        cmd.add("--add-opens");
        cmd.add("java.base/java.util=ALL-UNNAMED");
        cmd.add("-Xmx2g");
        cmd.add("-Drawwise.warehouse.path=" + warehousePath);
        cmd.add("-Drawwise.metastore.type=inmemory");
        cmd.add("-Drawwise.auth.enabled=false");
        cmd.add("-Drawwise.auth.explicitly-unsafe-disable=true");
        cmd.add("-Drawwise.auth.token-dir=" + tokenDir);
        cmd.add("-Drawwise.axiom.orchestrator.enabled=false");
        cmd.add("-Drawwise.context.enabled=true");
        cmd.add("-Drawwise.frontend.ir.enabled=true");
        cmd.add("-Drawwise.frontend.grpc.tls.enabled=false");
        cmd.add("-Drawwise.axiom.enabled=true");
        cmd.add("-Drawwise.axiom.derivation.async=false");
        cmd.add("-Drawwise.axiom.cognition.stage.enabled=false");
        cmd.add("-D" + EmbeddingProviderFactory.KEY_TYPE + "=stub");
        cmd.add("-D" + EmbeddingProviderFactory.KEY_DIMENSION + "=16");
        cmd.add("-Drawwise.storage.lmdb.initial-map-size-mb=16");
        cmd.add("-Drawwise.storage.lmdb.max-map-size-mb=256");
        cmd.add("-Drawwise.metastore.grpc-port=" + metastoreGrpcPort);
        cmd.add("-Drawwise.metrics.prometheus-port=" + prometheusPort);
        // Point the MemoryFacade AxiomClient at this test instance's own /api/axiom/* endpoint,
        // not the default http://localhost:8080 (which may host a different process).
        cmd.add("-Drawwise.axiom.base-url=http://localhost:" + frontendPort);
        cmd.add("-cp");
        cmd.add(classpath);
        cmd.add("io.github.rawwise.frontend.server.Main");
        cmd.add("--port");
        cmd.add(String.valueOf(frontendPort));
        cmd.add("--embedded");

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(serverLog));
        // classpath rawwise.conf pins metastore.grpc-port=9001 and file config outranks
        // -D system properties; use the higher-priority env var to move it off any
        // shared/occupied fixed port.
        pb.environment().put("RAWWISE_METASTORE_GRPC__PORT", String.valueOf(metastoreGrpcPort));
        serverProcess = pb.start();

        await().atMost(90, SECONDS)
                .pollInterval(1, SECONDS)
                .untilAsserted(
                        () -> {
                            try {
                                HttpResponse<String> resp =
                                        HTTP.send(
                                                HttpRequest.newBuilder()
                                                        .uri(
                                                                URI.create(
                                                                        "http://localhost:"
                                                                                + frontendPort
                                                                                + "/health"))
                                                        .GET()
                                                        .build(),
                                                HttpResponse.BodyHandlers.ofString());
                                assertThat(resp.statusCode())
                                        .as("Main --embedded should be ready")
                                        .isEqualTo(200);
                            } catch (IOException e) {
                                throw new AssertionError(
                                        "Main --embedded not ready, log: " + serverLog, e);
                            }
                        });

        frontendBaseUrl = "http://localhost:" + frontendPort;
    }

    @AfterAll
    static void tearDownAll() {
        if (serverProcess != null) {
            serverProcess.destroy();
            try {
                serverProcess.waitFor(15, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
            if (serverProcess.isAlive()) {
                serverProcess.destroyForcibly();
            }
        }
    }

    @Test
    @DisplayName("REWIRE: store -> recall(DETAIL) -> decay through production memory endpoints")
    void storeRecallDecayThroughProductionEntry() throws Exception {
        bootstrapAgentMemoryTable();

        String storeResp =
                postJson(
                        "/api/v1/axiom/memory/store",
                        MAPPER.writeValueAsString(
                                Map.of(
                                        "content",
                                        Map.of(
                                                "text",
                                                "LMDB write transaction serialization uses a "
                                                        + "ReentrantLock per LmdbInstance to "
                                                        + "serialize concurrent writes."),
                                        "memoryTier",
                                        "EPISODIC",
                                        "importance",
                                        5)));
        JsonNode storeJson = MAPPER.readTree(storeResp);
        assertThat(storeJson.has("error")).as("store should succeed: %s", storeResp).isFalse();
        assertThat(storeJson.get("entryId").asText())
                .as("store should return an entryId")
                .isNotEmpty();

        String recallResp =
                postJson(
                        "/api/v1/axiom/memory/recall",
                        MAPPER.writeValueAsString(
                                Map.of(
                                        "query",
                                        "LMDB write transaction serialization",
                                        "topK",
                                        5,
                                        "memoryTier",
                                        "EPISODIC",
                                        "depth",
                                        "DETAIL")));
        JsonNode recallJson = MAPPER.readTree(recallResp);
        assertThat(recallJson.has("error")).as("recall should succeed: %s", recallResp).isFalse();
        assertThat(recallJson.get("entries").size())
                .as("recall should return hits: %s", recallResp)
                .isGreaterThan(0);

        JsonNode content = recallJson.get("entries").get(0).get("content");
        assertThat(content.has("l0Abstract"))
                .as("recall content must not contain legacy l0Abstract key: %s", recallResp)
                .isFalse();
        assertThat(content.has("l1Overview"))
                .as("recall content must not contain legacy l1Overview key: %s", recallResp)
                .isFalse();
        assertThat(content.get("body").asText())
                .as("depth=DETAIL must include original body text")
                .contains("LMDB write transaction serialization");

        List<String> idsBefore = recallEntryIds(recallJson);

        String decayResp =
                postJson(
                        "/api/v1/axiom/memory/decay",
                        MAPPER.writeValueAsString(Map.of("ageMs", 86_400_000L)));
        JsonNode decayJson = MAPPER.readTree(decayResp);
        assertThat(decayJson.has("error")).as("decay should not error: %s", decayResp).isFalse();
        assertThat(decayJson.get("status").asText()).as("decay should return ok").isEqualTo("ok");

        String recallAfterResp =
                postJson(
                        "/api/v1/axiom/memory/recall",
                        MAPPER.writeValueAsString(
                                Map.of(
                                        "query",
                                        "LMDB write transaction serialization",
                                        "topK",
                                        5,
                                        "memoryTier",
                                        "EPISODIC",
                                        "depth",
                                        "DETAIL")));
        JsonNode recallAfterJson = MAPPER.readTree(recallAfterResp);
        assertThat(recallAfterJson.has("error"))
                .as("recall after decay should succeed: %s", recallAfterResp)
                .isFalse();
        List<String> idsAfter = recallEntryIds(recallAfterJson);
        assertThat(idsAfter)
                .as("decay must not drop or re-derive entries (View/Raw unchanged)")
                .containsExactlyElementsOf(idsBefore);

        String statusResp = get("/api/v1/axiom/memory/status");
        JsonNode statusJson = MAPPER.readTree(statusResp);
        assertThat(statusJson.has("error"))
                .as("status should be healthy after decay: %s", statusResp)
                .isFalse();
        assertThat(statusJson.has("totalCount")).as("status should expose total count").isTrue();
    }

    private static List<String> recallEntryIds(JsonNode recallJson) {
        List<String> ids = new ArrayList<>();
        for (JsonNode entry : recallJson.get("entries")) {
            ids.add(entry.path("entryId").asText());
        }
        return ids;
    }

    private String bootstrapAgentMemoryTable() throws Exception {
        return executeDdl(
                "CREATE TABLE IF NOT EXISTS default.agent_memory ("
                        + "  entry_id STRING, agent_id STRING, memory_tier STRING,"
                        + "  importance INT, content STRING"
                        + ") USING rawwise TBLPROPERTIES ("
                        + "  'primary-key'='entry_id',"
                        + "  'rawwise.ai-native'='true',"
                        + "  'derivation.strategy'='auto'"
                        + ")");
    }

    private String executeDdl(String sql) throws Exception {
        String body =
                MAPPER.writeValueAsString(
                        Map.of("sql", sql, "type", "DDL", "options", Map.of("collect", "true")));
        String submitResp = postJson("/api/query", body);
        JsonNode submitJson = MAPPER.readTree(submitResp);
        String jobId = submitJson.get("jobId").asText();
        assertThat(jobId).isNotEmpty();

        if (submitJson.has("state") && !"PENDING".equals(submitJson.get("state").asText())) {
            return submitResp;
        }

        await().atMost(60, SECONDS)
                .untilAsserted(
                        () -> {
                            String statusResp = get("/api/query/" + jobId + "/status");
                            String state = MAPPER.readTree(statusResp).get("state").asText();
                            assertThat(state).isIn("SUCCEEDED", "FAILED");
                        });

        return get("/api/query/" + jobId + "/result");
    }

    private String postJson(String path, String body) throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(frontendBaseUrl + path))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

    private String get(String path) throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder().uri(URI.create(frontendBaseUrl + path)).GET().build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
