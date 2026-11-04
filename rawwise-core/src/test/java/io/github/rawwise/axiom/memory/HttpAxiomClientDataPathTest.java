package io.github.rawwise.axiom.memory;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Task 1.4 - HttpAxiomClient data path (filter passthrough / getEntry / scan)")
class HttpAxiomClientDataPathTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private HttpServer server;
    private HttpAxiomClient client;
    private final List<String> capturedSearchBodies = new ArrayList<>();
    private final AtomicReference<String> capturedFetchBody = new AtomicReference<>();

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext(
                "/api/axiom/search",
                exchange -> {
                    capturedSearchBodies.add(readBody(exchange));
                    writeJson(
                            exchange,
                            200,
                            "{\"results\":["
                                    + "{\"key\":\"rec:entry-1\","
                                    + "\"entryKey\":\"rec:entry-1\","
                                    + "\"viewId\":\"rec:entry-1\","
                                    + "\"score\":0.95,"
                                    + "\"summary\":\"test summary\","
                                    + "\"body\":\"raw body for entry-1\","
                                    + "\"properties\":{\"agent_id\":\"agent-1\",\"memory_tier\":\"episodic\"}}"
                                    + "]}");
                });
        server.createContext(
                "/api/axiom/fetch-details",
                exchange -> {
                    capturedFetchBody.set(readBody(exchange));
                    writeJson(
                            exchange,
                            200,
                            "{\"results\":["
                                    + "{\"key\":\"rec:entry-1\","
                                    + "\"body\":\"raw body text for rec:entry-1\","
                                    + "\"properties\":{\"agent_id\":\"agent-1\",\"memory_tier\":\"episodic\"}}"
                                    + "]}");
                });
        server.start();
        int port = server.getAddress().getPort();
        client = new HttpAxiomClient("http://localhost:" + port);
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.stop(0);
    }

    private static String readBody(HttpExchange exchange) throws IOException {
        return new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    }

    private static void writeJson(HttpExchange exchange, int status, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    @Test
    @DisplayName("search passes filter into POST /api/axiom/search body")
    void searchPassesFilterIntoRequestBody() {
        client.search(
                "agent_memory",
                "hello",
                5,
                "agent-1",
                Map.of("agent_id", "alice", "memory_tier", "episodic"));

        assertThat(capturedSearchBodies).hasSize(1);
        try {
            var node = MAPPER.readTree(capturedSearchBodies.get(0));
            assertThat(node.path("filter").path("agent_id").asText()).isEqualTo("alice");
            assertThat(node.path("filter").path("memory_tier").asText()).isEqualTo("episodic");
            assertThat(node.path("tableId").asText()).isEqualTo("agent_memory");
            assertThat(node.path("query").asText()).isEqualTo("hello");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @DisplayName("getEntry parses results[] and returns real properties + body")
    void getEntryParsesResultsWithBodyAndProperties() {
        Optional<Map<String, Object>> entry = client.getEntry("agent_memory", "rec:entry-1");

        assertThat(entry).isPresent();
        assertThat(entry.get()).containsEntry("tableId", "agent_memory");
        assertThat(entry.get()).containsEntry("key", "rec:entry-1");
        assertThat(entry.get()).containsEntry("body", "raw body text for rec:entry-1");
        @SuppressWarnings("unchecked")
        Map<String, String> props = (Map<String, String>) entry.get().get("properties");
        assertThat(props).containsEntry("agent_id", "agent-1");
        assertThat(props).containsEntry("memory_tier", "episodic");
    }

    @Test
    @DisplayName("getEntry returns empty when key not present in results[]")
    void getEntryReturnsEmptyWhenKeyAbsent() {
        Optional<Map<String, Object>> entry = client.getEntry("agent_memory", "rec:missing");
        assertThat(entry).isEmpty();
    }

    @Test
    @DisplayName("scan returns original body (not summary) for each entry")
    void scanReturnsOriginalBodyNotSummary() {
        List<Map<String, Object>> entries = client.scan("agent_memory", "agent-1");

        assertThat(entries).hasSize(1);
        assertThat(entries.get(0)).containsEntry("key", "rec:entry-1");
        assertThat(entries.get(0))
                .as("scan body must be the original record body, not the summary")
                .containsEntry("body", "raw body for entry-1");
        @SuppressWarnings("unchecked")
        Map<String, String> props = (Map<String, String>) entries.get(0).get("properties");
        assertThat(props).containsEntry("agent_id", "agent-1");
        assertThat(props).containsEntry("memory_tier", "episodic");
    }

    @Test
    @DisplayName("scan passes agent_id filter into POST /api/axiom/search body")
    void scanPassesAgentIdFilterIntoRequestBody() {
        client.scan("agent_memory", "agent-1");

        assertThat(capturedSearchBodies).hasSize(1);
        try {
            var node = MAPPER.readTree(capturedSearchBodies.get(0));
            assertThat(node.path("filter").path("agent_id").asText())
                    .as("scan must scope search by agent_id to avoid cross-agent counting")
                    .isEqualTo("agent-1");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
