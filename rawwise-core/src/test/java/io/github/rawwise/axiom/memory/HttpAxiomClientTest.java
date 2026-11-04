package io.github.rawwise.axiom.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.axiom.memory.exception.AxiomClientException;
import io.javalin.Javalin;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("HttpAxiomClient - AxiomClient HTTP indeedpresent")
class HttpAxiomClientTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Javalin app;
    private int port;
    private HttpAxiomClient client;

    private final AtomicReference<String> lastAuthHeader = new AtomicReference<>();
    private final AtomicReference<JsonNode> lastIngestBody = new AtomicReference<>();
    private final AtomicReference<JsonNode> lastSearchBody = new AtomicReference<>();

    @BeforeEach
    void setUp() {
        app = Javalin.create();
        app.post(
                "/api/axiom/ingest",
                ctx -> {
                    lastAuthHeader.set(ctx.header("Authorization"));
                    JsonNode body = MAPPER.readTree(ctx.body());
                    lastIngestBody.set(body);
                    ctx.status(201)
                            .json(
                                    Map.of(
                                            "recordKey",
                                            body.path("key").asText(),
                                            "chunkCount",
                                            1,
                                            "status",
                                            "OK"));
                });
        app.post(
                "/api/axiom/search",
                ctx -> {
                    lastAuthHeader.set(ctx.header("Authorization"));
                    JsonNode body = MAPPER.readTree(ctx.body());
                    lastSearchBody.set(body);
                    List<Map<String, Object>> results =
                            List.of(
                                    Map.of(
                                            "entryKey", "rec:entry-1",
                                            "viewId", "rec:entry-1",
                                            "score", 0.95,
                                            "summary", "test summary",
                                            "body", "raw body for entry-1",
                                            "properties",
                                                    Map.of(
                                                            "agent_id", "agent-1",
                                                            "memory_tier", "EPISODIC")),
                                    Map.of(
                                            "entryKey", "rec:entry-2",
                                            "viewId", "rec:entry-2",
                                            "score", 0.80,
                                            "summary", "another summary",
                                            "body", "raw body for entry-2",
                                            "properties",
                                                    Map.of(
                                                            "agent_id", "agent-2",
                                                            "memory_tier", "SEMANTIC")));
                    ctx.json(Map.of("results", results));
                });
        app.post(
                "/api/axiom/fetch-details",
                ctx -> {
                    lastAuthHeader.set(ctx.header("Authorization"));
                    JsonNode body = MAPPER.readTree(ctx.body());
                    String entryKey = body.path("entryKeys").path(0).asText();
                    Map<String, Object> result = new LinkedHashMap<>();
                    result.put("key", entryKey);
                    result.put("body", "raw body text for " + entryKey);
                    result.put("properties", Map.of("agent_id", "agent-1"));
                    ctx.json(Map.of("results", List.of(result)));
                });
        app.start(0);
        port = app.port();
        client = new HttpAxiomClient("http://localhost:" + port);
    }

    @AfterEach
    void tearDown() {
        if (app != null) {
            app.stop();
        }
    }

    @Nested
    @DisplayName("ingest - write")
    class Ingest {

        @Test
        @DisplayName("ingest call POST /api/axiom/ingest,returns recordKey")
        void ingestReturnsRecordKey() {
            Map<String, String> properties = new LinkedHashMap<>();
            properties.put("agent_id", "agent-1");
            properties.put("memory_tier", "EPISODIC");

            String recordKey =
                    client.ingest("agent_memory", "entry-123", "body content", properties);

            assertThat(recordKey).isEqualTo("entry-123");
            assertThat(lastIngestBody.get()).isNotNull();
            assertThat(lastIngestBody.get().path("tableId").asText()).isEqualTo("agent_memory");
            assertThat(lastIngestBody.get().path("key").asText()).isEqualTo("entry-123");
            assertThat(lastIngestBody.get().path("body").asText()).isEqualTo("body content");
            assertThat(lastIngestBody.get().path("immutable").asBoolean()).isFalse();
            assertThat(lastIngestBody.get().path("properties").path("agent_id").asText())
                    .isEqualTo("agent-1");
        }

        @Test
        @DisplayName("ingest body is null whenpassempty string")
        void ingestNullBodySendsEmptyString() {
            String recordKey = client.ingest("agent_memory", "k1", null, Map.of());
            assertThat(recordKey).isEqualTo("k1");
            assertThat(lastIngestBody.get().path("body").asText()).isEmpty();
        }

        @Test
        @DisplayName("ingest properties is null whenpassempty map")
        void ingestNullPropertiesSendsEmptyMap() {
            client.ingest("agent_memory", "k2", "body", null);
            assertThat(lastIngestBody.get().path("properties").isObject()).isTrue();
            assertThat(lastIngestBody.get().path("properties").size()).isZero();
        }
    }

    @Nested
    @DisplayName("search - retrieve")
    class Search {

        @Test
        @DisplayName("search call POST /api/axiom/search,returnsresultlist")
        void searchReturnsResultsList() {
            List<Map<String, Object>> results =
                    client.search("agent_memory", "query text", 5, "agent-1");

            assertThat(results).hasSize(2);
            assertThat(results.get(0)).containsEntry("entryKey", "rec:entry-1");
            assertThat(results.get(0)).containsEntry("score", 0.95);
            assertThat(results.get(0)).containsEntry("summary", "test summary");
            assertThat(results.get(1)).containsEntry("entryKey", "rec:entry-2");
        }

        @Test
        @DisplayName("search requestbodycontains tableId, query, topK, snapshotId")
        void searchRequestBodyIsCorrect() {
            client.search("agent_memory", "hello", 10, "agent-1");

            assertThat(lastSearchBody.get()).isNotNull();
            assertThat(lastSearchBody.get().path("tableId").asText()).isEqualTo("agent_memory");
            assertThat(lastSearchBody.get().path("query").asText()).isEqualTo("hello");
            assertThat(lastSearchBody.get().path("topK").asInt()).isEqualTo(10);
            assertThat(lastSearchBody.get().path("snapshotId").asLong()).isZero();
        }

        @Test
        @DisplayName("search topK <= 0 whendefaultis 10")
        void searchTopKDefaultsTo10() {
            client.search("agent_memory", "hello", 0, null);
            assertThat(lastSearchBody.get().path("topK").asInt()).isEqualTo(10);
        }

        @Test
        @DisplayName("search namespace non-emptywhenresult with properties.agent_id")
        void searchWithNamespaceAddsProperties() {
            List<Map<String, Object>> results = client.search("agent_memory", "q", 5, "agent-99");
            @SuppressWarnings("unchecked")
            Map<String, String> props = (Map<String, String>) results.get(0).get("properties");
            assertThat(props).containsEntry("agent_id", "agent-1");
            assertThat(props).containsEntry("memory_tier", "EPISODIC");
        }

        @Test
        @DisplayName("search passes filter into request body")
        void searchPassesFilter() {
            client.search(
                    "agent_memory",
                    "hello",
                    10,
                    "agent-1",
                    Map.of("agent_id", "alice", "memory_tier", "episodic"));

            assertThat(lastSearchBody.get()).isNotNull();
            assertThat(lastSearchBody.get().path("filter").path("agent_id").asText())
                    .isEqualTo("alice");
            assertThat(lastSearchBody.get().path("filter").path("memory_tier").asText())
                    .isEqualTo("episodic");
        }

        @Test
        @DisplayName("search request body carries empty filter when null")
        void searchNullFilterSendsEmptyMap() {
            client.search("agent_memory", "hello", 10, "agent-1", null);
            assertThat(lastSearchBody.get().path("filter").isObject()).isTrue();
            assertThat(lastSearchBody.get().path("filter").size()).isZero();
        }
    }

    @Nested
    @DisplayName("getEntry - singleentriesread")
    class GetEntry {

        @Test
        @DisplayName("getEntry call fetch-details,returnswith body 's entry map")
        void getEntryReturnsEntryMap() {
            Optional<Map<String, Object>> entry = client.getEntry("agent_memory", "rec:entry-1");

            assertThat(entry).isPresent();
            assertThat(entry.get()).containsEntry("tableId", "agent_memory");
            assertThat(entry.get()).containsEntry("key", "rec:entry-1");
            assertThat(entry.get()).containsEntry("body", "raw body text for rec:entry-1");
            assertThat(entry.get()).containsKey("properties");
        }
    }

    @Nested
    @DisplayName("scan - batchscan")
    class Scan {

        @Test
        @DisplayName("scan call search,returnsmappingafter's entries")
        void scanReturnsMappedEntries() {
            List<Map<String, Object>> entries = client.scan("agent_memory", "agent-1");

            assertThat(entries).hasSize(2);
            assertThat(entries.get(0)).containsEntry("key", "rec:entry-1");
            assertThat(entries.get(0)).containsEntry("body", "raw body for entry-1");
            @SuppressWarnings("unchecked")
            Map<String, String> props = (Map<String, String>) entries.get(0).get("properties");
            assertThat(props).containsEntry("agent_id", "agent-1");
        }
    }

    @Nested
    @DisplayName("authToken - auth header injection")
    class AuthToken {

        @Test
        @DisplayName("authToken injectto Authorization header (Bearer)")
        void authTokenInjectedIntoHeader() {
            HttpAxiomClient authClient =
                    new HttpAxiomClient("http://localhost:" + port, "my-secret-token");
            authClient.ingest("agent_memory", "k1", "body", Map.of());

            assertThat(lastAuthHeader.get()).isEqualTo("Bearer my-secret-token");
        }

        @Test
        @DisplayName("no authToken whennotsend Authorization header")
        void noAuthTokenNoHeader() {
            client.ingest("agent_memory", "k1", "body", Map.of());
            assertThat(lastAuthHeader.get()).isNull();
        }
    }

    @Nested
    @DisplayName("fault tolerance - mustnotcanreachorexception")
    class FaultTolerance {

        @Test
        @DisplayName("mustnotcanreachwhen ingest returns key(does not throw)")
        void unreachableServerIngestReturnsKey() {
            HttpAxiomClient unreachable = new HttpAxiomClient("http://localhost:1");
            String recordKey = unreachable.ingest("agent_memory", "fallback-key", "body", Map.of());
            assertThat(recordKey).isEqualTo("fallback-key");
        }

        @Test
        @DisplayName("mustnotcanreachwhen search returns empty list(does not throw)")
        void unreachableServerSearchReturnsEmpty() {
            HttpAxiomClient unreachable = new HttpAxiomClient("http://localhost:1");
            List<Map<String, Object>> results = unreachable.search("agent_memory", "q", 5, "a");
            assertThat(results).isEmpty();
        }

        @Test
        @DisplayName("mustnotcanreachwhen getEntry returns empty(does not throw)")
        void unreachableServerGetEntryReturnsEmpty() {
            HttpAxiomClient unreachable = new HttpAxiomClient("http://localhost:1");
            Optional<Map<String, Object>> entry = unreachable.getEntry("agent_memory", "k");
            assertThat(entry).isEmpty();
        }

        @Test
        @DisplayName("mustnotcanreachwhen scan returns empty list(does not throw)")
        void unreachableServerScanReturnsEmpty() {
            HttpAxiomClient unreachable = new HttpAxiomClient("http://localhost:1");
            List<Map<String, Object>> entries = unreachable.scan("agent_memory", "a");
            assertThat(entries).isEmpty();
        }

        @Test
        @DisplayName("ingest HTTP 500 whenreturns key(degradation)")
        void ingestServerErrorReturnsKey() {
            app.stop();
            app = Javalin.create();
            app.post("/api/axiom/ingest", ctx -> ctx.status(500).json(Map.of("error", "boom")));
            app.post("/api/axiom/search", ctx -> ctx.status(500).json(Map.of("error", "boom")));
            app.post(
                    "/api/axiom/fetch-details",
                    ctx -> ctx.status(500).json(Map.of("error", "boom")));
            app.start(0);
            port = app.port();
            HttpAxiomClient errorClient = new HttpAxiomClient("http://localhost:" + port);

            assertThat(errorClient.ingest("t", "err-key", "b", Map.of())).isEqualTo("err-key");
            assertThat(errorClient.search("t", "q", 5, "a")).isEmpty();
            assertThat(errorClient.getEntry("t", "k")).isEmpty();
        }

        @Test
        @DisplayName(
                "ingest HTTP 409 whenthrows AxiomClientException (deterministic business error)")
        void ingestClientErrorThrows() {
            app.stop();
            app = Javalin.create();
            app.post(
                    "/api/axiom/ingest",
                    ctx -> ctx.status(409).json(Map.of("error", "Table not registered")));
            app.start(0);
            HttpAxiomClient errorClient = new HttpAxiomClient("http://localhost:" + app.port());

            assertThatThrownBy(() -> errorClient.ingest("t", "k", "b", Map.of()))
                    .isInstanceOf(AxiomClientException.class)
                    .hasMessageContaining("HTTP 409");
        }

        @Test
        @DisplayName("search HTTP 400 whenthrows AxiomClientException")
        void searchClientErrorThrows() {
            app.stop();
            app = Javalin.create();
            app.post("/api/axiom/search", ctx -> ctx.status(400).json(Map.of("error", "bad")));
            app.start(0);
            HttpAxiomClient errorClient = new HttpAxiomClient("http://localhost:" + app.port());

            assertThatThrownBy(() -> errorClient.search("t", "q", 5, "a"))
                    .isInstanceOf(AxiomClientException.class)
                    .hasMessageContaining("HTTP 400");
        }

        @Test
        @DisplayName("getEntry HTTP 404 whenthrows AxiomClientException")
        void getEntryClientErrorThrows() {
            app.stop();
            app = Javalin.create();
            app.post(
                    "/api/axiom/fetch-details",
                    ctx -> ctx.status(404).json(Map.of("error", "not found")));
            app.start(0);
            HttpAxiomClient errorClient = new HttpAxiomClient("http://localhost:" + app.port());

            assertThatThrownBy(() -> errorClient.getEntry("t", "k"))
                    .isInstanceOf(AxiomClientException.class)
                    .hasMessageContaining("HTTP 404");
        }
    }

    @Nested
    @DisplayName("baseUrl normalization")
    class BaseUrlNormalization {

        @Test
        @DisplayName("baseUrl tailberemove")
        void trailingSlashRemoved() throws Exception {
            HttpAxiomClient slashClient = new HttpAxiomClient("http://localhost:" + port + "/");
            String recordKey = slashClient.ingest("agent_memory", "slash-key", "body", Map.of());
            assertThat(recordKey).isEqualTo("slash-key");
        }
    }
}
