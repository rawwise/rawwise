package io.github.rawwise.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.backend.server.EmbeddedBackend;
import io.github.rawwise.frontend.api.FrontendServer;
import io.github.rawwise.frontend.backend.GrpcBackendClient;
import io.github.rawwise.frontend.config.FrontendOptions;
import io.github.rawwise.frontend.evaluator.DefaultQueryEvaluator;
import io.github.rawwise.frontend.mcp.RawWiseMcpConfig;
import io.github.rawwise.frontend.mcp.RawWiseMcpPrompts;
import io.github.rawwise.frontend.mcp.RawWiseMcpResources;
import io.github.rawwise.frontend.mcp.RawWiseMcpServer;
import io.github.rawwise.frontend.mcp.RawWiseMcpTools;
import io.github.rawwise.frontend.session.FrontendSessionStore;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("RawWise MCP Integration E2E Test")
class RawWiseMcpIntegrationE2ETest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    private static EmbeddedBackend embeddedBackend;
    private static FrontendServer frontendServer;
    private static GrpcBackendClient backendClient;
    private static String frontendBaseUrl;
    private static int requestId;

    @TempDir static Path tempDir;

    @BeforeAll
    static void setUpAll() throws Exception {
        System.setProperty("rawwise.warehouse.path", tempDir.resolve("warehouse").toString());
        System.setProperty("rawwise.storage.lmdb.initial-map-size-mb", "16");
        System.setProperty("rawwise.storage.lmdb.max-map-size-mb", "256");
        System.setProperty("rawwise.metastore.type", "inmemory");
        System.setProperty("rawwise.mcp.enabled", "true");
        System.setProperty("rawwise.mcp.tools.ingest.enabled", "true");
        System.setProperty("rawwise.mcp.tools.search.enabled", "true");
        System.setProperty("rawwise.mcp.tools.annotate.enabled", "true");
        System.setProperty("rawwise.mcp.tools.provenance.enabled", "true");
        System.setProperty("rawwise.mcp.tools.sql.enabled", "true");
        System.setProperty("rawwise.mcp.tools.list-tables.enabled", "true");
        System.setProperty("rawwise.mcp.resources.enabled", "true");
        System.setProperty("rawwise.mcp.prompts.enabled", "true");

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("rawwise.metastore.type", "inmemory");
        RawWiseConfig config = RawWiseConfig.fromMap(configMap);

        embeddedBackend = new EmbeddedBackend(config);
        embeddedBackend.start();

        backendClient = GrpcBackendClient.forInProcess(EmbeddedBackend.BACKEND_INPROC_NAME, 60);

        FrontendSessionStore sessionStore = new FrontendSessionStore();
        DefaultQueryEvaluator evaluator = new DefaultQueryEvaluator();

        ManagedChannel axiomChannel =
                InProcessChannelBuilder.forName(EmbeddedBackend.BACKEND_INPROC_NAME)
                        .directExecutor()
                        .build();
        io.github.rawwise.proto.axiom.AxiomServiceGrpc.AxiomServiceBlockingStub axiomStub =
                io.github.rawwise.proto.axiom.AxiomServiceGrpc.newBlockingStub(axiomChannel);

        RawWiseMcpConfig mcpConfig = RawWiseMcpConfig.fromSystemProperties();
        RawWiseMcpTools mcpTools =
                new RawWiseMcpTools(axiomStub, backendClient, mcpConfig, null, null, null);
        RawWiseMcpResources mcpResources = new RawWiseMcpResources(backendClient, axiomStub, null);
        RawWiseMcpPrompts mcpPrompts = new RawWiseMcpPrompts();
        RawWiseMcpServer mcpServer =
                new RawWiseMcpServer(mcpTools, mcpConfig, mcpResources, mcpPrompts);

        Map<String, Object> frontendConfig = new HashMap<>();
        frontendConfig.put("frontend.deployment.mode", "sidecar");
        frontendConfig.put("auth.enabled", false);
        frontendConfig.put("rawwise.frontend.port", 0);
        frontendConfig.put("rawwise.orchestrator.enabled", false);
        frontendConfig.put("rawwise.axiom.enabled", false);
        FrontendOptions options = new FrontendOptions(frontendConfig);

        frontendServer =
                new FrontendServer(
                        options,
                        sessionStore,
                        backendClient,
                        evaluator,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        mcpServer);

        frontendServer.start();
        frontendBaseUrl = "http://localhost:" + frontendServer.getPort();
        requestId = 0;
    }

    @AfterAll
    static void tearDownAll() {
        if (frontendServer != null) frontendServer.stop();
        if (backendClient != null) backendClient.close();
        if (embeddedBackend != null) embeddedBackend.close();
        System.clearProperty("rawwise.warehouse.path");
        System.clearProperty("rawwise.storage.lmdb.initial-map-size-mb");
        System.clearProperty("rawwise.storage.lmdb.max-map-size-mb");
        System.clearProperty("rawwise.metastore.type");
        System.clearProperty("rawwise.mcp.enabled");
        System.clearProperty("rawwise.mcp.resources.enabled");
        System.clearProperty("rawwise.mcp.prompts.enabled");
    }

    @Test
    @DisplayName("S1: tools/list returns available tools")
    void testMcpToolsList() throws Exception {
        var resp = mcpRequest("tools/list", null);
        assertThat(resp.statusCode()).isEqualTo(200);
        JsonNode json = MAPPER.readTree(resp.body());
        assertThat(json.has("result")).isTrue();
        assertThat(json.get("result").has("tools")).isTrue();
        var tools = json.get("result").get("tools");
        assertThat(tools.isArray()).isTrue();
        assertThat(tools.size()).isGreaterThanOrEqualTo(32);
    }

    @Test
    @DisplayName("S1: tools/call rawwise_list_tables returns result")
    void testMcpListTables() throws Exception {
        var resp =
                mcpRequest(
                        "tools/call",
                        MAPPER.createObjectNode()
                                .put("name", "rawwise_list_tables")
                                .putNull("arguments"));
        assertThat(resp.statusCode()).isEqualTo(200);
        JsonNode json = MAPPER.readTree(resp.body());
        assertThat(json.has("result")).isTrue();
    }

    @Test
    @DisplayName("S3: missing method returns JSON-RPC error")
    void testMcpMissingMethod() throws Exception {
        var req = MAPPER.createObjectNode().put("jsonrpc", "2.0").put("id", "1");
        var resp = postJson(req.toString());
        assertThat(resp.statusCode()).isEqualTo(200);
        JsonNode json = MAPPER.readTree(resp.body());
        assertThat(json.has("error")).isTrue();
        assertThat(json.get("error").get("code").asInt()).isEqualTo(-32600);
    }

    @Test
    @DisplayName("S3: unknown method returns Method not found")
    void testMcpUnknownMethod() throws Exception {
        var resp = mcpRequest("unknown/method", null);
        assertThat(resp.statusCode()).isEqualTo(200);
        JsonNode json = MAPPER.readTree(resp.body());
        assertThat(json.has("error")).isTrue();
        assertThat(json.get("error").get("code").asInt()).isEqualTo(-32601);
    }

    @Test
    @DisplayName("S3: invalid JSON returns parse error")
    void testMcpInvalidJson() throws Exception {
        var resp = postJson("not json");
        assertThat(resp.statusCode()).isEqualTo(400);
        JsonNode json = MAPPER.readTree(resp.body());
        assertThat(json.has("error")).isTrue();
        assertThat(json.get("error").get("code").asInt()).isEqualTo(-32700);
    }

    @Test
    @DisplayName("S4: tools/call with unknown tool name returns error")
    void testMcpUnknownTool() throws Exception {
        var resp =
                mcpRequest(
                        "tools/call",
                        MAPPER.createObjectNode()
                                .put("name", "nonexistent_tool")
                                .putNull("arguments"));
        assertThat(resp.statusCode()).isEqualTo(200);
        JsonNode json = MAPPER.readTree(resp.body());
        assertThat(json.has("result")).isTrue();
        var content = json.get("result").get("content").get(0).get("text").asText();
        assertThat(content).contains("Unknown tool");
    }

    @Test
    @DisplayName("S1: initialize handshake")
    void testMcpInitialize() throws Exception {
        var resp =
                mcpRequest(
                        "initialize",
                        MAPPER.createObjectNode()
                                .put("protocolVersion", "2024-11-05")
                                .putObject("clientInfo")
                                .put("name", "test-client")
                                .put("version", "1.0"));
        assertThat(resp.statusCode()).isEqualTo(200);
        JsonNode json = MAPPER.readTree(resp.body());
        assertThat(json.get("result").get("protocolVersion").asText()).isEqualTo("2024-11-05");
        assertThat(json.get("result").get("serverInfo").get("name").asText())
                .isEqualTo("RawWise MCP Server");
    }

    @Test
    @DisplayName("S1: initialize returns resources and prompts capabilities")
    void testMcpInitializeCapabilities() throws Exception {
        var resp =
                mcpRequest(
                        "initialize",
                        MAPPER.createObjectNode()
                                .put("protocolVersion", "2024-11-05")
                                .putObject("clientInfo")
                                .put("name", "test-client")
                                .put("version", "1.0"));
        assertThat(resp.statusCode()).isEqualTo(200);
        JsonNode json = MAPPER.readTree(resp.body());
        JsonNode capabilities = json.get("result").get("capabilities");
        assertThat(capabilities.has("resources")).isTrue();
        assertThat(capabilities.has("prompts")).isTrue();
    }

    @Test
    @DisplayName("S1: resources/list returns 4 resource definitions")
    void testMcpResourcesList() throws Exception {
        var resp = mcpRequest("resources/list", null);
        assertThat(resp.statusCode()).isEqualTo(200);
        JsonNode json = MAPPER.readTree(resp.body());
        assertThat(json.has("result")).isTrue();
        JsonNode resources = json.get("result").get("resources");
        assertThat(resources.isArray()).isTrue();
        assertThat(resources.size()).isEqualTo(4);
        boolean hasTablesResource = false;
        for (JsonNode resource : resources) {
            String uri = resource.get("uri").asText();
            if (uri.equals("rawwise://tables")) {
                hasTablesResource = true;
                break;
            }
        }
        assertThat(hasTablesResource).isTrue();
    }

    @Test
    @DisplayName("S1: resources/read rawwise://tables returns table list")
    void testMcpResourcesReadTables() throws Exception {
        var resp =
                mcpRequest(
                        "resources/read", MAPPER.createObjectNode().put("uri", "rawwise://tables"));
        assertThat(resp.statusCode()).isEqualTo(200);
        JsonNode json = MAPPER.readTree(resp.body());
        assertThat(json.has("result")).isTrue();
        JsonNode contents = json.get("result").get("contents");
        assertThat(contents.isArray()).isTrue();
        assertThat(contents.size()).isEqualTo(1);
        assertThat(contents.get(0).get("uri").asText()).isEqualTo("rawwise://tables");
        assertThat(contents.get(0).get("mimeType").asText()).isEqualTo("application/json");
        String text = contents.get(0).get("text").asText();
        assertThat(text).isNotEmpty();
    }

    @Test
    @DisplayName("S1: resources/read unknown URI returns error in content")
    void testMcpResourcesReadUnknown() throws Exception {
        var resp =
                mcpRequest(
                        "resources/read",
                        MAPPER.createObjectNode().put("uri", "rawwise://unknown/uri"));
        assertThat(resp.statusCode()).isEqualTo(200);
        JsonNode json = MAPPER.readTree(resp.body());
        assertThat(json.has("result")).isTrue();
        String text = json.get("result").get("contents").get(0).get("text").asText();
        assertThat(text).contains("Unknown resource URI");
    }

    @Test
    @DisplayName("S1: resources/read without uri param returns JSON-RPC error")
    void testMcpResourcesReadMissingUri() throws Exception {
        var resp = mcpRequest("resources/read", MAPPER.createObjectNode().putNull("uri"));
        assertThat(resp.statusCode()).isEqualTo(200);
        JsonNode json = MAPPER.readTree(resp.body());
        assertThat(json.has("error")).isTrue();
        assertThat(json.get("error").get("code").asInt()).isEqualTo(-32602);
    }

    @Test
    @DisplayName("S1: prompts/list returns rawwise_sql_guide prompt")
    void testMcpPromptsList() throws Exception {
        var resp = mcpRequest("prompts/list", null);
        assertThat(resp.statusCode()).isEqualTo(200);
        JsonNode json = MAPPER.readTree(resp.body());
        assertThat(json.has("result")).isTrue();
        JsonNode prompts = json.get("result").get("prompts");
        assertThat(prompts.isArray()).isTrue();
        assertThat(prompts.size()).isGreaterThanOrEqualTo(1);
        boolean hasSqlGuide = false;
        for (JsonNode prompt : prompts) {
            if (prompt.get("name").asText().equals("rawwise_sql_guide")) {
                hasSqlGuide = true;
                break;
            }
        }
        assertThat(hasSqlGuide).isTrue();
    }

    @Test
    @DisplayName("S1: prompts/get rawwise_sql_guide returns SQL guide content")
    void testMcpPromptsGetSqlGuide() throws Exception {
        var resp =
                mcpRequest(
                        "prompts/get", MAPPER.createObjectNode().put("name", "rawwise_sql_guide"));
        assertThat(resp.statusCode()).isEqualTo(200);
        JsonNode json = MAPPER.readTree(resp.body());
        assertThat(json.has("result")).isTrue();
        JsonNode messages = json.get("result").get("messages");
        assertThat(messages.isArray()).isTrue();
        assertThat(messages.size()).isGreaterThanOrEqualTo(1);
        String promptText = messages.get(0).get("content").get("text").asText();
        assertThat(promptText).contains("RawWise SQL Guide");
        assertThat(promptText).contains("USING rawwise");
        assertThat(promptText).contains("FastPath");
    }

    @Test
    @DisplayName("S4: prompts/get with unknown name returns error in result")
    void testMcpPromptsGetUnknown() throws Exception {
        var resp =
                mcpRequest(
                        "prompts/get", MAPPER.createObjectNode().put("name", "nonexistent_prompt"));
        assertThat(resp.statusCode()).isEqualTo(200);
        JsonNode json = MAPPER.readTree(resp.body());
        assertThat(json.has("result")).isTrue();
        JsonNode error = json.get("result").get("error");
        assertThat(error.asText()).contains("Unknown prompt");
    }

    @Test
    @DisplayName("S3: prompts/get without name param returns JSON-RPC error")
    void testMcpPromptsGetMissingName() throws Exception {
        var resp = mcpRequest("prompts/get", MAPPER.createObjectNode().putNull("name"));
        assertThat(resp.statusCode()).isEqualTo(200);
        JsonNode json = MAPPER.readTree(resp.body());
        assertThat(json.has("error")).isTrue();
        assertThat(json.get("error").get("code").asInt()).isEqualTo(-32602);
    }

    private HttpResponse<String> mcpRequest(String method, JsonNode params) throws Exception {
        int id = ++requestId;
        var req =
                MAPPER.createObjectNode()
                        .put("jsonrpc", "2.0")
                        .put("id", String.valueOf(id))
                        .put("method", method);
        if (params != null) {
            req.set("params", params);
        }
        return postJson(req.toString());
    }

    private HttpResponse<String> postJson(String body) throws Exception {
        return HTTP.send(
                HttpRequest.newBuilder()
                        .uri(URI.create(frontendBaseUrl + "/mcp"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .timeout(Duration.ofSeconds(15))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
