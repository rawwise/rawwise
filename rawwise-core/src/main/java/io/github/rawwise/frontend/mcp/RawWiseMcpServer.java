package io.github.rawwise.frontend.mcp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.javalin.Javalin;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

@Slf4j
public class RawWiseMcpServer implements AutoCloseable {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String JSONRPC_VERSION = "2.0";
    private static final String PROTOCOL_VERSION = "2024-11-05";
    private static final String SERVER_NAME = "RawWise MCP Server";
    private static final String SERVER_VERSION = "1.0.0";

    private final RawWiseMcpTools tools;
    private final RawWiseMcpConfig config;
    @Nullable private final RawWiseMcpResources resources;
    @Nullable private final RawWiseMcpPrompts prompts;
    private volatile boolean initialized;

    public RawWiseMcpServer(
            RawWiseMcpTools tools,
            RawWiseMcpConfig config,
            @Nullable RawWiseMcpResources resources,
            @Nullable RawWiseMcpPrompts prompts) {
        this.tools = tools;
        this.config = config;
        this.resources = resources;
        this.prompts = prompts;
    }

    public RawWiseMcpServer(RawWiseMcpTools tools, RawWiseMcpConfig config) {
        this(tools, config, null, null);
    }

    public void register(Javalin app) {
        if (!config.enabled()) {
            log.info("MCP server is disabled (rawwise.mcp.enabled=false)");
            return;
        }
        app.post(
                config.endpoint(),
                ctx -> {
                    try {
                        String requestBody = ctx.body();
                        JsonNode requestNode = MAPPER.readTree(requestBody);
                        String response = handleJsonRpc(requestNode);
                        ctx.contentType("application/json");
                        ctx.result(response);
                    } catch (JsonProcessingException e) {
                        ctx.status(400).contentType("application/json");
                        ctx.result(jsonRpcError(null, -32700, "Parse error: " + e.getMessage()));
                    } catch (Exception e) {
                        log.error("MCP request handling failed", e);
                        ctx.status(500).contentType("application/json");
                        ctx.result(jsonRpcError(null, -32603, "Internal error: " + e.getMessage()));
                    }
                });
        log.info(
                "MCP server registered at {} (maxConcurrent={}, resources={}, prompts={})",
                config.endpoint(),
                config.maxConcurrentRequests(),
                config.resourcesEnabled(),
                config.promptsEnabled());
    }

    private String handleJsonRpc(JsonNode request) {
        String requestId = request.has("id") ? request.get("id").asText() : null;
        String method = request.has("method") ? request.get("method").asText() : null;

        if (method == null) {
            return jsonRpcError(requestId, -32600, "Invalid Request: missing method");
        }

        try {
            return switch (method) {
                case "initialize" -> handleInitialize(requestId, request);
                case "tools/list" -> handleToolsList(requestId);
                case "tools/call" -> handleToolsCall(requestId, request);
                case "resources/list" -> handleResourcesList(requestId);
                case "resources/read" -> handleResourcesRead(requestId, request);
                case "prompts/list" -> handlePromptsList(requestId);
                case "prompts/get" -> handlePromptsGet(requestId, request);
                case "notifications/initialized" -> jsonRpcOk(requestId, Map.of());
                default -> jsonRpcError(requestId, -32601, "Method not found: " + method);
            };
        } catch (Exception e) {
            log.error("MCP method '{}' failed", method, e);
            return jsonRpcError(requestId, -32603, "Internal error: " + e.getMessage());
        }
    }

    private String handleInitialize(String requestId, JsonNode request) {
        initialized = true;
        Map<String, Object> serverInfo = new LinkedHashMap<>();
        serverInfo.put("name", SERVER_NAME);
        serverInfo.put("version", SERVER_VERSION);

        Map<String, Object> capabilities = new LinkedHashMap<>();
        capabilities.put("tools", Map.of());
        if (config.resourcesEnabled() && resources != null) {
            capabilities.put("resources", Map.of("list", true, "read", true));
        }
        if (config.promptsEnabled() && prompts != null) {
            capabilities.put("prompts", Map.of("list", true, "get", true));
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("protocolVersion", PROTOCOL_VERSION);
        result.put("serverInfo", serverInfo);
        result.put("capabilities", capabilities);
        return jsonRpcOk(requestId, result);
    }

    private String handleToolsList(String requestId) {
        List<Map<String, Object>> toolDefs = tools.getToolDefinitions();
        return jsonRpcOk(requestId, Map.of("tools", toolDefs));
    }

    private String handleToolsCall(String requestId, JsonNode request) {
        JsonNode params = request.get("params");
        if (params == null || !params.has("name")) {
            return jsonRpcError(requestId, -32602, "Invalid params: missing tool name");
        }

        String toolName = params.get("name").asText();
        JsonNode arguments =
                params.has("arguments") ? params.get("arguments") : MAPPER.createObjectNode();

        Map<String, Object> toolResult = tools.call(toolName, arguments);

        ObjectNode result = MAPPER.createObjectNode();
        result.putArray("content").addObject().put("type", "text").put("text", toJson(toolResult));

        return jsonRpcOk(requestId, result);
    }

    private String handleResourcesList(String requestId) {
        if (!config.resourcesEnabled() || resources == null) {
            return jsonRpcError(requestId, -32601, "Resources not enabled");
        }
        List<Map<String, Object>> resourceDefs = resources.getResourceDefinitions();
        return jsonRpcOk(requestId, Map.of("resources", resourceDefs));
    }

    private String handleResourcesRead(String requestId, JsonNode request) {
        if (!config.resourcesEnabled() || resources == null) {
            return jsonRpcError(requestId, -32601, "Resources not enabled");
        }
        JsonNode params = request.get("params");
        if (params == null || !params.has("uri") || params.get("uri").isNull()) {
            return jsonRpcError(requestId, -32602, "Invalid params: missing uri");
        }
        String uri = params.get("uri").asText();
        Map<String, Object> resourceResult = resources.read(uri);

        ObjectNode result = MAPPER.createObjectNode();
        result.putArray("contents")
                .addObject()
                .put("uri", uri)
                .put("mimeType", "application/json")
                .put("text", toJson(resourceResult));

        return jsonRpcOk(requestId, result);
    }

    private String handlePromptsList(String requestId) {
        if (!config.promptsEnabled() || prompts == null) {
            return jsonRpcError(requestId, -32601, "Prompts not enabled");
        }
        List<Map<String, Object>> promptDefs = prompts.getPromptDefinitions();
        return jsonRpcOk(requestId, Map.of("prompts", promptDefs));
    }

    private String handlePromptsGet(String requestId, JsonNode request) {
        if (!config.promptsEnabled() || prompts == null) {
            return jsonRpcError(requestId, -32601, "Prompts not enabled");
        }
        JsonNode params = request.get("params");
        if (params == null || !params.has("name") || params.get("name").isNull()) {
            return jsonRpcError(requestId, -32602, "Invalid params: missing prompt name");
        }
        String promptName = params.get("name").asText();
        Map<String, Object> promptResult = prompts.get(promptName);
        return jsonRpcOk(requestId, promptResult);
    }

    private String jsonRpcOk(String id, Object result) {
        try {
            ObjectNode response = MAPPER.createObjectNode();
            response.put("jsonrpc", JSONRPC_VERSION);
            if (id != null) {
                response.put("id", id);
            }
            response.set("result", MAPPER.valueToTree(result));
            return MAPPER.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize JSON-RPC response", e);
            return "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32603,\"message\":\"Serialization error\"}}";
        }
    }

    private String jsonRpcError(String id, int code, String message) {
        try {
            ObjectNode response = MAPPER.createObjectNode();
            response.put("jsonrpc", JSONRPC_VERSION);
            if (id != null) {
                response.put("id", id);
            } else {
                response.putNull("id");
            }
            ObjectNode error = response.putObject("error");
            error.put("code", code);
            error.put("message", message);
            return MAPPER.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            return "{\"jsonrpc\":\"2.0\",\"id\":null,\"error\":{\"code\":-32603,\"message\":\"Serialization error\"}}";
        }
    }

    private String toJson(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            return obj != null ? obj.toString() : "null";
        }
    }

    @Override
    public void close() {
        log.info("MCP server closed");
    }
}
