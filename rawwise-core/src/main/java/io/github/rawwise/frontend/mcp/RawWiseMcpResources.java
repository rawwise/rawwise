package io.github.rawwise.frontend.mcp;

import io.github.rawwise.axiom.memory.semantic.GraphEngine;
import io.github.rawwise.axiom.memory.semantic.GraphNode;
import io.github.rawwise.frontend.backend.BackendClient;
import io.github.rawwise.proto.axiom.AxiomSearchRequest;
import io.github.rawwise.proto.axiom.AxiomServiceGrpc;
import io.github.rawwise.server.JobState;
import io.github.rawwise.server.QueryType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

@Slf4j
public class RawWiseMcpResources {

    private static final int RESOURCE_POLL_MAX_RETRIES = 10;
    private static final long RESOURCE_POLL_INTERVAL_MS = 500;

    private final BackendClient backendClient;
    private final AxiomServiceGrpc.AxiomServiceBlockingStub axiomStub;
    @Nullable private final GraphEngine graphEngine;

    public RawWiseMcpResources(
            BackendClient backendClient,
            AxiomServiceGrpc.AxiomServiceBlockingStub axiomStub,
            @Nullable GraphEngine graphEngine) {
        this.backendClient = backendClient;
        this.axiomStub = axiomStub;
        this.graphEngine = graphEngine;
    }

    public List<Map<String, Object>> getResourceDefinitions() {
        List<Map<String, Object>> resources = new ArrayList<>();
        resources.add(
                resourceDef(
                        "rawwise://tables", "All tables in RawWise data lake", "application/json"));
        resources.add(
                resourceDef(
                        "rawwise://tables/{table}",
                        "Schema details for a specific table",
                        "application/json"));
        resources.add(
                resourceDef(
                        "rawwise://knowledge/{docId}",
                        "Knowledge document details via semantic search",
                        "application/json"));
        resources.add(
                resourceDef(
                        "rawwise://graph/{graphId}/node/{nodeId}",
                        "Graph node details from the knowledge graph",
                        "application/json"));
        return resources;
    }

    public Map<String, Object> read(String uri) {
        try {
            if (uri.equals("rawwise://tables")) {
                return readTablesList();
            }
            if (uri.startsWith("rawwise://tables/")) {
                String table = uri.substring("rawwise://tables/".length());
                return readTableSchema(table);
            }
            if (uri.startsWith("rawwise://knowledge/")) {
                String docId = uri.substring("rawwise://knowledge/".length());
                return readKnowledge(docId);
            }
            if (uri.startsWith("rawwise://graph/")) {
                String remainder = uri.substring("rawwise://graph/".length());
                int nodeIdx = remainder.indexOf("/node/");
                if (nodeIdx > 0) {
                    String graphId = remainder.substring(0, nodeIdx);
                    String nodeId = remainder.substring(nodeIdx + 6);
                    return readGraphNode(graphId, nodeId);
                }
            }
            return Map.of("error", "Unknown resource URI: " + uri);
        } catch (Exception e) {
            log.warn("Failed to read resource {}: {}", uri, e.getMessage());
            return Map.of("error", e.getMessage(), "uri", uri);
        }
    }

    private Map<String, Object> readTablesList() {
        var submit = backendClient.submitQuery("SHOW TABLES", QueryType.SQL, Map.of());
        if (!submit.success()) {
            return Map.of("error", submit.errorMessage(), "tables", List.of());
        }
        var result = pollForResult(submit.jobId());
        if (result.containsKey("error")) {
            return result;
        }
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        List<Map<String, Object>> tables = new ArrayList<>();
        if (rows != null) {
            for (var row : rows) {
                if (!row.isEmpty()) {
                    tables.add(Map.of("name", String.valueOf(row.get(0))));
                }
            }
        }
        return Map.of("tables", tables, "count", tables.size());
    }

    private Map<String, Object> readTableSchema(String table) {
        var submit = backendClient.submitQuery("DESCRIBE TABLE " + table, QueryType.SQL, Map.of());
        if (!submit.success()) {
            return Map.of("error", submit.errorMessage(), "table", table);
        }
        var result = pollForResult(submit.jobId());
        if (result.containsKey("error")) {
            return result;
        }
        @SuppressWarnings("unchecked")
        List<Map<String, String>> columns = (List<Map<String, String>>) result.get("columns");
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        return Map.of(
                "table", table,
                "columns", columns != null ? columns : List.of(),
                "rows", rows != null ? rows : List.of());
    }

    private Map<String, Object> readKnowledge(String docId) {
        var request =
                AxiomSearchRequest.newBuilder()
                        .setTableId("default.project_docs")
                        .setQuery(docId)
                        .setTopK(1)
                        .setSnapshotId(0)
                        .build();
        var response = axiomStub.search(request);

        if (response.getResultsList().isEmpty()) {
            return Map.of(
                    "docId",
                    docId,
                    "found",
                    false,
                    "message",
                    "No knowledge document found for: " + docId);
        }

        var item = response.getResultsList().get(0);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("docId", docId);
        result.put("found", true);
        result.put("viewId", item.getViewId());
        result.put("score", item.getScore());
        result.put("summary", item.getSummary());
        if (item.hasProvenance()) {
            Map<String, Object> provenance = new LinkedHashMap<>();
            provenance.put("rawKey", item.getProvenance().getRawKey());
            List<Map<String, Object>> chain = new ArrayList<>();
            for (var link : item.getProvenance().getChainList()) {
                chain.add(
                        Map.of(
                                "from", link.getFrom(),
                                "fromLayer", link.getFromLayer(),
                                "to", link.getTo(),
                                "toLayer", link.getToLayer()));
            }
            provenance.put("chain", chain);
            result.put("provenance", provenance);
        }
        return result;
    }

    private Map<String, Object> readGraphNode(String graphId, String nodeId) {
        if (graphEngine == null) {
            return Map.of(
                    "error",
                    "GraphEngine not available (context module not enabled)",
                    "graphId",
                    graphId,
                    "nodeId",
                    nodeId);
        }
        try {
            var nodeOpt = graphEngine.getNode(graphId, nodeId);
            if (nodeOpt.isEmpty()) {
                return Map.of(
                        "found",
                        false,
                        "graphId",
                        graphId,
                        "nodeId",
                        nodeId,
                        "message",
                        "Node not found");
            }
            GraphNode node = nodeOpt.get();
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("found", true);
            result.put("nodeId", node.nodeId());
            result.put("graphId", node.graphId());
            result.put("type", node.type());
            result.put("properties", node.properties());
            result.put("importance", node.importance());
            result.put("createdAt", node.createdAt());
            result.put("updatedAt", node.updatedAt());
            return result;
        } catch (Exception e) {
            return Map.of("error", e.getMessage(), "graphId", graphId, "nodeId", nodeId);
        }
    }

    private Map<String, Object> pollForResult(String jobId) {
        for (int i = 0; i < RESOURCE_POLL_MAX_RETRIES; i++) {
            try {
                Thread.sleep(RESOURCE_POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Map.of("error", "Interrupted while waiting for result", "jobId", jobId);
            }
            var jobState = backendClient.getJobStatus(jobId);
            if (jobState == JobState.SUCCEEDED) {
                var result = backendClient.getQueryResult(jobId);
                List<Map<String, String>> columns = new ArrayList<>();
                if (result.columns() != null) {
                    for (var col : result.columns()) {
                        columns.add(Map.of("name", col.name(), "type", col.type()));
                    }
                }
                List<List<Object>> rows = result.rows() != null ? result.rows() : List.of();
                return Map.of(
                        "columns", columns,
                        "rows", rows,
                        "rowCount", result.rowCount());
            }
            if (jobState == JobState.FAILED || jobState == JobState.CANCELLED) {
                return Map.of("error", "Query failed: " + jobState.name(), "jobId", jobId);
            }
        }
        return Map.of("error", "Query timed out", "jobId", jobId);
    }

    private static Map<String, Object> resourceDef(
            String uri, String description, String mimeType) {
        Map<String, Object> resource = new LinkedHashMap<>();
        resource.put("uri", uri);
        resource.put("description", description);
        resource.put("mimeType", mimeType);
        return resource;
    }
}
