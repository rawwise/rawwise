package io.github.rawwise.axiom.pipeline.graph;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.axiom.pipeline.KnowledgeDataAccessor;
import io.github.rawwise.metastore.KeyValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class GraphEdgeScanner {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private GraphEdgeScanner() {}

    /**
     * Scan all schema: prefixed out-edges of the specified entity. Follows Local Graph Query
     * Engine.read Out Edges() pattern, but only returns edges whose rel Type starts with "schema:".
     */
    public static List<GraphEdge> scanSchemaOutEdges(
            KnowledgeDataAccessor accessor, String entityId) throws IOException {
        byte[] prefix = GraphStoreSchema.outEdgePrefix(entityId);
        List<GraphEdge> edges = new ArrayList<>();
        for (KeyValue kv : accessor.scanWithFallback(prefix)) {
            try {
                var node = MAPPER.readTree(kv.value());
                String relType = node.get("relType").asText();
                if (relType.startsWith("schema:")) {
                    edges.add(
                            new GraphEdge(
                                    node.get("srcId").asText(),
                                    relType,
                                    node.get("dstId").asText(),
                                    node.has("temporal") && !node.get("temporal").isNull()
                                            ? node.get("temporal").asText()
                                            : null,
                                    node.has("confidence")
                                            ? node.get("confidence").asDouble()
                                            : 1.0,
                                    node.has("weight") ? node.get("weight").asDouble() : 1.0));
                }
            } catch (Exception e) {
                log.debug(
                        "Failed to parse edge from key={} ({}): {}",
                        new String(kv.key(), java.nio.charset.StandardCharsets.UTF_8),
                        e.getClass().getSimpleName(),
                        e.getMessage());
            }
        }
        return edges;
    }

    public static List<GraphEdge> scanSchemaInEdges(KnowledgeDataAccessor accessor, String entityId)
            throws IOException {
        byte[] prefix = GraphStoreSchema.inEdgePrefix(entityId);
        List<GraphEdge> edges = new ArrayList<>();
        for (KeyValue kv : accessor.scanWithFallback(prefix)) {
            try {
                var node = MAPPER.readTree(kv.value());
                String relType = node.get("relType").asText();
                if (relType.startsWith("schema:")) {
                    edges.add(
                            new GraphEdge(
                                    node.get("srcId").asText(),
                                    relType,
                                    node.get("dstId").asText(),
                                    node.has("temporal") && !node.get("temporal").isNull()
                                            ? node.get("temporal").asText()
                                            : null,
                                    node.has("confidence")
                                            ? node.get("confidence").asDouble()
                                            : 1.0,
                                    node.has("weight") ? node.get("weight").asDouble() : 1.0));
                }
            } catch (Exception e) {
                log.debug(
                        "Failed to parse in-edge from key={} ({}): {}",
                        new String(kv.key(), java.nio.charset.StandardCharsets.UTF_8),
                        e.getClass().getSimpleName(),
                        e.getMessage());
            }
        }
        return edges;
    }

    /** Remove schema: prefix, return readable relationship name. */
    public static String stripSchemaPrefix(String relType) {
        return relType.startsWith("schema:") ? relType.substring(7) : relType;
    }
}
