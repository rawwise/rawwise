package io.github.rawwise.benchmark.vbench.memory;

import io.github.rawwise.axiom.memory.semantic.ExtractionPolicy;
import io.github.rawwise.axiom.memory.semantic.ExtractionResult;
import io.github.rawwise.axiom.memory.semantic.GraphEdge;
import io.github.rawwise.axiom.memory.semantic.GraphEngine;
import io.github.rawwise.axiom.memory.semantic.GraphException;
import io.github.rawwise.axiom.memory.semantic.GraphNode;
import io.github.rawwise.axiom.memory.semantic.GraphSchema;
import io.github.rawwise.axiom.memory.semantic.PathResult;
import io.github.rawwise.axiom.memory.semantic.TraversalDirection;
import io.github.rawwise.axiom.memory.semantic.VectorSearchHit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Lightweight in-memory GraphEngine for benchmark experiments. Does NOT persist to RocksDB —
 * sufficient for closed-loop benchmark runs.
 */
public final class InMemoryGraphEngine implements GraphEngine {

    private final Map<String, GraphSchema> schemas = new ConcurrentHashMap<>();
    private final Map<String, Map<String, GraphNode>> nodes = new ConcurrentHashMap<>();
    private final Map<String, Map<String, GraphEdge>> edges = new ConcurrentHashMap<>();
    private final Map<String, Map<String, List<GraphEdge>>> outAdj = new ConcurrentHashMap<>();
    private final Map<String, Map<String, List<GraphEdge>>> inAdj = new ConcurrentHashMap<>();

    @Override
    public GraphSchema createGraph(
            String agentId,
            String name,
            String description,
            List<String> nodeTypes,
            List<String> edgeLabels,
            Map<String, String> metadata) {
        String graphId = "graph-" + UUID.randomUUID().toString().substring(0, 8);
        long now = System.currentTimeMillis();
        GraphSchema schema =
                new GraphSchema(
                        graphId,
                        agentId,
                        name,
                        description,
                        nodeTypes,
                        edgeLabels,
                        metadata == null ? Map.of() : Map.copyOf(metadata),
                        0,
                        0,
                        100_000,
                        1_000_000,
                        now,
                        now);
        schemas.put(graphId, schema);
        nodes.put(graphId, new ConcurrentHashMap<>());
        edges.put(graphId, new ConcurrentHashMap<>());
        outAdj.put(graphId, new ConcurrentHashMap<>());
        inAdj.put(graphId, new ConcurrentHashMap<>());
        return schema;
    }

    @Override
    public Optional<GraphSchema> getGraph(String graphId) {
        return Optional.ofNullable(schemas.get(graphId));
    }

    @Override
    public List<GraphSchema> listGraphsByAgent(String agentId) {
        return schemas.values().stream()
                .filter(s -> s.agentId().equals(agentId))
                .collect(Collectors.toList());
    }

    @Override
    public boolean deleteGraph(String graphId) {
        schemas.remove(graphId);
        nodes.remove(graphId);
        edges.remove(graphId);
        outAdj.remove(graphId);
        inAdj.remove(graphId);
        return true;
    }

    @Override
    public GraphNode addNode(String graphId, String type, Map<String, String> properties)
            throws GraphException {
        ensureGraphExists(graphId);
        String nodeId = "node-" + UUID.randomUUID().toString().substring(0, 8);
        long now = System.currentTimeMillis();
        Map<String, String> cleanProps = properties == null ? Map.of() : new HashMap<>(properties);
        GraphNode node =
                new GraphNode(
                        nodeId, graphId, type, cleanProps, 5, 1.0f, null, null, null, 1, now, now);
        nodes.get(graphId).put(nodeId, node);
        outAdj.get(graphId).put(nodeId, new ArrayList<>());
        inAdj.get(graphId).put(nodeId, new ArrayList<>());
        updateSchemaCounts(graphId);
        return node;
    }

    @Override
    public Optional<GraphNode> getNode(String graphId, String nodeId) {
        Map<String, GraphNode> graphNodes = nodes.get(graphId);
        return graphNodes == null ? Optional.empty() : Optional.ofNullable(graphNodes.get(nodeId));
    }

    @Override
    public GraphNode updateNode(String graphId, GraphNode node) throws GraphException {
        ensureGraphExists(graphId);
        GraphNode existing =
                getNode(graphId, node.nodeId())
                        .orElseThrow(
                                () ->
                                        new GraphException(
                                                graphId, "Node not found: " + node.nodeId(), null));
        GraphNode updated =
                new GraphNode(
                        node.nodeId(),
                        graphId,
                        node.type(),
                        node.properties(),
                        node.importance(),
                        node.decayFactor(),
                        existing.embeddingRef(),
                        existing.embeddingModel(),
                        existing.embeddingText(),
                        existing.version() + 1,
                        existing.createdAt(),
                        System.currentTimeMillis());
        nodes.get(graphId).put(node.nodeId(), updated);
        return updated;
    }

    @Override
    public boolean removeNode(String graphId, String nodeId) {
        nodes.get(graphId).remove(nodeId);
        outAdj.get(graphId).remove(nodeId);
        inAdj.get(graphId).remove(nodeId);
        updateSchemaCounts(graphId);
        return true;
    }

    @Override
    public List<GraphNode> queryNodes(
            String graphId, String type, Map<String, String> propertyFilter, int limit) {
        Map<String, GraphNode> graphNodes = nodes.get(graphId);
        if (graphNodes == null) return List.of();
        return graphNodes.values().stream()
                .filter(n -> type == null || type.isEmpty() || type.equals(n.type()))
                .filter(n -> matchesProperties(n, propertyFilter))
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public GraphEdge addEdge(
            String graphId,
            String srcNodeId,
            String label,
            String dstNodeId,
            Map<String, String> properties,
            float weight)
            throws GraphException {
        ensureGraphExists(graphId);
        ensureNodeExists(graphId, srcNodeId);
        ensureNodeExists(graphId, dstNodeId);
        String edgeId = "edge-" + UUID.randomUUID().toString().substring(0, 8);
        long now = System.currentTimeMillis();
        GraphEdge edge =
                new GraphEdge(
                        edgeId,
                        graphId,
                        srcNodeId,
                        label,
                        dstNodeId,
                        properties == null ? Map.of() : Map.copyOf(properties),
                        weight,
                        1,
                        now,
                        now);
        edges.get(graphId).put(edgeId, edge);
        outAdj.get(graphId).get(srcNodeId).add(edge);
        inAdj.get(graphId).get(dstNodeId).add(edge);
        updateSchemaCounts(graphId);
        return edge;
    }

    @Override
    public Optional<GraphEdge> getEdge(String graphId, String edgeId) {
        Map<String, GraphEdge> graphEdges = edges.get(graphId);
        return graphEdges == null ? Optional.empty() : Optional.ofNullable(graphEdges.get(edgeId));
    }

    @Override
    public GraphEdge updateEdge(String graphId, GraphEdge edge) throws GraphException {
        ensureGraphExists(graphId);
        GraphEdge existing =
                getEdge(graphId, edge.edgeId())
                        .orElseThrow(
                                () ->
                                        new GraphException(
                                                graphId, "Edge not found: " + edge.edgeId(), null));
        GraphEdge updated =
                new GraphEdge(
                        edge.edgeId(),
                        graphId,
                        edge.srcNodeId(),
                        edge.label(),
                        edge.dstNodeId(),
                        edge.properties(),
                        edge.weight(),
                        existing.version() + 1,
                        existing.createdAt(),
                        System.currentTimeMillis());
        edges.get(graphId).put(edge.edgeId(), updated);
        return updated;
    }

    @Override
    public boolean removeEdge(String graphId, String edgeId) {
        GraphEdge edge = edges.get(graphId).remove(edgeId);
        if (edge != null) {
            outAdj.get(graphId).get(edge.srcNodeId()).removeIf(e -> e.edgeId().equals(edgeId));
            inAdj.get(graphId).get(edge.dstNodeId()).removeIf(e -> e.edgeId().equals(edgeId));
        }
        updateSchemaCounts(graphId);
        return true;
    }

    @Override
    public List<GraphEdge> getOutEdges(String graphId, String nodeId, String label) {
        List<GraphEdge> all = outAdj.get(graphId) != null ? outAdj.get(graphId).get(nodeId) : null;
        if (all == null) return List.of();
        return all.stream()
                .filter(e -> label == null || label.isEmpty() || label.equals(e.label()))
                .collect(Collectors.toList());
    }

    @Override
    public List<GraphEdge> getInEdges(String graphId, String nodeId, String label) {
        List<GraphEdge> all = inAdj.get(graphId) != null ? inAdj.get(graphId).get(nodeId) : null;
        if (all == null) return List.of();
        return all.stream()
                .filter(e -> label == null || label.isEmpty() || label.equals(e.label()))
                .collect(Collectors.toList());
    }

    @Override
    public List<GraphNode> neighbors(
            String graphId,
            String nodeId,
            String label,
            int maxHops,
            int limit,
            TraversalDirection direction)
            throws GraphException {
        List<GraphNode> result = new ArrayList<>();
        for (GraphEdge edge : getOutEdges(graphId, nodeId, label)) {
            if (result.size() >= limit) break;
            getNode(graphId, edge.dstNodeId()).ifPresent(result::add);
        }
        return result;
    }

    @Override
    public List<PathResult> findPaths(
            String graphId, String srcNodeId, String dstNodeId, int maxHops) {
        return List.of();
    }

    @Override
    public List<VectorSearchHit> vectorSearch(String graphId, String query, int topK) {
        return List.of();
    }

    @Override
    public ExtractionResult extractFromContext(
            String agentId, String graphId, ExtractionPolicy policy) {
        return new ExtractionResult(agentId, graphId, 0, 0, 0, null, 0);
    }

    @Override
    public int reindexGraph(String graphId, String newModelIdentifier) {
        return 0;
    }

    private void ensureGraphExists(String graphId) throws GraphException {
        if (!schemas.containsKey(graphId)) {
            throw new GraphException(graphId, "Graph not found: " + graphId, null);
        }
    }

    private void ensureNodeExists(String graphId, String nodeId) throws GraphException {
        if (getNode(graphId, nodeId).isEmpty()) {
            throw new GraphException(graphId, "Node not found: " + nodeId, null);
        }
    }

    private boolean matchesProperties(GraphNode node, Map<String, String> filter) {
        if (filter == null || filter.isEmpty()) return true;
        for (Map.Entry<String, String> entry : filter.entrySet()) {
            if (!entry.getValue().equals(node.properties().get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    private void updateSchemaCounts(String graphId) {
        GraphSchema old = schemas.get(graphId);
        if (old == null) return;
        int nodeCount = nodes.get(graphId) != null ? nodes.get(graphId).size() : 0;
        int edgeCount = edges.get(graphId) != null ? edges.get(graphId).size() : 0;
        schemas.put(
                graphId,
                new GraphSchema(
                        old.graphId(),
                        old.agentId(),
                        old.name(),
                        old.description(),
                        old.nodeTypes(),
                        old.edgeLabels(),
                        old.metadata(),
                        nodeCount,
                        edgeCount,
                        old.maxNodes(),
                        old.maxEdges(),
                        old.createdAt(),
                        System.currentTimeMillis()));
    }
}
