package io.github.rawwise.axiom.memory.semantic;

import io.github.rawwise.axiom.pipeline.state.DefaultStateEngine;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.VectorIndex;
import io.github.rawwise.engine.vector.VectorSearchResult;
import io.github.rawwise.governance.context.ContextEntry;
import io.github.rawwise.governance.context.ContextManager;
import io.github.rawwise.governance.context.KnowledgeContext;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultGraphEngine implements GraphEngine {

    private static final String SCHEMA_PREFIX = "graph::schema::";
    private static final String NODE_PREFIX = "graph::node::";
    private static final String EDGE_PREFIX = "graph::edge::";
    private static final String ADJ_PREFIX = "graph::adj::";
    private static final String NODE_IDX_PREFIX = "graph::idx::node::";
    private static final String EDGE_IDX_PREFIX = "graph::idx::edge::";
    private static final String DELETING_PREFIX = "graph::deleting::";
    private static final String SEP = "::";
    private static final int COUNT_CAS_MAX_RETRIES = 16;

    private final Metastore metastore;
    private final KnowledgeStore knowledgeStore;
    private final ContextManager contextManager;
    private final VectorIndex vectorIndex;
    private final EmbeddingProvider embeddingProvider;
    private final KnowledgeExtractor knowledgeExtractor;
    private final GraphTrustAdapter trustAdapter;
    private final GraphStateAdapter stateAdapter;

    public DefaultGraphEngine(GraphEngineConfig config) {
        this.metastore = config.metastore();
        this.knowledgeStore = config.knowledgeStore();
        this.contextManager = config.contextManager();
        this.vectorIndex = config.vectorIndex();
        this.embeddingProvider = config.embeddingProvider();
        this.knowledgeExtractor = config.knowledgeExtractor();
        this.trustAdapter = config.trustAdapter();
        this.stateAdapter =
                config.stateAdapter() != null
                        ? config.stateAdapter()
                        : new GraphStateAdapter(this, new DefaultStateEngine(metastore), metastore);
    }

    @Override
    public GraphSchema createGraph(
            String agentId,
            String name,
            String description,
            List<String> nodeTypes,
            List<String> edgeLabels,
            Map<String, String> metadata)
            throws GraphException {
        String graphId = "graph-" + UUID.randomUUID();
        long now = System.currentTimeMillis();
        GraphSchema schema =
                new GraphSchema(
                        graphId,
                        agentId,
                        name,
                        description,
                        nodeTypes,
                        edgeLabels,
                        metadata,
                        0,
                        0,
                        100_000,
                        1_000_000,
                        now,
                        now);

        byte[] key = schemaKey(graphId);
        byte[] value = serialize(schema);
        byte[] nodeCountKey = schemaNodeCountKey(graphId);
        byte[] edgeCountKey = schemaEdgeCountKey(graphId);
        byte[] zero = "0".getBytes(StandardCharsets.UTF_8);
        List<Precondition> preconditions =
                List.of(
                        new Precondition.NotExists(key),
                        new Precondition.NotExists(nodeCountKey),
                        new Precondition.NotExists(edgeCountKey));
        List<Mutation> mutations =
                List.of(
                        new Mutation.Put(key, value),
                        new Mutation.Put(nodeCountKey, zero),
                        new Mutation.Put(edgeCountKey, zero));

        boolean success = cas(graphId, preconditions, mutations);
        if (!success) {
            throw new GraphCasConflictException(graphId, "Graph already exists: " + graphId);
        }
        log.info("Created graph graphId={} agentId={} name={}", graphId, agentId, name);
        return schema;
    }

    @Override
    public Optional<GraphSchema> getGraph(String graphId) throws GraphException {
        byte[] key = schemaKey(graphId);
        try {
            Optional<byte[]> value = metastore.get(key);
            if (value.isEmpty()) return Optional.empty();
            GraphSchema base = deserialize(value.get(), GraphSchema.class);
            int nodeCount = readCount(schemaNodeCountKey(graphId));
            int edgeCount = readCount(schemaEdgeCountKey(graphId));
            return Optional.of(
                    new GraphSchema(
                            base.graphId(),
                            base.agentId(),
                            base.name(),
                            base.description(),
                            base.nodeTypes(),
                            base.edgeLabels(),
                            base.metadata(),
                            nodeCount,
                            edgeCount,
                            base.maxNodes(),
                            base.maxEdges(),
                            base.createdAt(),
                            base.updatedAt()));
        } catch (IOException e) {
            throw new GraphException(graphId, "Failed to read graph schema", e);
        }
    }

    @Override
    public List<GraphSchema> listGraphsByAgent(String agentId) throws GraphException {
        byte[] prefix = SCHEMA_PREFIX.getBytes(StandardCharsets.UTF_8);
        List<KeyValue> entries = scanAll(agentId, prefix);
        return entries.stream()
                .filter(
                        kv -> {
                            String keyStr = new String(kv.key(), StandardCharsets.UTF_8);
                            // Skip counter keys (graph::schema::<id>::node Count / e.g. Count);
                            // they share prefix
                            // with the schema JSON itself but contain plain integer text.
                            return !keyStr.endsWith(SEP + "nodeCount")
                                    && !keyStr.endsWith(SEP + "edgeCount");
                        })
                .map(kv -> deserialize(kv.value(), GraphSchema.class))
                .filter(s -> s.agentId().equals(agentId))
                .collect(Collectors.toList());
    }

    @Override
    public boolean deleteGraph(String graphId) throws GraphException {
        GraphSchema schema =
                getGraph(graphId).orElseThrow(() -> new GraphNotFoundException(graphId));

        byte[] deletingKey = deletingKey(graphId);
        try {
            metastore.put(deletingKey, "1".getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new GraphException(graphId, "Failed to mark graph as deleting", e);
        }

        List<Mutation> knowledgeMutations = new ArrayList<>();
        List<Mutation> metaMutations = new ArrayList<>();
        metaMutations.add(new Mutation.Delete(schemaKey(graphId)));
        metaMutations.add(new Mutation.Delete(schemaNodeCountKey(graphId)));
        metaMutations.add(new Mutation.Delete(schemaEdgeCountKey(graphId)));
        metaMutations.add(new Mutation.Delete(deletingKey));

        byte[] nodePrefix = (NODE_PREFIX + graphId + SEP).getBytes(StandardCharsets.UTF_8);
        List<KeyValue> nodeEntries = scanAll(graphId, nodePrefix);
        for (KeyValue kv : nodeEntries) {
            GraphNode node = deserialize(kv.value(), GraphNode.class);
            knowledgeMutations.add(new Mutation.Delete(kv.key()));
            knowledgeMutations.add(new Mutation.Delete(outAdjKey(graphId, node.nodeId(), "")));
            knowledgeMutations.add(new Mutation.Delete(inAdjKey(graphId, node.nodeId(), "")));
            metaMutations.add(new Mutation.Delete(nodeIdxKey(graphId, node.type(), node.nodeId())));
            if (node.embeddingRef() != null && !node.embeddingRef().isEmpty()) {
                vectorIndex.delete(graphId, node.embeddingRef());
            }
        }

        byte[] edgePrefix = (EDGE_PREFIX + graphId + SEP).getBytes(StandardCharsets.UTF_8);
        List<KeyValue> edgeEntries = scanAll(graphId, edgePrefix);
        for (KeyValue kv : edgeEntries) {
            knowledgeMutations.add(new Mutation.Delete(kv.key()));
        }

        byte[] adjPrefix = (ADJ_PREFIX + graphId + SEP).getBytes(StandardCharsets.UTF_8);
        List<KeyValue> adjEntries = scanAll(graphId, adjPrefix);
        for (KeyValue kv : adjEntries) {
            knowledgeMutations.add(new Mutation.Delete(kv.key()));
        }

        byte[] nodeIdxPrefix = (NODE_IDX_PREFIX + graphId + SEP).getBytes(StandardCharsets.UTF_8);
        List<KeyValue> nodeIdxEntries = scanAll(graphId, nodeIdxPrefix);
        for (KeyValue kv : nodeIdxEntries) {
            metaMutations.add(new Mutation.Delete(kv.key()));
        }

        byte[] edgeIdxPrefix = (EDGE_IDX_PREFIX + graphId + SEP).getBytes(StandardCharsets.UTF_8);
        List<KeyValue> edgeIdxEntries = scanAll(graphId, edgeIdxPrefix);
        for (KeyValue kv : edgeIdxEntries) {
            metaMutations.add(new Mutation.Delete(kv.key()));
        }

        try {
            if (!knowledgeMutations.isEmpty()) {
                knowledgeStore.batchWrite(knowledgeMutations);
            }
            if (!metaMutations.isEmpty()) {
                metastore.batchWrite(metaMutations);
            }
        } catch (IOException e) {
            throw new GraphException(graphId, "Failed to delete graph", e);
        }
        log.info(
                "Deleted graph graphId={} nodes={} edges={}",
                graphId,
                nodeEntries.size(),
                edgeEntries.size());
        return true;
    }

    @Override
    public GraphNode addNode(String graphId, String type, Map<String, String> properties)
            throws GraphException {
        ensureGraphExists(graphId);

        GraphSchema schema =
                getGraph(graphId).orElseThrow(() -> new GraphNotFoundException(graphId));
        if (schema.nodeCount() >= schema.maxNodes()) {
            throw new GraphCapacityExceededException(
                    graphId, "nodes", schema.nodeCount(), schema.maxNodes());
        }

        String nodeId = "node-" + UUID.randomUUID();
        long now = System.currentTimeMillis();

        String embeddingRef = null;
        String embeddingModel = null;
        String embeddingText = properties.get("embeddingText");
        if (embeddingText != null && !embeddingText.isEmpty()) {
            float[] embedding = embeddingProvider.embed(embeddingText);
            embeddingRef = nodeId;
            embeddingModel = embeddingProvider.modelIdentifier();
            vectorIndex.indexWithEmbedding(graphId, embeddingRef, embedding);
        }

        Map<String, String> cleanProps = new LinkedHashMap<>(properties);
        cleanProps.remove("embeddingText");

        int importance = parseImportance(cleanProps);
        float decayFactor = parseDecayFactor(cleanProps);

        GraphNode node =
                new GraphNode(
                        nodeId,
                        graphId,
                        type,
                        cleanProps,
                        importance,
                        decayFactor,
                        embeddingRef,
                        embeddingModel,
                        embeddingText,
                        1,
                        now,
                        now);

        byte[] key = nodeKey(graphId, nodeId);
        byte[] value = serialize(node);
        byte[] countKey = schemaNodeCountKey(graphId);

        boolean success = false;
        for (int attempt = 0; attempt < COUNT_CAS_MAX_RETRIES && !success; attempt++) {
            int currentCount = readCount(countKey);
            if (currentCount >= schema.maxNodes()) {
                throw new GraphCapacityExceededException(
                        graphId, "nodes", currentCount, schema.maxNodes());
            }
            byte[] currentCountBytes =
                    Integer.toString(currentCount).getBytes(StandardCharsets.UTF_8);
            byte[] newCountBytes =
                    Integer.toString(currentCount + 1).getBytes(StandardCharsets.UTF_8);
            List<Precondition> preconditions =
                    List.of(
                            new Precondition.NotExists(key),
                            new Precondition.ValueEquals(countKey, currentCountBytes));
            List<Mutation> mutations =
                    List.of(
                            new Mutation.Put(key, value),
                            new Mutation.Put(countKey, newCountBytes),
                            new Mutation.Put(
                                    nodeIdxKey(graphId, type, nodeId),
                                    nodeId.getBytes(StandardCharsets.UTF_8)));
            success = cas(graphId, preconditions, mutations);
        }
        if (!success) {
            if (embeddingRef != null) {
                try {
                    vectorIndex.delete(graphId, embeddingRef);
                } catch (Exception e) {
                    log.warn(
                            "Vector index rollback failed for node graphId={} nodeId={}",
                            graphId,
                            nodeId,
                            e);
                }
            }
            throw new GraphCasConflictException(
                    graphId, "Node counter CAS exhausted retries: " + nodeId);
        }

        if (trustAdapter != null) {
            try {
                trustAdapter.recordNodeChange(graphId, nodeId, 1, node);
            } catch (Exception e) {
                log.warn(
                        "Trust recording failed for node graphId={} nodeId={}", graphId, nodeId, e);
            }
        }

        log.debug("Added node graphId={} nodeId={} type={}", graphId, nodeId, type);
        return node;
    }

    @Override
    public Optional<GraphNode> getNode(String graphId, String nodeId) throws GraphException {
        byte[] key = nodeKey(graphId, nodeId);
        try {
            Optional<byte[]> value = knowledgeStore.get(key);
            return value.map(v -> deserialize(v, GraphNode.class));
        } catch (IOException e) {
            throw new GraphException(graphId, "Failed to read node: " + nodeId, e);
        }
    }

    @Override
    public GraphNode updateNode(String graphId, GraphNode node) throws GraphException {
        ensureGraphExists(graphId);

        GraphNode existing =
                getNode(graphId, node.nodeId())
                        .orElseThrow(() -> new GraphNodeNotFoundException(graphId, node.nodeId()));

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

        byte[] key = nodeKey(graphId, node.nodeId());
        byte[] oldValue = serialize(existing);
        byte[] newValue = serialize(updated);
        List<Precondition> preconditions = List.of(new Precondition.ValueEquals(key, oldValue));
        List<Mutation> mutations = List.of(new Mutation.Put(key, newValue));

        boolean success = cas(graphId, preconditions, mutations);
        if (!success) {
            throw new GraphCasConflictException(graphId, "Node version conflict: " + node.nodeId());
        }

        if (trustAdapter != null) {
            try {
                trustAdapter.recordNodeChange(graphId, node.nodeId(), updated.version(), updated);
            } catch (Exception e) {
                log.warn(
                        "Trust recording failed for node graphId={} nodeId={}",
                        graphId,
                        node.nodeId(),
                        e);
            }
        }

        log.debug(
                "Updated node graphId={} nodeId={} version={}",
                graphId,
                node.nodeId(),
                updated.version());
        return updated;
    }

    @Override
    public boolean removeNode(String graphId, String nodeId) throws GraphException {
        ensureGraphExists(graphId);

        GraphNode node =
                getNode(graphId, nodeId)
                        .orElseThrow(() -> new GraphNodeNotFoundException(graphId, nodeId));

        byte[] deletingKey = deletingNodeKey(graphId, nodeId);
        try {
            metastore.put(deletingKey, "1".getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new GraphException(graphId, "Failed to mark node as deleting: " + nodeId, e);
        }

        List<Mutation> knowledgeMutations = new ArrayList<>();
        List<Mutation> metaMutations = new ArrayList<>();
        List<GraphEdge> outEdges = getOutEdges(graphId, nodeId, null);
        List<GraphEdge> inEdges = getInEdges(graphId, nodeId, null);

        for (GraphEdge edge : outEdges) {
            knowledgeMutations.add(new Mutation.Delete(edgeKey(graphId, edge.edgeId())));
            knowledgeMutations.add(
                    new Mutation.Delete(outAdjKey(graphId, edge.srcNodeId(), edge.edgeId())));
            knowledgeMutations.add(
                    new Mutation.Delete(inAdjKey(graphId, edge.dstNodeId(), edge.edgeId())));
            metaMutations.add(
                    new Mutation.Delete(edgeIdxKey(graphId, edge.label(), edge.edgeId())));
        }
        for (GraphEdge edge : inEdges) {
            if (!edge.srcNodeId().equals(nodeId)) {
                knowledgeMutations.add(new Mutation.Delete(edgeKey(graphId, edge.edgeId())));
                knowledgeMutations.add(
                        new Mutation.Delete(outAdjKey(graphId, edge.srcNodeId(), edge.edgeId())));
                knowledgeMutations.add(
                        new Mutation.Delete(inAdjKey(graphId, edge.dstNodeId(), edge.edgeId())));
                metaMutations.add(
                        new Mutation.Delete(edgeIdxKey(graphId, edge.label(), edge.edgeId())));
            }
        }

        knowledgeMutations.add(new Mutation.Delete(nodeKey(graphId, nodeId)));
        metaMutations.add(new Mutation.Delete(deletingKey));
        metaMutations.add(new Mutation.Delete(nodeIdxKey(graphId, node.type(), nodeId)));

        try {
            if (!knowledgeMutations.isEmpty()) {
                knowledgeStore.batchWrite(knowledgeMutations);
            }
            if (!metaMutations.isEmpty()) {
                metastore.batchWrite(metaMutations);
            }
        } catch (IOException e) {
            throw new GraphException(graphId, "Failed to remove node: " + nodeId, e);
        }

        if (node.embeddingRef() != null && !node.embeddingRef().isEmpty()) {
            try {
                vectorIndex.delete(graphId, node.embeddingRef());
            } catch (Exception e) {
                log.warn(
                        "Vector index delete failed for node graphId={} nodeId={}",
                        graphId,
                        nodeId,
                        e);
            }
        }

        log.info(
                "Removed node graphId={} nodeId={} cascadeEdges={}",
                graphId,
                nodeId,
                outEdges.size() + inEdges.size());
        return true;
    }

    @Override
    public List<GraphNode> queryNodes(
            String graphId, String type, Map<String, String> propertyFilter, int limit)
            throws GraphException {
        if (type != null && !type.isEmpty()) {
            byte[] idxPrefix = nodeIdxKey(graphId, type, "");
            List<KeyValue> idxEntries = scanAll(graphId, idxPrefix);
            List<GraphNode> results = new ArrayList<>();
            for (KeyValue kv : idxEntries) {
                if (results.size() >= limit) break;
                String nodeId = new String(kv.value(), StandardCharsets.UTF_8);
                Optional<GraphNode> node = getNode(graphId, nodeId);
                if (node.isPresent() && matchesProperties(node.get(), propertyFilter)) {
                    results.add(node.get());
                }
            }
            return results;
        }

        byte[] nodePrefix = (NODE_PREFIX + graphId + SEP).getBytes(StandardCharsets.UTF_8);
        List<KeyValue> entries = scanAll(graphId, nodePrefix);
        List<GraphNode> results = new ArrayList<>();
        for (KeyValue kv : entries) {
            if (results.size() >= limit) break;
            GraphNode node = deserialize(kv.value(), GraphNode.class);
            if (matchesProperties(node, propertyFilter)) {
                results.add(node);
            }
        }
        return results;
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

        GraphSchema schema =
                getGraph(graphId).orElseThrow(() -> new GraphNotFoundException(graphId));
        if (schema.edgeCount() >= schema.maxEdges()) {
            throw new GraphCapacityExceededException(
                    graphId, "edges", schema.edgeCount(), schema.maxEdges());
        }

        String edgeId = "edge-" + UUID.randomUUID();
        long now = System.currentTimeMillis();

        GraphEdge edge =
                new GraphEdge(
                        edgeId,
                        graphId,
                        srcNodeId,
                        label,
                        dstNodeId,
                        properties,
                        weight,
                        1,
                        now,
                        now);

        byte[] edgeKey = edgeKey(graphId, edgeId);
        byte[] edgeValue = serialize(edge);
        byte[] countKey = schemaEdgeCountKey(graphId);

        boolean success = false;
        for (int attempt = 0; attempt < COUNT_CAS_MAX_RETRIES && !success; attempt++) {
            int currentCount = readCount(countKey);
            if (currentCount >= schema.maxEdges()) {
                throw new GraphCapacityExceededException(
                        graphId, "edges", currentCount, schema.maxEdges());
            }
            byte[] currentCountBytes =
                    Integer.toString(currentCount).getBytes(StandardCharsets.UTF_8);
            byte[] newCountBytes =
                    Integer.toString(currentCount + 1).getBytes(StandardCharsets.UTF_8);
            List<Precondition> preconditions =
                    List.of(
                            new Precondition.NotExists(edgeKey),
                            new Precondition.ValueEquals(countKey, currentCountBytes));
            List<Mutation> mutations =
                    List.of(
                            new Mutation.Put(edgeKey, edgeValue),
                            new Mutation.Put(countKey, newCountBytes),
                            new Mutation.Put(outAdjKey(graphId, srcNodeId, edgeId), edgeValue),
                            new Mutation.Put(inAdjKey(graphId, dstNodeId, edgeId), edgeValue),
                            new Mutation.Put(
                                    edgeIdxKey(graphId, label, edgeId),
                                    nodeIdBytes(srcNodeId, dstNodeId)));
            success = cas(graphId, preconditions, mutations);
        }
        if (!success) {
            throw new GraphCasConflictException(
                    graphId, "Edge counter CAS exhausted retries: " + edgeId);
        }

        if (trustAdapter != null) {
            try {
                trustAdapter.recordEdgeChange(graphId, edgeId, 1, edge);
            } catch (Exception e) {
                log.warn(
                        "Trust recording failed for edge graphId={} edgeId={}", graphId, edgeId, e);
            }
        }

        log.debug(
                "Added edge graphId={} edgeId={} src={} label={} dst={}",
                graphId,
                edgeId,
                srcNodeId,
                label,
                dstNodeId);
        return edge;
    }

    @Override
    public Optional<GraphEdge> getEdge(String graphId, String edgeId) throws GraphException {
        byte[] key = edgeKey(graphId, edgeId);
        try {
            Optional<byte[]> value = knowledgeStore.get(key);
            return value.map(v -> deserialize(v, GraphEdge.class));
        } catch (IOException e) {
            throw new GraphException(graphId, "Failed to read edge: " + edgeId, e);
        }
    }

    @Override
    public GraphEdge updateEdge(String graphId, GraphEdge edge) throws GraphException {
        ensureGraphExists(graphId);

        GraphEdge existing =
                getEdge(graphId, edge.edgeId())
                        .orElseThrow(() -> new GraphEdgeNotFoundException(graphId, edge.edgeId()));

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

        byte[] key = edgeKey(graphId, edge.edgeId());
        byte[] oldValue = serialize(existing);
        byte[] newValue = serialize(updated);
        List<Precondition> preconditions = List.of(new Precondition.ValueEquals(key, oldValue));
        List<Mutation> mutations =
                List.of(
                        new Mutation.Put(key, newValue),
                        new Mutation.Put(
                                outAdjKey(graphId, updated.srcNodeId(), updated.edgeId()),
                                newValue),
                        new Mutation.Put(
                                inAdjKey(graphId, updated.dstNodeId(), updated.edgeId()),
                                newValue));

        boolean success = cas(graphId, preconditions, mutations);
        if (!success) {
            throw new GraphCasConflictException(graphId, "Edge version conflict: " + edge.edgeId());
        }

        if (trustAdapter != null) {
            try {
                trustAdapter.recordEdgeChange(graphId, edge.edgeId(), updated.version(), updated);
            } catch (Exception e) {
                log.warn(
                        "Trust recording failed for edge graphId={} edgeId={}",
                        graphId,
                        edge.edgeId(),
                        e);
            }
        }

        log.debug(
                "Updated edge graphId={} edgeId={} version={}",
                graphId,
                edge.edgeId(),
                updated.version());
        return updated;
    }

    @Override
    public boolean removeEdge(String graphId, String edgeId) throws GraphException {
        GraphEdge edge =
                getEdge(graphId, edgeId)
                        .orElseThrow(() -> new GraphEdgeNotFoundException(graphId, edgeId));

        List<Mutation> knowledgeMutations =
                List.of(
                        new Mutation.Delete(edgeKey(graphId, edgeId)),
                        new Mutation.Delete(outAdjKey(graphId, edge.srcNodeId(), edgeId)),
                        new Mutation.Delete(inAdjKey(graphId, edge.dstNodeId(), edgeId)));
        List<Mutation> metaMutations =
                List.of(new Mutation.Delete(edgeIdxKey(graphId, edge.label(), edgeId)));

        try {
            knowledgeStore.batchWrite(knowledgeMutations);
            metastore.batchWrite(metaMutations);
        } catch (IOException e) {
            throw new GraphException(graphId, "Failed to remove edge: " + edgeId, e);
        }

        log.debug("Removed edge graphId={} edgeId={}", graphId, edgeId);
        return true;
    }

    @Override
    public List<GraphEdge> getOutEdges(String graphId, String nodeId, String label)
            throws GraphException {
        byte[] prefix = outAdjKey(graphId, nodeId, "");
        List<KeyValue> entries = scanAll(graphId, prefix);
        List<GraphEdge> edges = new ArrayList<>();
        for (KeyValue kv : entries) {
            GraphEdge edge = deserialize(kv.value(), GraphEdge.class);
            if (label == null || label.isEmpty() || label.equals(edge.label())) {
                edges.add(edge);
            }
        }
        return edges;
    }

    @Override
    public List<GraphEdge> getInEdges(String graphId, String nodeId, String label)
            throws GraphException {
        byte[] prefix = inAdjKey(graphId, nodeId, "");
        List<KeyValue> entries = scanAll(graphId, prefix);
        List<GraphEdge> edges = new ArrayList<>();
        for (KeyValue kv : entries) {
            GraphEdge edge = deserialize(kv.value(), GraphEdge.class);
            if (label == null || label.isEmpty() || label.equals(edge.label())) {
                edges.add(edge);
            }
        }
        return edges;
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
        Set<String> visited = new HashSet<>();
        visited.add(nodeId);
        List<GraphNode> result = new ArrayList<>();
        Set<String> frontier = new HashSet<>();
        frontier.add(nodeId);

        for (int hop = 0; hop < maxHops && !frontier.isEmpty() && result.size() < limit; hop++) {
            Set<String> nextFrontier = new HashSet<>();
            for (String currentId : frontier) {
                List<GraphEdge> edges = collectEdges(graphId, currentId, direction, label);
                for (GraphEdge edge : edges) {
                    String neighborId = resolveNeighbor(edge, currentId);
                    if (neighborId != null && visited.add(neighborId)) {
                        Optional<GraphNode> neighbor = getNode(graphId, neighborId);
                        if (neighbor.isPresent() && result.size() < limit) {
                            result.add(neighbor.get());
                            nextFrontier.add(neighborId);
                        }
                    }
                }
            }
            frontier = nextFrontier;
        }
        return result;
    }

    @Override
    public List<PathResult> findPaths(
            String graphId, String srcNodeId, String dstNodeId, int maxHops) throws GraphException {
        Map<String, String> srcParent = new HashMap<>();
        Map<String, String> srcEdge = new HashMap<>();
        Map<String, String> srcEdgeLabel = new HashMap<>();
        Map<String, Float> srcEdgeWeight = new HashMap<>();
        Map<String, String> dstParent = new HashMap<>();
        Map<String, String> dstEdge = new HashMap<>();
        Map<String, String> dstEdgeLabel = new HashMap<>();
        Map<String, Float> dstEdgeWeight = new HashMap<>();

        Set<String> srcFrontier = new HashSet<>();
        srcFrontier.add(srcNodeId);
        srcParent.put(srcNodeId, null);
        Set<String> dstFrontier = new HashSet<>();
        dstFrontier.add(dstNodeId);
        dstParent.put(dstNodeId, null);

        List<PathResult> paths = new ArrayList<>();

        for (int hop = 0; hop < maxHops; hop++) {
            Set<String> nextSrcFrontier = new HashSet<>();
            for (String currentId : srcFrontier) {
                List<GraphEdge> outEdges = getOutEdges(graphId, currentId, null);
                for (GraphEdge edge : outEdges) {
                    if (!srcParent.containsKey(edge.dstNodeId())) {
                        srcParent.put(edge.dstNodeId(), currentId);
                        srcEdge.put(edge.dstNodeId(), edge.edgeId());
                        srcEdgeLabel.put(edge.dstNodeId(), edge.label());
                        srcEdgeWeight.put(edge.dstNodeId(), edge.weight());
                        nextSrcFrontier.add(edge.dstNodeId());
                    }
                }
            }
            srcFrontier = nextSrcFrontier;

            Set<String> meetingPoints = new HashSet<>(srcParent.keySet());
            meetingPoints.retainAll(dstParent.keySet());
            if (!meetingPoints.isEmpty()) {
                for (String meeting : meetingPoints) {
                    paths.add(
                            buildPath(
                                    graphId,
                                    srcNodeId,
                                    dstNodeId,
                                    meeting,
                                    srcParent,
                                    srcEdge,
                                    srcEdgeLabel,
                                    srcEdgeWeight,
                                    dstParent,
                                    dstEdge,
                                    dstEdgeLabel,
                                    dstEdgeWeight));
                }
                return paths;
            }

            Set<String> nextDstFrontier = new HashSet<>();
            for (String currentId : dstFrontier) {
                List<GraphEdge> inEdges = getInEdges(graphId, currentId, null);
                for (GraphEdge edge : inEdges) {
                    if (!dstParent.containsKey(edge.srcNodeId())) {
                        dstParent.put(edge.srcNodeId(), currentId);
                        dstEdge.put(edge.srcNodeId(), edge.edgeId());
                        dstEdgeLabel.put(edge.srcNodeId(), edge.label());
                        dstEdgeWeight.put(edge.srcNodeId(), edge.weight());
                        nextDstFrontier.add(edge.srcNodeId());
                    }
                }
            }
            dstFrontier = nextDstFrontier;

            meetingPoints = new HashSet<>(srcParent.keySet());
            meetingPoints.retainAll(dstParent.keySet());
            if (!meetingPoints.isEmpty()) {
                for (String meeting : meetingPoints) {
                    paths.add(
                            buildPath(
                                    graphId,
                                    srcNodeId,
                                    dstNodeId,
                                    meeting,
                                    srcParent,
                                    srcEdge,
                                    srcEdgeLabel,
                                    srcEdgeWeight,
                                    dstParent,
                                    dstEdge,
                                    dstEdgeLabel,
                                    dstEdgeWeight));
                }
                return paths;
            }
        }
        return paths;
    }

    @Override
    public List<VectorSearchHit> vectorSearch(String graphId, String query, int topK)
            throws GraphException {
        getGraph(graphId).orElseThrow(() -> new GraphNotFoundException(graphId));

        float[] queryEmbedding = embeddingProvider.embed(query);
        List<VectorSearchResult> results = vectorIndex.search(graphId, queryEmbedding, topK * 2);

        List<VectorSearchHit> hits = new ArrayList<>();
        for (VectorSearchResult vr : results) {
            Optional<GraphNode> nodeOpt = getNode(graphId, vr.entryKey());
            if (nodeOpt.isPresent()) {
                hits.add(new VectorSearchHit(nodeOpt.get(), vr.score()));
            }
            if (hits.size() >= topK) break;
        }
        return hits;
    }

    @Override
    public ExtractionResult extractFromContext(
            String agentId, String graphId, ExtractionPolicy policy) throws GraphException {
        ensureGraphExists(graphId);
        long start = System.currentTimeMillis();

        List<ContextEntry> allEntries = new ArrayList<>();
        try {
            List<KnowledgeContext> knowledgeContexts = contextManager.listKnowledgeByAgent(agentId);
            for (KnowledgeContext kc : knowledgeContexts) {
                List<ContextEntry> entries = contextManager.listKnowledgeEntries(kc.knowledgeId());
                allEntries.addAll(entries);
            }
        } catch (IOException e) {
            throw new GraphException(graphId, "Failed to list knowledge entries", e);
        }

        if (allEntries.isEmpty()) {
            return new ExtractionResult(
                    agentId, graphId, 0, 0, 0, null, System.currentTimeMillis() - start);
        }

        return knowledgeExtractor.extract(agentId, graphId, allEntries, policy);
    }

    @Override
    public int reindexGraph(String graphId, String newModelIdentifier) throws GraphException {
        GraphSchema schema =
                getGraph(graphId).orElseThrow(() -> new GraphNotFoundException(graphId));

        byte[] nodePrefix = (NODE_PREFIX + graphId + SEP).getBytes(StandardCharsets.UTF_8);
        List<KeyValue> entries = scanAll(graphId, nodePrefix);

        int reindexed = 0;
        List<String> texts = new ArrayList<>();
        List<GraphNode> nodes = new ArrayList<>();

        for (KeyValue kv : entries) {
            GraphNode node = deserialize(kv.value(), GraphNode.class);
            if (node.embeddingRef() != null && !node.embeddingRef().isEmpty()) {
                String embText = node.embeddingText();
                if (embText == null || embText.isEmpty()) {
                    embText = node.properties().get("embeddingText");
                }
                if (embText != null && !embText.isEmpty()) {
                    texts.add(embText);
                    nodes.add(node);
                }
            }
        }

        if (!texts.isEmpty()) {
            float[][] embeddings = embeddingProvider.embedBatch(texts);
            for (int i = 0; i < nodes.size(); i++) {
                GraphNode node = nodes.get(i);
                vectorIndex.indexWithEmbedding(graphId, node.embeddingRef(), embeddings[i]);

                GraphNode updated =
                        new GraphNode(
                                node.nodeId(),
                                node.graphId(),
                                node.type(),
                                node.properties(),
                                node.importance(),
                                node.decayFactor(),
                                node.embeddingRef(),
                                newModelIdentifier,
                                node.embeddingText(),
                                node.version() + 1,
                                node.createdAt(),
                                System.currentTimeMillis());

                byte[] key = nodeKey(graphId, node.nodeId());
                byte[] oldValue = serialize(node);
                byte[] newValue = serialize(updated);
                List<Precondition> preconditions =
                        List.of(new Precondition.ValueEquals(key, oldValue));
                List<Mutation> mutations = List.of(new Mutation.Put(key, newValue));

                try {
                    boolean success = knowledgeStore.compareAndSwap(preconditions, mutations);
                    if (success) {
                        reindexed++;
                    } else {
                        log.debug(
                                "CAS conflict during reindex for node={} version={}",
                                node.nodeId(),
                                node.version());
                    }
                } catch (IOException e) {
                    log.warn("Failed to reindex node={} in graph={}", node.nodeId(), graphId, e);
                }
            }
        }

        log.info("Reindexed graph graphId={} nodes={}/{}", graphId, reindexed, nodes.size());
        return reindexed;
    }

    private byte[] schemaKey(String graphId) {
        return (SCHEMA_PREFIX + graphId).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] nodeKey(String graphId, String nodeId) {
        return (NODE_PREFIX + graphId + SEP + nodeId).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] edgeKey(String graphId, String edgeId) {
        return (EDGE_PREFIX + graphId + SEP + edgeId).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] outAdjKey(String graphId, String nodeId, String edgeId) {
        return (ADJ_PREFIX + graphId + SEP + "out" + SEP + nodeId + SEP + edgeId)
                .getBytes(StandardCharsets.UTF_8);
    }

    private byte[] inAdjKey(String graphId, String nodeId, String edgeId) {
        return (ADJ_PREFIX + graphId + SEP + "in" + SEP + nodeId + SEP + edgeId)
                .getBytes(StandardCharsets.UTF_8);
    }

    private byte[] nodeIdxKey(String graphId, String type, String id) {
        return (NODE_IDX_PREFIX + graphId + SEP + type + SEP + id).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] edgeIdxKey(String graphId, String label, String id) {
        return (EDGE_IDX_PREFIX + graphId + SEP + label + SEP + id)
                .getBytes(StandardCharsets.UTF_8);
    }

    private byte[] deletingKey(String graphId) {
        return (DELETING_PREFIX + graphId).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] deletingNodeKey(String graphId, String nodeId) {
        return (DELETING_PREFIX + graphId + SEP + nodeId).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] schemaNodeCountKey(String graphId) {
        return (SCHEMA_PREFIX + graphId + SEP + "nodeCount").getBytes(StandardCharsets.UTF_8);
    }

    private byte[] schemaEdgeCountKey(String graphId) {
        return (SCHEMA_PREFIX + graphId + SEP + "edgeCount").getBytes(StandardCharsets.UTF_8);
    }

    private byte[] nodeIdBytes(String srcNodeId, String dstNodeId) {
        return (srcNodeId + SEP + dstNodeId).getBytes(StandardCharsets.UTF_8);
    }

    private int readCount(byte[] countKey) throws GraphException {
        try {
            Optional<byte[]> raw = metastore.get(countKey);
            if (raw.isEmpty()) return 0;
            try {
                return Integer.parseInt(new String(raw.get(), StandardCharsets.UTF_8));
            } catch (NumberFormatException e) {
                return 0;
            }
        } catch (IOException e) {
            throw new GraphException(null, "Failed to read counter", e);
        }
    }

    private byte[] serialize(Object obj) {
        try {
            return io.github.rawwise.governance.context.ContextJson.writeValueAsBytes(obj);
        } catch (IOException e) {
            throw new GraphSerializationException(null, "Serialization failed", e);
        }
    }

    private <T> T deserialize(byte[] data, Class<T> type) {
        try {
            return io.github.rawwise.governance.context.ContextJson.readValue(data, type);
        } catch (IOException e) {
            throw new GraphSerializationException(
                    null, "Deserialization failed for " + type.getSimpleName(), e);
        }
    }

    private boolean cas(String graphId, List<Precondition> preconditions, List<Mutation> mutations)
            throws GraphException {
        List<Precondition> ksPreconditions = new ArrayList<>();
        List<Mutation> ksMutations = new ArrayList<>();
        List<Precondition> msPreconditions = new ArrayList<>();
        List<Mutation> msMutations = new ArrayList<>();

        for (Precondition p : preconditions) {
            byte[] pKey = preconditionKey(p);
            if (isKnowledgeKey(pKey)) {
                ksPreconditions.add(p);
            } else {
                msPreconditions.add(p);
            }
        }

        for (Mutation m : mutations) {
            byte[] key = mutationKey(m);
            if (isKnowledgeKey(key)) {
                ksMutations.add(m);
            } else {
                msMutations.add(m);
            }
        }

        try {
            if (!ksMutations.isEmpty()) {
                boolean ksSuccess = knowledgeStore.compareAndSwap(ksPreconditions, ksMutations);
                if (!ksSuccess) return false;
            }
            if (!msMutations.isEmpty()) {
                boolean msSuccess = metastore.compareAndSwap(msPreconditions, msMutations);
                if (!msSuccess) {
                    compensateKnowledgeStore(ksMutations);
                    return false;
                }
            }
            return true;
        } catch (IOException e) {
            throw new GraphException(graphId, "CAS operation failed", e);
        }
    }

    private byte[] preconditionKey(Precondition p) {
        if (p instanceof Precondition.Exists e) return e.key();
        if (p instanceof Precondition.NotExists e) return e.key();
        if (p instanceof Precondition.ValueEquals e) return e.key();
        return new byte[0];
    }

    private void compensateKnowledgeStore(List<Mutation> ksMutations) {
        try {
            List<Mutation> compensations = new ArrayList<>();
            for (Mutation m : ksMutations) {
                if (m instanceof Mutation.Put) {
                    compensations.add(new Mutation.Delete(mutationKey(m)));
                }
            }
            if (!compensations.isEmpty()) {
                knowledgeStore.batchWrite(compensations);
            }
        } catch (IOException e) {
            log.warn("KnowledgeStore compensation failed", e);
        }
    }

    private byte[] mutationKey(Mutation m) {
        if (m instanceof Mutation.Put put) return put.key();
        if (m instanceof Mutation.Delete del) return del.key();
        return new byte[0];
    }

    private boolean isKnowledgeKey(byte[] key) {
        String keyStr = new String(key, StandardCharsets.UTF_8);
        return keyStr.startsWith(NODE_PREFIX)
                || keyStr.startsWith(EDGE_PREFIX)
                || keyStr.startsWith(ADJ_PREFIX);
    }

    private List<KeyValue> scanAll(String graphId, byte[] prefix) throws GraphException {
        String prefixStr = new String(prefix, StandardCharsets.UTF_8);
        try {
            if (prefixStr.startsWith(NODE_PREFIX)
                    || prefixStr.startsWith(EDGE_PREFIX)
                    || prefixStr.startsWith(ADJ_PREFIX)) {
                return knowledgeStore.scanAll(prefix);
            }
            return metastore.scanAll(prefix);
        } catch (IOException e) {
            throw new GraphException(graphId, "Scan operation failed", e);
        }
    }

    private void ensureGraphExists(String graphId) throws GraphException {
        if (getGraph(graphId).isEmpty()) {
            throw new GraphNotFoundException(graphId);
        }
    }

    private void ensureNodeExists(String graphId, String nodeId) throws GraphException {
        if (getNode(graphId, nodeId).isEmpty()) {
            throw new GraphNodeNotFoundException(graphId, nodeId);
        }
    }

    private boolean matchesProperties(GraphNode node, Map<String, String> propertyFilter) {
        if (propertyFilter == null || propertyFilter.isEmpty()) return true;
        for (Map.Entry<String, String> filter : propertyFilter.entrySet()) {
            String nodeValue = node.properties().get(filter.getKey());
            if (!filter.getValue().equals(nodeValue)) return false;
        }
        return true;
    }

    private int parseImportance(Map<String, String> properties) {
        String val = properties.get("importance");
        if (val != null) {
            try {
                return Integer.parseInt(val);
            } catch (NumberFormatException ignored) {
            }
        }
        return 5;
    }

    private float parseDecayFactor(Map<String, String> properties) {
        String val = properties.get("decayFactor");
        if (val != null) {
            try {
                return Float.parseFloat(val);
            } catch (NumberFormatException ignored) {
            }
        }
        return 1.0f;
    }

    private List<GraphEdge> collectEdges(
            String graphId, String nodeId, TraversalDirection direction, String label)
            throws GraphException {
        List<GraphEdge> edges = new ArrayList<>();
        if (direction == TraversalDirection.OUT || direction == TraversalDirection.BOTH) {
            edges.addAll(getOutEdges(graphId, nodeId, label));
        }
        if (direction == TraversalDirection.IN || direction == TraversalDirection.BOTH) {
            edges.addAll(getInEdges(graphId, nodeId, label));
        }
        return edges;
    }

    private String resolveNeighbor(GraphEdge edge, String currentId) {
        if (edge.srcNodeId().equals(currentId)) return edge.dstNodeId();
        if (edge.dstNodeId().equals(currentId)) return edge.srcNodeId();
        return null;
    }

    private PathResult buildPath(
            String graphId,
            String srcNodeId,
            String dstNodeId,
            String meetingPoint,
            Map<String, String> srcParent,
            Map<String, String> srcEdge,
            Map<String, String> srcEdgeLabel,
            Map<String, Float> srcEdgeWeight,
            Map<String, String> dstParent,
            Map<String, String> dstEdge,
            Map<String, String> dstEdgeLabel,
            Map<String, Float> dstEdgeWeight) {
        List<String> nodeIds = new ArrayList<>();
        List<String> edgeIds = new ArrayList<>();
        List<String> edgeLabels = new ArrayList<>();
        float totalWeight = 0;

        List<String> srcPath = new ArrayList<>();
        List<String> srcEdgeIds = new ArrayList<>();
        List<String> srcEdgeLabels = new ArrayList<>();
        String current = meetingPoint;
        while (current != null) {
            srcPath.add(current);
            if (srcEdge.containsKey(current)) {
                srcEdgeIds.add(srcEdge.get(current));
                srcEdgeLabels.add(srcEdgeLabel.get(current));
                totalWeight += srcEdgeWeight.getOrDefault(current, 0f);
            }
            current = srcParent.get(current);
        }
        Collections.reverse(srcPath);
        Collections.reverse(srcEdgeIds);
        Collections.reverse(srcEdgeLabels);

        List<String> dstPath = new ArrayList<>();
        List<String> dstEdgeIds = new ArrayList<>();
        List<String> dstEdgeLabels = new ArrayList<>();
        current = meetingPoint;
        while (current != null) {
            dstPath.add(current);
            if (dstEdge.containsKey(current)) {
                dstEdgeIds.add(dstEdge.get(current));
                dstEdgeLabels.add(dstEdgeLabel.get(current));
                totalWeight += dstEdgeWeight.getOrDefault(current, 0f);
            }
            current = dstParent.get(current);
        }

        nodeIds.addAll(srcPath);
        for (int i = 1; i < dstPath.size(); i++) {
            nodeIds.add(dstPath.get(i));
        }
        edgeIds.addAll(srcEdgeIds);
        edgeIds.addAll(dstEdgeIds);
        edgeLabels.addAll(srcEdgeLabels);
        edgeLabels.addAll(dstEdgeLabels);

        return new PathResult(
                graphId,
                srcNodeId,
                dstNodeId,
                nodeIds,
                edgeIds,
                edgeLabels,
                totalWeight,
                edgeIds.size());
    }
}
