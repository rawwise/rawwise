package io.github.rawwise.axiom.memory.semantic;

import io.github.rawwise.axiom.memory.MemoryTier;
import io.github.rawwise.axiom.pipeline.state.StateEngine;
import io.github.rawwise.governance.context.ContextJson;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphStateAdapter {

    private static final String STATE_MAPPING_PREFIX = "graph::state::";

    private final GraphEngine graphEngine;
    private final StateEngine stateEngine;
    private final Metastore metastore;

    public GraphStateAdapter(
            GraphEngine graphEngine, StateEngine stateEngine, Metastore metastore) {
        this.graphEngine = graphEngine;
        this.stateEngine = stateEngine;
        this.metastore = metastore;
    }

    public StateMapping bindNodeToStateSpace(
            String graphId, String nodeId, String spaceId, String initialState)
            throws GraphException {
        graphEngine
                .getNode(graphId, nodeId)
                .orElseThrow(() -> new GraphNodeNotFoundException(graphId, nodeId));

        String mappingKey = STATE_MAPPING_PREFIX + graphId + "::" + nodeId;
        StateMapping mapping =
                new StateMapping(
                        graphId,
                        nodeId,
                        spaceId,
                        initialState,
                        1L,
                        System.currentTimeMillis(),
                        System.currentTimeMillis());

        byte[] keyBytes = mappingKey.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes;
        try {
            valueBytes = ContextJson.writeValueAsBytes(mapping);
        } catch (IOException e) {
            throw new GraphException(graphId, "Failed to serialize state mapping", e);
        }

        List<Precondition> preconditions = List.of(new Precondition.NotExists(keyBytes));
        List<Mutation> mutations = List.of(new Mutation.Put(keyBytes, valueBytes));

        try {
            boolean success = metastore.compareAndSwap(preconditions, mutations);
            if (!success) {
                throw new GraphCasConflictException(
                        graphId, "Node already bound to state space: " + nodeId);
            }
        } catch (IOException e) {
            throw new GraphException(graphId, "Failed to bind node to state space", e);
        }

        log.debug(
                "Bound node graphId={} nodeId={} to spaceId={} state={}",
                graphId,
                nodeId,
                spaceId,
                initialState);
        return mapping;
    }

    public StateMapping transitionNode(
            String graphId, String nodeId, String toState, Map<String, String> metadata)
            throws GraphException {
        StateMapping current = getStateMapping(graphId, nodeId);

        StateMapping proposed =
                new StateMapping(
                        graphId,
                        nodeId,
                        current.spaceId(),
                        toState,
                        current.version() + 1,
                        current.createdAt(),
                        System.currentTimeMillis());

        try {
            stateEngine.transition(current.spaceId(), nodeId, toState, metadata);
        } catch (Exception e) {
            throw new GraphException(
                    graphId, "StateEngine transition failed for node: " + nodeId, e);
        }

        String mappingKey = STATE_MAPPING_PREFIX + graphId + "::" + nodeId;
        byte[] keyBytes = mappingKey.getBytes(StandardCharsets.UTF_8);
        byte[] oldValue;
        byte[] newValue;
        try {
            oldValue = ContextJson.writeValueAsBytes(current);
            newValue = ContextJson.writeValueAsBytes(proposed);
        } catch (IOException e) {
            throw new GraphException(graphId, "Failed to serialize state mapping", e);
        }

        List<Precondition> preconditions =
                List.of(new Precondition.ValueEquals(keyBytes, oldValue));
        List<Mutation> mutations = List.of(new Mutation.Put(keyBytes, newValue));

        boolean casSuccess;
        try {
            casSuccess = metastore.compareAndSwap(preconditions, mutations);
        } catch (IOException e) {
            throw new GraphException(graphId, "Failed to transition node state: " + nodeId, e);
        }

        if (!casSuccess) {
            throw new GraphCasConflictException(
                    graphId, "State mapping version conflict for node: " + nodeId);
        }

        log.debug(
                "Transitioned node graphId={} nodeId={} from={} to={}",
                graphId,
                nodeId,
                current.stateName(),
                toState);

        return proposed;
    }

    public StateMapping getStateMapping(String graphId, String nodeId) throws GraphException {
        String mappingKey = STATE_MAPPING_PREFIX + graphId + "::" + nodeId;
        byte[] keyBytes = mappingKey.getBytes(StandardCharsets.UTF_8);

        try {
            Optional<byte[]> value = metastore.get(keyBytes);
            if (value.isEmpty()) {
                throw new GraphException(graphId, "No state mapping found for node: " + nodeId);
            }
            return ContextJson.readValue(value.get(), StateMapping.class);
        } catch (IOException e) {
            throw new GraphException(
                    graphId, "Failed to read state mapping for node: " + nodeId, e);
        }
    }

    public MemoryTier resolveMemoryTier(GraphNode node) {
        if (node == null) return MemoryTier.WORKING;
        String type = node.type();
        if (type != null
                && (type.equalsIgnoreCase("procedure") || type.equalsIgnoreCase("action"))) {
            return MemoryTier.PROCEDURAL;
        }
        int importance = node.importance();
        if (importance >= 8) return MemoryTier.SEMANTIC;
        if (importance >= 4) return MemoryTier.EPISODIC;
        return MemoryTier.WORKING;
    }

    public NodeStateTier extractNodeStateTier(String graphId, String nodeId) throws GraphException {
        GraphNode node =
                graphEngine
                        .getNode(graphId, nodeId)
                        .orElseThrow(() -> new GraphNodeNotFoundException(graphId, nodeId));
        StateMapping mapping = getStateMapping(graphId, nodeId);
        MemoryTier tier = resolveMemoryTier(node);
        return new NodeStateTier(nodeId, mapping.stateName(), mapping.version(), tier);
    }

    public record StateMapping(
            String graphId,
            String nodeId,
            String spaceId,
            String stateName,
            long version,
            long createdAt,
            long updatedAt) {}

    public record NodeStateTier(
            String nodeId, String stateName, long version, MemoryTier memoryTier) {}
}
