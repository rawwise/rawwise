package io.github.rawwise.axiom.memory.semantic;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface GraphNodeOperations {
    GraphNode addNode(String graphId, String type, Map<String, String> properties)
            throws GraphException;

    Optional<GraphNode> getNode(String graphId, String nodeId) throws GraphException;

    GraphNode updateNode(String graphId, GraphNode node) throws GraphException;

    boolean removeNode(String graphId, String nodeId) throws GraphException;

    List<GraphNode> queryNodes(
            String graphId, String type, Map<String, String> propertyFilter, int limit)
            throws GraphException;
}
