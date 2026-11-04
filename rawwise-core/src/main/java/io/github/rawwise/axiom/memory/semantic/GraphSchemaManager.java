package io.github.rawwise.axiom.memory.semantic;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface GraphSchemaManager {
    GraphSchema createGraph(
            String agentId,
            String name,
            String description,
            List<String> nodeTypes,
            List<String> edgeLabels,
            Map<String, String> metadata)
            throws GraphException;

    Optional<GraphSchema> getGraph(String graphId) throws GraphException;

    List<GraphSchema> listGraphsByAgent(String agentId) throws GraphException;

    boolean deleteGraph(String graphId) throws GraphException;
}
