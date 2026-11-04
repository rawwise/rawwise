package io.github.rawwise.axiom.memory.semantic;

public final class GraphNodeNotFoundException extends GraphException {
    public GraphNodeNotFoundException(String graphId, String nodeId) {
        super(graphId, "AXIOM_GRAPH_NODE_NOT_FOUND", 404, "Node not found: " + nodeId);
    }

    public GraphNodeNotFoundException(String graphId, String nodeId, Throwable cause) {
        super(graphId, "AXIOM_GRAPH_NODE_NOT_FOUND", 404, "Node not found: " + nodeId, cause);
    }
}
