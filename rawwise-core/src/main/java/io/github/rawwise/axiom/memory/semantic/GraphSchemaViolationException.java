package io.github.rawwise.axiom.memory.semantic;

public final class GraphSchemaViolationException extends GraphException {
    public GraphSchemaViolationException(String graphId, String detail) {
        super(graphId, "AXIOM_GRAPH_SCHEMA_VIOLATION", 422, "Schema violation: " + detail);
    }

    public GraphSchemaViolationException(String graphId, String detail, Throwable cause) {
        super(graphId, "AXIOM_GRAPH_SCHEMA_VIOLATION", 422, "Schema violation: " + detail, cause);
    }
}
