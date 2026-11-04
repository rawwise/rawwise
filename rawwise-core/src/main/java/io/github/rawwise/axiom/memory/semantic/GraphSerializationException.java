package io.github.rawwise.axiom.memory.semantic;

public final class GraphSerializationException extends GraphException {

    private static final String ERROR_CODE = "AXIOM_GRAPH_SERIALIZATION_ERROR";
    private static final int HTTP_STATUS = 500;

    public GraphSerializationException(String graphId, String detail) {
        super(graphId, ERROR_CODE, HTTP_STATUS, "Serialization error: " + detail);
    }

    public GraphSerializationException(String graphId, String detail, Throwable cause) {
        super(graphId, ERROR_CODE, HTTP_STATUS, "Serialization error: " + detail, cause);
    }
}
