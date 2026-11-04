package io.github.rawwise.axiom.memory.semantic;

import io.github.rawwise.axiom.memory.exception.AxiomException;

public sealed class GraphException extends AxiomException
        permits GraphCapacityExceededException,
                GraphCasConflictException,
                GraphEdgeNotFoundException,
                GraphIntegrityException,
                GraphNodeNotFoundException,
                GraphNotFoundException,
                GraphSchemaViolationException,
                GraphSerializationException {

    private static final String DEFAULT_ERROR_CODE = "AXIOM_GRAPH_ERROR";
    private static final int DEFAULT_HTTP_STATUS = 500;

    private final String graphId;

    public GraphException(String graphId, String message) {
        super(DEFAULT_ERROR_CODE, DEFAULT_HTTP_STATUS, message);
        this.graphId = graphId;
    }

    public GraphException(String graphId, String message, Throwable cause) {
        super(DEFAULT_ERROR_CODE, DEFAULT_HTTP_STATUS, message, cause);
        this.graphId = graphId;
    }

    public GraphException(String graphId, String errorCode, int httpStatus, String message) {
        super(errorCode, httpStatus, message);
        this.graphId = graphId;
    }

    public GraphException(
            String graphId, String errorCode, int httpStatus, String message, Throwable cause) {
        super(errorCode, httpStatus, message, cause);
        this.graphId = graphId;
    }

    public String graphId() {
        return graphId;
    }
}
