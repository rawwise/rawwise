package io.github.rawwise.axiom.orchestrator;

import io.github.rawwise.axiom.memory.exception.AxiomException;

public sealed class CoordException extends AxiomException
        permits CoordAcquireTimeoutException,
                CoordConflictException,
                CoordExpiredException,
                CoordNotFoundException,
                CoordSessionException {

    private static final String DEFAULT_ERROR_CODE = "AXIOM_COORD_ERROR";
    private static final int DEFAULT_HTTP_STATUS = 500;

    private final String resourceId;

    public CoordException(String resourceId, String message) {
        super(DEFAULT_ERROR_CODE, DEFAULT_HTTP_STATUS, message);
        this.resourceId = resourceId;
    }

    public CoordException(String resourceId, String message, Throwable cause) {
        super(DEFAULT_ERROR_CODE, DEFAULT_HTTP_STATUS, message, cause);
        this.resourceId = resourceId;
    }

    public CoordException(String resourceId, String errorCode, int httpStatus, String message) {
        super(errorCode, httpStatus, message);
        this.resourceId = resourceId;
    }

    public CoordException(
            String resourceId, String errorCode, int httpStatus, String message, Throwable cause) {
        super(errorCode, httpStatus, message, cause);
        this.resourceId = resourceId;
    }

    public String resourceId() {
        return resourceId;
    }
}
