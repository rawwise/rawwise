package io.github.rawwise.axiom.memory.exception;

import lombok.Getter;

@Getter
public class AxiomException extends RuntimeException {
    private final String errorCode;
    private final int httpStatus;

    public AxiomException(String errorCode, int httpStatus, String message) {
        super(message);
        this.errorCode = errorCode;
        this.httpStatus = httpStatus;
    }

    public AxiomException(String errorCode, int httpStatus, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.httpStatus = httpStatus;
    }
}
