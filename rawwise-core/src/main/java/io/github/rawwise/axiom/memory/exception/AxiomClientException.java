package io.github.rawwise.axiom.memory.exception;

/** Raised when the Axiom HTTP client receives a deterministic 4xx (client/business) error. */
public class AxiomClientException extends AxiomException {
    public AxiomClientException(int httpStatus, String message) {
        super("AXIOM_CLIENT_ERROR", httpStatus, message);
    }

    public AxiomClientException(int httpStatus, String message, Throwable cause) {
        super("AXIOM_CLIENT_ERROR", httpStatus, message, cause);
    }
}
