package io.github.rawwise.axiom.memory.exception;

public class StateCasConflictException extends AxiomException {
    public StateCasConflictException(String message) {
        super("AXIOM_STATE_CAS_CONFLICT", 409, message);
    }

    public StateCasConflictException(String message, Throwable cause) {
        super("AXIOM_STATE_CAS_CONFLICT", 409, message, cause);
    }
}
