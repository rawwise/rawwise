package io.github.rawwise.axiom.memory.exception;

public class StateSpaceNotFoundException extends AxiomException {
    public StateSpaceNotFoundException(String message) {
        super("AXIOM_STATE_SPACE_NOT_FOUND", 404, message);
    }

    public StateSpaceNotFoundException(String message, Throwable cause) {
        super("AXIOM_STATE_SPACE_NOT_FOUND", 404, message, cause);
    }
}
