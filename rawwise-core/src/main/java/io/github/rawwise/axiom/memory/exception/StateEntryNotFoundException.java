package io.github.rawwise.axiom.memory.exception;

public class StateEntryNotFoundException extends AxiomException {
    public StateEntryNotFoundException(String message) {
        super("AXIOM_STATE_ENTRY_NOT_FOUND", 404, message);
    }

    public StateEntryNotFoundException(String message, Throwable cause) {
        super("AXIOM_STATE_ENTRY_NOT_FOUND", 404, message, cause);
    }
}
