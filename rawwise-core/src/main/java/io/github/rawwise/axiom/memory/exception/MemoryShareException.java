package io.github.rawwise.axiom.memory.exception;

public class MemoryShareException extends AxiomException {
    public MemoryShareException(String message) {
        super("AXIOM_MEMORY_SHARE_FAILED", 500, message);
    }

    public MemoryShareException(String message, Throwable cause) {
        super("AXIOM_MEMORY_SHARE_FAILED", 500, message, cause);
    }
}
