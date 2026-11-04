package io.github.rawwise.axiom.memory.exception;

public class MemoryRecallException extends AxiomException {
    public MemoryRecallException(String message) {
        super("AXIOM_MEMORY_RECALL_FAILED", 500, message);
    }

    public MemoryRecallException(String message, Throwable cause) {
        super("AXIOM_MEMORY_RECALL_FAILED", 500, message, cause);
    }
}
