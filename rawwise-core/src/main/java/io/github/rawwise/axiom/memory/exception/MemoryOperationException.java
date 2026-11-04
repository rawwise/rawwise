package io.github.rawwise.axiom.memory.exception;

public class MemoryOperationException extends AxiomException {
    public MemoryOperationException(String message) {
        super("AXIOM_MEMORY_OPERATION_FAILED", 500, message);
    }

    public MemoryOperationException(String message, Throwable cause) {
        super("AXIOM_MEMORY_OPERATION_FAILED", 500, message, cause);
    }
}
