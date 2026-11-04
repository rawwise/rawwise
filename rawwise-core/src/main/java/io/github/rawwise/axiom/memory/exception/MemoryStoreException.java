package io.github.rawwise.axiom.memory.exception;

public class MemoryStoreException extends AxiomException {
    public MemoryStoreException(String message) {
        super("AXIOM_MEMORY_STORE_FAILED", 500, message);
    }

    public MemoryStoreException(String message, Throwable cause) {
        super("AXIOM_MEMORY_STORE_FAILED", 500, message, cause);
    }
}
