package io.github.rawwise.axiom.memory.exception;

public class MemoryEntryNotFoundException extends AxiomException {
    public MemoryEntryNotFoundException(String message) {
        super("AXIOM_MEMORY_ENTRY_NOT_FOUND", 404, message);
    }

    public MemoryEntryNotFoundException(String message, Throwable cause) {
        super("AXIOM_MEMORY_ENTRY_NOT_FOUND", 404, message, cause);
    }
}
