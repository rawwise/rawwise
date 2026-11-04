package io.github.rawwise.engine.storage;

public class DurabilityViolationException extends DynamicMmapStore.RecoverableWriteException {
    public DurabilityViolationException(String message) {
        super(message);
    }
}
