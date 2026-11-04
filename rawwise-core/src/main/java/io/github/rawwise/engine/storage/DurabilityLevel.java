package io.github.rawwise.engine.storage;

public enum DurabilityLevel {
    PROCESS,
    NODE;

    public boolean isWalRequired() {
        return this == NODE;
    }
}
