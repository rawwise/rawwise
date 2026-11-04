package io.github.rawwise.engine.storage.metrics;

public enum ReaderErrorType {
    THROTTLING,
    TIMEOUT,
    CORRUPTION,
    DECOMPRESSION,
    NETWORK,
    STORAGE_UNAVAILABLE,
    PERMISSION_DENIED,
    UNKNOWN
}
