package io.github.rawwise.backend.server.session;

public enum SessionState {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED,
    TIMEOUT
}
