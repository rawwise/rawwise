package io.github.rawwise.backend.server.job;

public enum JobStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLING,
    CANCELLED,
    TIMED_OUT;

    public boolean canTransitionTo(JobStatus target) {
        return switch (target) {
            case RUNNING -> this == PENDING;
            case SUCCEEDED -> this == RUNNING;
            case FAILED -> this == RUNNING || this == CANCELLING;
            case CANCELLING -> this == RUNNING;
            case CANCELLED -> this == PENDING || this == CANCELLING;
            case TIMED_OUT -> this == PENDING || this == RUNNING || this == CANCELLING;
            default -> false;
        };
    }
}
