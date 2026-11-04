package io.github.rawwise.axiom.orchestrator;

public enum StepStatus {
    PENDING,
    READY,
    RUNNING,
    WAITING,
    COMPLETED,
    FAILED,
    RETRYING,
    TIMEOUT,
    COMPENSATING,
    COMPENSATED,
    COMPENSATION_FAILED,
    BLOCKED,
    SKIPPED,
    CANCELLED
}
