package io.github.rawwise.axiom.orchestrator;

public enum WorkflowInstanceStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED,
    COMPENSATING,
    COMPENSATED,
    COMPENSATION_FAILED,
    TIMEOUT,
    CANCELLED
}
