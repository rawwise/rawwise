package io.github.rawwise.axiom.memory.exception;

public class WorkflowQuotaExceededException extends AxiomException {
    public WorkflowQuotaExceededException(String message) {
        super("AXIOM_WORKFLOW_QUOTA_EXCEEDED", 429, message);
    }

    public WorkflowQuotaExceededException(String message, Throwable cause) {
        super("AXIOM_WORKFLOW_QUOTA_EXCEEDED", 429, message, cause);
    }
}
