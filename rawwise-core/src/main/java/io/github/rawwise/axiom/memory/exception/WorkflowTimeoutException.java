package io.github.rawwise.axiom.memory.exception;

public class WorkflowTimeoutException extends AxiomException {
    public WorkflowTimeoutException(String message) {
        super("AXIOM_WORKFLOW_TIMEOUT", 408, message);
    }

    public WorkflowTimeoutException(String message, Throwable cause) {
        super("AXIOM_WORKFLOW_TIMEOUT", 408, message, cause);
    }
}
