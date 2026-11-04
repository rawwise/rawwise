package io.github.rawwise.axiom.memory.exception;

public class WorkflowCompensationFailedException extends AxiomException {
    public WorkflowCompensationFailedException(String message) {
        super("AXIOM_WORKFLOW_COMPENSATION_FAILED", 500, message);
    }

    public WorkflowCompensationFailedException(String message, Throwable cause) {
        super("AXIOM_WORKFLOW_COMPENSATION_FAILED", 500, message, cause);
    }
}
