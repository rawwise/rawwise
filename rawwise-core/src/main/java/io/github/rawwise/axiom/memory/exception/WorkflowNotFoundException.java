package io.github.rawwise.axiom.memory.exception;

public class WorkflowNotFoundException extends AxiomException {
    public WorkflowNotFoundException(String message) {
        super("AXIOM_WORKFLOW_NOT_FOUND", 404, message);
    }

    public WorkflowNotFoundException(String message, Throwable cause) {
        super("AXIOM_WORKFLOW_NOT_FOUND", 404, message, cause);
    }
}
