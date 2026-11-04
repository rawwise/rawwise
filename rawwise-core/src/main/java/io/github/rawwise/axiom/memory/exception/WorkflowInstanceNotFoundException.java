package io.github.rawwise.axiom.memory.exception;

public class WorkflowInstanceNotFoundException extends AxiomException {
    public WorkflowInstanceNotFoundException(String message) {
        super("AXIOM_WORKFLOW_INSTANCE_NOT_FOUND", 404, message);
    }

    public WorkflowInstanceNotFoundException(String message, Throwable cause) {
        super("AXIOM_WORKFLOW_INSTANCE_NOT_FOUND", 404, message, cause);
    }
}
