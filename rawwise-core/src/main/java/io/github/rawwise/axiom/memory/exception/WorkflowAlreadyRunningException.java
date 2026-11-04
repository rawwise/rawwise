package io.github.rawwise.axiom.memory.exception;

public class WorkflowAlreadyRunningException extends AxiomException {
    public WorkflowAlreadyRunningException(String message) {
        super("AXIOM_WORKFLOW_ALREADY_RUNNING", 409, message);
    }

    public WorkflowAlreadyRunningException(String message, Throwable cause) {
        super("AXIOM_WORKFLOW_ALREADY_RUNNING", 409, message, cause);
    }
}
