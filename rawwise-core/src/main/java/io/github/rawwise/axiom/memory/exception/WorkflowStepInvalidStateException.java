package io.github.rawwise.axiom.memory.exception;

public class WorkflowStepInvalidStateException extends AxiomException {
    public WorkflowStepInvalidStateException(String message) {
        super("AXIOM_WORKFLOW_STEP_INVALID_STATE", 409, message);
    }

    public WorkflowStepInvalidStateException(String message, Throwable cause) {
        super("AXIOM_WORKFLOW_STEP_INVALID_STATE", 409, message, cause);
    }
}
