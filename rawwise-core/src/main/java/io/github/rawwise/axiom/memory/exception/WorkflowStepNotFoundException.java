package io.github.rawwise.axiom.memory.exception;

public class WorkflowStepNotFoundException extends AxiomException {
    public WorkflowStepNotFoundException(String message) {
        super("AXIOM_WORKFLOW_STEP_NOT_FOUND", 404, message);
    }

    public WorkflowStepNotFoundException(String message, Throwable cause) {
        super("AXIOM_WORKFLOW_STEP_NOT_FOUND", 404, message, cause);
    }
}
