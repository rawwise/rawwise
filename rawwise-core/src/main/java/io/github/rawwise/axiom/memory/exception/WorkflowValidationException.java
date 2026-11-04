package io.github.rawwise.axiom.memory.exception;

public class WorkflowValidationException extends AxiomException {
    public WorkflowValidationException(String message) {
        super("AXIOM_WORKFLOW_VALIDATION_ERROR", 422, message);
    }

    public WorkflowValidationException(String message, Throwable cause) {
        super("AXIOM_WORKFLOW_VALIDATION_ERROR", 422, message, cause);
    }
}
