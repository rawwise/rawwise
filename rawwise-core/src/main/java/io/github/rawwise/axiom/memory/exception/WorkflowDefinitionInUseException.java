package io.github.rawwise.axiom.memory.exception;

public class WorkflowDefinitionInUseException extends AxiomException {
    public WorkflowDefinitionInUseException(String message) {
        super("AXIOM_WORKFLOW_DEFINITION_IN_USE", 409, message);
    }

    public WorkflowDefinitionInUseException(String message, Throwable cause) {
        super("AXIOM_WORKFLOW_DEFINITION_IN_USE", 409, message, cause);
    }
}
