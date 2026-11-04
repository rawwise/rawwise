package io.github.rawwise.axiom.orchestrator;

import java.util.List;

public sealed interface WorkflowValidationResult {
    record Valid(WorkflowDefinition definition) implements WorkflowValidationResult {}

    record Invalid(List<ValidationError> errors) implements WorkflowValidationResult {
        public Invalid {
            errors = List.copyOf(errors);
        }
    }
}
