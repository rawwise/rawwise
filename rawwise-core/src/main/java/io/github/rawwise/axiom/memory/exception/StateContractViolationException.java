package io.github.rawwise.axiom.memory.exception;

public class StateContractViolationException extends AxiomException {
    public StateContractViolationException(String message) {
        super("AXIOM_STATE_CONTRACT_VIOLATION", 422, message);
    }

    public StateContractViolationException(String message, Throwable cause) {
        super("AXIOM_STATE_CONTRACT_VIOLATION", 422, message, cause);
    }
}
