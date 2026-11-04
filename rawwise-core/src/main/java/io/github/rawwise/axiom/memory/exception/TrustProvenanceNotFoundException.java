package io.github.rawwise.axiom.memory.exception;

public class TrustProvenanceNotFoundException extends AxiomException {
    public TrustProvenanceNotFoundException(String message) {
        super("AXIOM_TRUST_PROVENANCE_NOT_FOUND", 404, message);
    }

    public TrustProvenanceNotFoundException(String message, Throwable cause) {
        super("AXIOM_TRUST_PROVENANCE_NOT_FOUND", 404, message, cause);
    }
}
