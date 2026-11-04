package io.github.rawwise.governance.trust;

import java.util.List;

public record ProvenanceChain(
        String id, List<VerifiableTransition> transitions, boolean valid, long verifiedAt) {
    public ProvenanceChain {
        transitions = transitions == null ? List.of() : List.copyOf(transitions);
    }
}
