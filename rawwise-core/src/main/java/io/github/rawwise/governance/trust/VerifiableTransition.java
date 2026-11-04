package io.github.rawwise.governance.trust;

import java.util.Map;

public record VerifiableTransition(
        String transitionId,
        String contextType,
        String contextId,
        String entryKey,
        int fromVersion,
        int toVersion,
        String fromHash,
        String toHash,
        String operation,
        Map<String, String> metadata,
        long timestamp) {
    public VerifiableTransition {
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }
}
