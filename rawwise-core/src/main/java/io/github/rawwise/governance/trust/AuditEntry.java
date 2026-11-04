package io.github.rawwise.governance.trust;

import java.util.Map;

public record AuditEntry(
        String agentId,
        String operation,
        String contextType,
        String contextId,
        String entryKey,
        Map<String, String> metadata,
        long timestamp) {
    public AuditEntry {
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }
}
