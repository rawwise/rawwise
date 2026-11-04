package io.github.rawwise.governance.auth;

import java.util.Map;

public record ProvenanceEvent(
        String eventId,
        String principalId,
        String principalType,
        String action,
        String targetId,
        String targetType,
        boolean granted,
        Map<String, String> context,
        long timestamp) {
    public ProvenanceEvent {
        context = context == null ? Map.of() : Map.copyOf(context);
    }

    public enum ActionType {
        ACCESS,
        GRANT,
        REVOKE,
        ROLE_CREATE,
        ROLE_DELETE,
        TOKEN_CREATE,
        TOKEN_REVOKE,
        AGENT_CREATE,
        DATA_READ,
        DATA_WRITE,
        DATA_DELETE
    }

    public enum TargetType {
        TABLE,
        ROLE,
        TOKEN,
        AGENT,
        SYSTEM
    }
}
