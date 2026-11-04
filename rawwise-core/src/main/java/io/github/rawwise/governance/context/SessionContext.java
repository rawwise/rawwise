package io.github.rawwise.governance.context;

public record SessionContext(
        String sessionId,
        String agentId,
        String userId,
        long createdAt,
        long updatedAt,
        long expiresAt,
        int entryCount,
        SessionStatus status) {

    public SessionContext {
        if (sessionId == null || sessionId.isEmpty()) {
            throw new IllegalArgumentException("sessionId must not be null or empty");
        }
        if (agentId == null || agentId.isEmpty()) {
            throw new IllegalArgumentException("agentId must not be null or empty");
        }
        if (userId == null || userId.isEmpty()) {
            throw new IllegalArgumentException("userId must not be null or empty");
        }
        if (status == null) {
            status = SessionStatus.ACTIVE;
        }
    }

    public enum SessionStatus {
        ACTIVE,
        EXPIRED,
        TERMINATED
    }
}
