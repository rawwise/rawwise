package io.github.rawwise.governance.context;

public record L0Summary(
        String contextType,
        String contextId,
        String entryKey,
        String l0Abstract,
        int importance,
        float decayFactor,
        long createdAt,
        long lastAccessedAt,
        int accessCount) {

    public L0Summary {
        if (contextType == null || contextType.isEmpty()) {
            throw new IllegalArgumentException("contextType must not be null or empty");
        }
        if (contextId == null || contextId.isEmpty()) {
            throw new IllegalArgumentException("contextId must not be null or empty");
        }
        if (entryKey == null || entryKey.isEmpty()) {
            throw new IllegalArgumentException("entryKey must not be null or empty");
        }
        if (l0Abstract == null) {
            l0Abstract = "";
        }
    }
}
