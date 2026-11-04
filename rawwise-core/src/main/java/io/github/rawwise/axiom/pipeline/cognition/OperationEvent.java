package io.github.rawwise.axiom.pipeline.cognition;

public record OperationEvent(
        String eventId,
        String actionId,
        String planId,
        String agentId,
        OperationType operation,
        String targetKey,
        String targetType,
        String detail,
        long timestamp) {
    public enum OperationType {
        READ,
        WRITE,
        LOCK,
        UNLOCK,
        DELETE,
        INDEX
    }

    public OperationEvent {
        if (targetType == null || targetType.isBlank()) {
            targetType = parseTargetType(targetKey);
        }
    }

    private static String parseTargetType(String key) {
        if (key == null) return "unknown";
        if (key.startsWith("view:")) return "view";
        if (key.startsWith("semantic:")) return "semantic";
        if (key.startsWith("action:")) return "action";
        if (key.startsWith("notification:")) return "notification";
        if (key.startsWith("coord::lock::")) return "coord-lock";
        if (key.startsWith("trust:")) return "trust";
        return "other";
    }
}
