package io.github.rawwise.model.record;

import java.util.List;

public record Action(
        String actionId,
        List<String> sourceSemanticIds,
        ActionType actionType,
        byte[] content,
        boolean executable) {
    public enum ActionType {
        RETRIEVE,
        TRANSFORM,
        NOTIFY,
        CORRECT
    }
}
