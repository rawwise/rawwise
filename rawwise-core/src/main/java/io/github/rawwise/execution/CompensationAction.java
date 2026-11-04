package io.github.rawwise.execution;

import java.util.Map;

public record CompensationAction(
        String compensationId,
        CompensationType type,
        String toolName,
        Map<String, String> toolInput,
        String description) {

    public CompensationAction {
        if (compensationId == null || compensationId.isEmpty()) {
            throw new IllegalArgumentException("compensationId must not be null or empty");
        }
        if (type == null) {
            type = CompensationType.MARK_ONLY;
        }
        if (toolName == null) {
            toolName = "";
        }
        if (toolInput == null) {
            toolInput = Map.of();
        } else {
            toolInput = Map.copyOf(toolInput);
        }
        if (description == null) {
            description = "";
        }
    }

    @Override
    public Map<String, String> toolInput() {
        return toolInput;
    }

    public enum CompensationType {
        TOOL_CALL,
        MARK_ONLY,
        MANUAL
    }
}
