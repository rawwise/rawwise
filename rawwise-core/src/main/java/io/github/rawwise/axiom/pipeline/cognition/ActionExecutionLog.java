package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.model.record.Action;
import java.util.List;

public record ActionExecutionLog(
        String logId,
        String agentId,
        String executorId,
        String actionId,
        String planId,
        Action.ActionType actionType,
        long startedAt,
        long completedAt,
        List<String> readData,
        List<String> modifiedData,
        List<String> lockResources,
        String output,
        boolean success,
        String errorMessage) {
    public ActionExecutionLog {
        readData = readData == null ? List.of() : List.copyOf(readData);
        modifiedData = modifiedData == null ? List.of() : List.copyOf(modifiedData);
        lockResources = lockResources == null ? List.of() : List.copyOf(lockResources);
    }
}
