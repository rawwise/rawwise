package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.model.record.Action;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ActionExecutorRegistry {

    private final Map<Action.ActionType, ActionExecutor> executors = new ConcurrentHashMap<>();

    public void register(Action.ActionType actionType, ActionExecutor executor) {
        if (actionType == null || executor == null) {
            throw new IllegalArgumentException("actionType and executor are required");
        }
        ActionExecutor previous = executors.put(actionType, executor);
        if (previous != null) {
            log.info(
                    "ActionExecutor for {} overridden: {} -> {}",
                    actionType,
                    previous.executorId(),
                    executor.executorId());
        } else {
            log.info("ActionExecutor registered for {}: {}", actionType, executor.executorId());
        }
    }

    public ActionExecutor resolve(Action.ActionType actionType) {
        return executors.get(actionType);
    }

    public Set<Action.ActionType> registeredTypes() {
        return Set.copyOf(executors.keySet());
    }

    public void unregister(Action.ActionType actionType) {
        executors.remove(actionType);
    }
}
