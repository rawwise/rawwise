package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.model.record.Action;
import java.util.Set;

public interface ActionExecutor {

    String executorId();

    Set<Action.ActionType> supportedActions();

    CognitionEngine.ActionResult execute(Action action, ExecutionContext context);

    CognitionEngine.PlanResult executePlan(ActionPlan plan, ExecutionContext context);
}
