package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.model.record.Action;
import io.github.rawwise.model.record.Semantic;
import java.util.List;
import java.util.Map;

public interface CognitionEngine {

    Semantic deriveSemantic(
            List<String> viewIds, Semantic.SemanticType type, Map<String, Object> params);

    Action deriveAction(
            List<String> semanticIds, Action.ActionType type, Map<String, Object> params);

    ActionPlan deriveActionPlan(
            List<String> semanticIds, PlanType planType, Map<String, Object> params);

    ActionResult execute(String actionId, ExecutionContext context);

    PlanResult executePlan(String planId, ExecutionContext context);

    CognitiveAssetMetric cognitiveAssetMetric();

    CognitiveAssetMetric incrementalAssetUpdate(String viewId);

    enum PlanType {
        CHAIN,
        DAG
    }

    record ActionResult(
            String actionId,
            boolean success,
            String output,
            Map<String, Object> artifacts,
            long timestamp) {}

    record PlanResult(
            String planId,
            boolean success,
            List<ActionResult> stepResults,
            String summary,
            long timestamp) {}
}
