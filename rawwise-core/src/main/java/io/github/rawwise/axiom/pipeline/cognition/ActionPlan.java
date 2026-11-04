package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.model.record.Action;
import java.util.List;
import java.util.Map;
import java.util.Set;

public record ActionPlan(
        String planId,
        List<Action> actions,
        CognitionEngine.PlanType planType,
        Map<String, Set<String>> dependencies) {}
