package io.github.rawwise.benchmark.vbench.memory;

import io.github.rawwise.axiom.pipeline.cognition.TextConditionEnhancement;
import io.github.rawwise.axiom.pipeline.cognition.TextConditionEnhancementContext;
import io.github.rawwise.axiom.pipeline.cognition.TextConditionEnhancer;

public final class PromptPlannerTextConditionEnhancer implements TextConditionEnhancer {
    private final PromptEnhancementPlanner planner;

    public PromptPlannerTextConditionEnhancer(PromptEnhancementPlanner planner) {
        this.planner = planner;
    }

    @Override
    public TextConditionEnhancement enhance(
            String textCondition, TextConditionEnhancementContext context) {
        if (planner == null) {
            return TextConditionEnhancement.unchanged(textCondition);
        }
        PromptEnhancement enhancement = planner.enhance(textCondition);
        return new TextConditionEnhancement(
                textCondition,
                enhancement.enhancedPrompt(),
                enhancement.constraints(),
                enhancement.sourceMemoryIds());
    }
}
