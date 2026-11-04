package io.github.rawwise.axiom.pipeline.cognition;

@FunctionalInterface
public interface TextConditionEnhancer {
    TextConditionEnhancement enhance(String textCondition, TextConditionEnhancementContext context);

    static TextConditionEnhancer noop() {
        return (textCondition, context) -> TextConditionEnhancement.unchanged(textCondition);
    }
}
