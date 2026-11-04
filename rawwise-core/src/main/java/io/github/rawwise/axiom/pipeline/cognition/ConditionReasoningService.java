package io.github.rawwise.axiom.pipeline.cognition;

/**
 * condition SPI.
 *
 * <p>retrieval,,information,via LLM generationcondition. --generation,generation,text.
 *
 * <p>defaultimplementation:{@link #noop()} returncondition,. LLM implementation:{@link Llm
 * Condition Reasoning Service} via LLM condition.
 */
public interface ConditionReasoningService {

    /**
     * contextcondition.
     *
     * @param context context(condition,,,information)
     * @return (condition + + ID)
     */
    ConditionReasoningResult reason(ConditionReasoningContext context);

    /** (LLM endpoint configcorrectreturn true). */
    boolean isAvailable();

    /** default noop implementation:condition. */
    static ConditionReasoningService noop() {
        return new ConditionReasoningService() {
            @Override
            public ConditionReasoningResult reason(ConditionReasoningContext context) {
                return new ConditionReasoningResult(
                        context.originalCondition(),
                        context.originalCondition(),
                        java.util.List.of(),
                        "noop");
            }

            @Override
            public boolean isAvailable() {
                return false;
            }
        };
    }
}
