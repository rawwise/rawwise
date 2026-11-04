package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.engine.storage.KnowledgeStore;

/**
 * cognition - produce cognitive decisions when feedback arrives. eventloop, Feedback Loop Closer.on
 * Annotation() async, Cognitive Decision persist Knowledge Store, Condition Generation
 * Service.consistency.
 *
 * <p>(Strategy Learning Service + Condition Generation Service):
 *
 * <ul>
 *   <li>Strategy Learning Service "strategy"(ΔM),""
 *   <li>Cognition Driver EXPLORE/EXPLOIT/TERMINATE,managementstatus
 *   <li>Effect Evaluator SNR estimation,"stop"()
 *   <li>Cognitive State persistprocessrecovery
 * </ul>
 */
public interface CognitionDriver {

    CognitiveDecision nextStep(CognitiveState state);

    void observe(CognitiveDecision decision, ExecutionResult result);

    CognitiveState currentState();

    boolean isAvailable();

    static CognitionDriver noop() {
        return new CognitionDriver() {
            @Override
            public CognitiveDecision nextStep(CognitiveState state) {
                return CognitiveDecision.terminate(
                        "CognitionDriver not available", state.sceneHash());
            }

            @Override
            public void observe(CognitiveDecision decision, ExecutionResult result) {}

            @Override
            public CognitiveState currentState() {
                return CognitiveState.initial("noop", java.util.Map.of());
            }

            @Override
            public boolean isAvailable() {
                return false;
            }
        };
    }

    /**
     * cognitionfactory - LLM.
     *
     * <p>config LLM endpoint, {@link Simple Strategy Optimizer}(ε-greedy) {@link Strategy
     * Optimizer}, {@link Default Cognition Driver} guardrails
     * (MAX_EXPLORE_ROUNDS,STAGNATION_THRESHOLD)-loop. factory:Default Cognition Driver Strategy
     * Optimizer implementation, "".
     *
     * @param optimizer optimization(Simple Strategy Optimizer)
     * @param evaluator (Default Effect Evaluator)
     * @param ks storage,persistcognitionstatus
     * @return Default Cognition Driver instance
     */
    static CognitionDriver ruleBased(
            StrategyOptimizer optimizer, EffectEvaluator evaluator, KnowledgeStore ks) {
        return new DefaultCognitionDriver(optimizer, evaluator, ks);
    }
}
