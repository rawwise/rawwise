package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.model.record.StrategyAction;
import java.util.List;

/** M_t retrievalmatchstrategy. vectorretrieval + match + /filter. */
public interface StrategyRetriever {

    /**
     * retrievalmatchstrategy
     *
     * @param scene Context scenariocontext(scenariodescription +)
     * @param top K return
     * @return sortstrategy
     */
    List<StrategyAction> retrieve(StrategyRetrievalContext sceneContext, int topK);

    /**
     * recordstrategy(thread-safe, CAS)
     *
     * @param strategy Id strategy ID
     * @param effect record
     */
    void recordEffect(String strategyId, StrategyAction.EffectRecord effect);

    /** default noop implementation:retrievalstrategy. */
    static StrategyRetriever noop() {
        return new StrategyRetriever() {
            @Override
            public List<StrategyAction> retrieve(StrategyRetrievalContext sceneContext, int topK) {
                return List.of();
            }

            @Override
            public void recordEffect(String strategyId, StrategyAction.EffectRecord effect) {
                // noop
            }
        };
    }
}
