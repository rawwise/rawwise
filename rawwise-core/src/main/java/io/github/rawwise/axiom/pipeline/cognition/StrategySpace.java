package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.model.record.StrategyAction;
import java.util.List;
import java.util.Map;

/**
 * strategy - GP-UCB implementationinternal. textstrategymappingparametervector R^d,distance.
 *
 * <p>Strategy Space GP-UCB implementationinternal,global. Strategy Action (text strategy Content).
 */
public interface StrategySpace {

    List<StrategyParam> params();

    StrategyAction materialize(Map<String, Object> paramValues);

    double[] toVector(StrategyAction action);

    int dimension();
}
