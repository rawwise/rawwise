package io.github.rawwise.axiom.pipeline.cognition;

import java.util.List;

/** strategyparameter - sealed hierarchy parametertype. */
public sealed interface StrategyParam permits EnumParam, ContinuousParam, OrdinalParam {

    String name();

    int vectorSize();
}

record EnumParam(String name, List<String> values) implements StrategyParam {
    @Override
    public int vectorSize() {
        return values.size();
    }
}

record ContinuousParam(String name, double min, double max) implements StrategyParam {
    @Override
    public int vectorSize() {
        return 1;
    }
}

record OrdinalParam(String name, List<String> orderedValues) implements StrategyParam {
    @Override
    public int vectorSize() {
        return 1;
    }
}
