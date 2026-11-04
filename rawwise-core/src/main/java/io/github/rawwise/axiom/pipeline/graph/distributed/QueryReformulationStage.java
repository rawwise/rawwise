package io.github.rawwise.axiom.pipeline.graph.distributed;

import java.util.List;

@FunctionalInterface
public interface QueryReformulationStage {

    List<String> expand(String query, int n);

    static QueryReformulationStage identity() {
        return (query, n) -> List.of(query);
    }
}
