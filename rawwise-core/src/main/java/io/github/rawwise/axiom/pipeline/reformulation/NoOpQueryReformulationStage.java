package io.github.rawwise.axiom.pipeline.reformulation;

import io.github.rawwise.axiom.pipeline.graph.distributed.QueryReformulationStage;
import java.util.List;

public class NoOpQueryReformulationStage implements QueryReformulationStage {

    @Override
    public List<String> expand(String query, int n) {
        return query == null ? List.of() : List.of(query);
    }
}
