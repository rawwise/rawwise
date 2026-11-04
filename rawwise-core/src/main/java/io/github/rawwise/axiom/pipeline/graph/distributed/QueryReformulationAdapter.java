package io.github.rawwise.axiom.pipeline.graph.distributed;

import io.github.rawwise.axiom.pipeline.reformulation.DefaultQueryReformulationStage;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryReformulationAdapter implements QueryReformulationStage {

    private final DefaultQueryReformulationStage delegate;

    public QueryReformulationAdapter(DefaultQueryReformulationStage delegate) {
        this.delegate = delegate;
    }

    @Override
    public List<String> expand(String query, int n) {
        if (delegate == null) {
            return List.of(query);
        }
        try {
            return delegate.expand(query, n);
        } catch (RuntimeException e) {
            log.warn(
                    "QueryReformulationAdapter: delegate.expand failed for query='{}': {}",
                    query,
                    e.getMessage());
            return List.of(query);
        }
    }
}
