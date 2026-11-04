package io.github.rawwise.backend.read;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReadOptimizerChain {

    private final List<ReadOptimizer> optimizers;

    public ReadOptimizerChain() {
        this.optimizers = new ArrayList<>();
        this.optimizers.add(new SmallPartitionMergeOptimizer());
        this.optimizers.add(new LocalityOptimizer());
        this.optimizers.sort(Comparator.comparingInt(ReadOptimizer::priority).reversed());
    }

    public ReadOptimizerChain(List<ReadOptimizer> optimizers) {
        this.optimizers = new ArrayList<>(optimizers);
        this.optimizers.sort(Comparator.comparingInt(ReadOptimizer::priority).reversed());
    }

    public ReadPlan optimize(ReadPlan plan) {
        if (plan == null || plan.isEmpty()) {
            return plan;
        }
        ReadPlan current = plan;
        for (ReadOptimizer optimizer : optimizers) {
            try {
                ReadPlan optimized = optimizer.optimize(current);
                if (optimized != null) {
                    current = optimized;
                }
            } catch (Exception e) {
                log.warn("Optimizer {} failed, skipping: {}", optimizer.name(), e.getMessage());
            }
        }
        return current;
    }

    public List<ReadOptimizer> optimizers() {
        return List.copyOf(optimizers);
    }
}
