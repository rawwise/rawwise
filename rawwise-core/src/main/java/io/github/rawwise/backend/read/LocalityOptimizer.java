package io.github.rawwise.backend.read;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class LocalityOptimizer implements ReadOptimizer {

    @Override
    public String name() {
        return "Locality";
    }

    @Override
    public int priority() {
        return 200;
    }

    @Override
    public ReadPlan optimize(ReadPlan plan) {
        if (plan.skipScanWithRowGeneration() || plan.partitions().size() <= 1) {
            return plan;
        }

        List<ReadPartition> sorted = new ArrayList<>(plan.partitions());
        sorted.sort(
                Comparator.comparing(ReadPartition::hasPreferredHost)
                        .reversed()
                        .thenComparing(p -> p.primaryHost() != null ? p.primaryHost() : "\uffff"));

        return plan.toBuilder().partitions(sorted).build();
    }
}
