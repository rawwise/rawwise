package io.github.rawwise.backend.read;

import io.github.rawwise.model.expression.Expression;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ReadPlan {

    private final List<ReadPartition> partitions;
    private final Expression predicate;
    private final boolean skipScanWithRowGeneration;
    private final List<java.util.Map<String, Comparable<?>>> generatedRows;

    private ReadPlan(Builder builder) {
        this.partitions = Collections.unmodifiableList(new ArrayList<>(builder.partitions));
        this.predicate = builder.predicate;
        this.skipScanWithRowGeneration = builder.skipScanWithRowGeneration;
        this.generatedRows =
                builder.generatedRows != null
                        ? Collections.unmodifiableList(new ArrayList<>(builder.generatedRows))
                        : null;
    }

    public List<ReadPartition> partitions() {
        return partitions;
    }

    public Expression predicate() {
        return predicate;
    }

    public boolean skipScanWithRowGeneration() {
        return skipScanWithRowGeneration;
    }

    public List<java.util.Map<String, Comparable<?>>> generatedRows() {
        return generatedRows;
    }

    public int partitionCount() {
        return partitions.size();
    }

    public long totalBlockCount() {
        return partitions.stream().mapToLong(ReadPartition::blockCount).sum();
    }

    public boolean isEmpty() {
        return !skipScanWithRowGeneration && partitions.isEmpty();
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        Builder b = new Builder();
        b.partitions.addAll(this.partitions);
        b.predicate = this.predicate;
        b.skipScanWithRowGeneration = this.skipScanWithRowGeneration;
        b.generatedRows = this.generatedRows;
        return b;
    }

    public static class Builder {
        private final List<ReadPartition> partitions = new ArrayList<>();
        private Expression predicate;
        private boolean skipScanWithRowGeneration = false;
        private List<java.util.Map<String, Comparable<?>>> generatedRows;

        private Builder() {}

        public Builder addPartition(ReadPartition partition) {
            this.partitions.add(partition);
            return this;
        }

        public Builder partitions(List<ReadPartition> partitions) {
            this.partitions.clear();
            this.partitions.addAll(partitions);
            return this;
        }

        public Builder predicate(Expression predicate) {
            this.predicate = predicate;
            return this;
        }

        public Builder skipScanWithRowGeneration(boolean skip) {
            this.skipScanWithRowGeneration = skip;
            return this;
        }

        public Builder generatedRows(List<java.util.Map<String, Comparable<?>>> rows) {
            this.generatedRows = rows;
            return this;
        }

        public ReadPlan build() {
            return new ReadPlan(this);
        }
    }
}
