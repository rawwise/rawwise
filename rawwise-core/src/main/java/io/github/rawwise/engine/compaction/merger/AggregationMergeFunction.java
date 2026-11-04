package io.github.rawwise.engine.compaction.merger;

import java.util.List;

public class AggregationMergeFunction implements MergeFunction {

    public enum AggregationOp {
        SUM,
        MAX,
        MIN,
        COUNT
    }

    private final String columnName;
    private final AggregationOp op;

    public AggregationMergeFunction(String columnName, AggregationOp op) {
        this.columnName = columnName;
        this.op = op;
    }

    @Override
    public byte[] merge(byte[] key, List<VersionedValue> versionedValues) {
        throw new UnsupportedOperationException(
                "AggregationMergeFunction.merge not yet implemented (col="
                        + columnName
                        + ", op="
                        + op
                        + ")");
    }
}
