package io.github.rawwise.engine.compaction.merger;

import java.util.List;

public class CompositeMergeFunction implements MergeFunction {

    private final List<MergeFunction> pipeline;

    public CompositeMergeFunction(List<MergeFunction> pipeline) {
        this.pipeline = List.copyOf(pipeline);
    }

    @Override
    public byte[] merge(byte[] key, List<VersionedValue> versionedValues) {
        List<VersionedValue> current = versionedValues;
        for (MergeFunction fn : pipeline) {
            byte[] result = fn.merge(key, current);
            if (result == null) {
                return null;
            }
            current = List.of(VersionedValue.mutable(result, Long.MAX_VALUE, 0, false));
        }
        return current.get(0).value();
    }
}
