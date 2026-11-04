package io.github.rawwise.engine.compaction.merger;

import java.util.Comparator;
import java.util.List;

public class VersionSelectMergeFunction implements MergeFunction {

    private final Comparator<VersionedValue> comparator;

    public VersionSelectMergeFunction(Comparator<VersionedValue> comparator) {
        this.comparator = comparator;
    }

    @Override
    public byte[] merge(byte[] key, List<VersionedValue> versionedValues) {
        return versionedValues.stream()
                .filter(v -> !v.isTombstone())
                .max(comparator)
                .map(VersionedValue::value)
                .orElse(null);
    }
}
