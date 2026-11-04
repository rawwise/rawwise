package io.github.rawwise.engine.compaction.merger;

import java.util.Comparator;
import java.util.List;

public class LatestWinMergeFunction implements MergeFunction {

    @Override
    public byte[] merge(byte[] key, List<VersionedValue> versionedValues) {
        return versionedValues.stream()
                .filter(v -> !v.isTombstone())
                .max(Comparator.comparingLong(VersionedValue::sequenceNumber))
                .map(VersionedValue::value)
                .orElse(null);
    }
}
