package io.github.rawwise.engine.compaction.merger;

import java.util.Comparator;
import java.util.List;

public class DeleteTombstoneMergeFunction implements MergeFunction {

    private final MergeFunction inner;

    public DeleteTombstoneMergeFunction(MergeFunction inner) {
        this.inner = inner;
    }

    @Override
    public byte[] merge(byte[] key, List<VersionedValue> versionedValues) {
        VersionedValue latestTombstone =
                versionedValues.stream()
                        .filter(VersionedValue::isTombstone)
                        .max(Comparator.comparingLong(VersionedValue::sequenceNumber))
                        .orElse(null);

        VersionedValue latestValue =
                versionedValues.stream()
                        .filter(v -> !v.isTombstone())
                        .max(Comparator.comparingLong(VersionedValue::sequenceNumber))
                        .orElse(null);

        if (latestTombstone != null
                && (latestValue == null
                        || latestTombstone.sequenceNumber() > latestValue.sequenceNumber())) {
            return null;
        }
        return inner.merge(key, versionedValues.stream().filter(v -> !v.isTombstone()).toList());
    }
}
