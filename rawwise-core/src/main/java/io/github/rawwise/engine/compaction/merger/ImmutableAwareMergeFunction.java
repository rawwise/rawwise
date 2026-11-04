package io.github.rawwise.engine.compaction.merger;

import java.util.Comparator;
import java.util.List;

public class ImmutableAwareMergeFunction implements MergeFunction {

    @Override
    public byte[] merge(byte[] key, List<VersionedValue> versionedValues) {
        boolean hasImmutable = versionedValues.stream().anyMatch(VersionedValue::immutable);

        if (hasImmutable) {
            return versionedValues.stream()
                    .filter(VersionedValue::immutable)
                    .max(Comparator.comparingLong(VersionedValue::sequenceNumber))
                    .filter(v -> !v.isTombstone())
                    .map(VersionedValue::value)
                    .orElse(null);
        }

        return versionedValues.stream()
                .filter(v -> !v.isTombstone())
                .max(Comparator.comparingLong(VersionedValue::sequenceNumber))
                .map(VersionedValue::value)
                .orElse(null);
    }
}
