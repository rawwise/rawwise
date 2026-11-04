package io.github.rawwise.engine.compaction.merger;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class TTLFilterMergeFunction implements MergeFunction {

    private final MergeFunction delegate;
    private final long expiryThresholdMs;
    private final TimestampExtractor timestampExtractor;

    public TTLFilterMergeFunction(
            MergeFunction delegate, long expiryThresholdMs, TimestampExtractor timestampExtractor) {
        this.delegate = delegate;
        this.expiryThresholdMs = expiryThresholdMs;
        this.timestampExtractor =
                Objects.requireNonNull(timestampExtractor, "timestampExtractor must not be null");
    }

    @Override
    public byte[] merge(byte[] key, List<VersionedValue> versionedValues) {
        if (key == null) return null;
        if (versionedValues == null || versionedValues.isEmpty()) return null;

        VersionedValue latest =
                versionedValues.stream()
                        .max(Comparator.comparingLong(VersionedValue::sequenceNumber))
                        .orElse(null);
        if (latest == null) return null;
        if (latest.isTombstone()) return null;

        long timestamp = timestampExtractor.extractTimestampMillis(key, latest.value());
        if (timestamp < expiryThresholdMs) return null;

        if (delegate != null) return delegate.merge(key, versionedValues);
        return latest.value();
    }
}
