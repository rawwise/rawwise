package io.github.rawwise.engine.compaction.merger;

import io.github.rawwise.model.record.ImmutabilitySource;

public record VersionedValue(
        byte[] value,
        long sequenceNumber,
        long commitTimestamp,
        boolean isTombstone,
        boolean immutable,
        ImmutabilitySource immutableSource) {
    public VersionedValue {
        java.util.Objects.requireNonNull(value, "value must not be null");
    }

    public static VersionedValue mutable(byte[] value, long seq, long ts, boolean tombstone) {
        return new VersionedValue(value, seq, ts, tombstone, false, ImmutabilitySource.MUTABLE);
    }
}
