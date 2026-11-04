package io.github.rawwise.engine.storage.cache;

public record CachedValue(byte[] data, long commitTs) {
    public CachedValue {
        if (data == null) {
            throw new IllegalArgumentException("data must not be null");
        }
        data = data.clone();
    }

    public byte[] getData() {
        return data.clone();
    }

    public boolean isVisibleTo(long snapshotTs) {
        return commitTs <= snapshotTs;
    }
}
