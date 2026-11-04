package io.github.rawwise.engine.storage.merge;

public record RawEntry(byte[] key, byte[] value, long sequenceNumber, String sourceBlockId) {
    public RawEntry {
        key = key == null ? new byte[0] : key.clone();
        value = value == null ? null : value.clone();
    }

    public boolean isTombstone() {
        return value == null;
    }

    public static int compareKey(RawEntry a, RawEntry b) {
        return io.github.rawwise.engine.storage.KeyRange.compare(a.key, b.key);
    }
}
