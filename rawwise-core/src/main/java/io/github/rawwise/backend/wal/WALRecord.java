package io.github.rawwise.backend.wal;

public interface WALRecord {
    long sequence();

    String marker();

    byte[] value();

    long timestamp();

    long commitId();

    String uniqTableIdentifier();

    static WALRecord createSyncMarker(String tableIdentifier) {
        return new DefaultWALRecord(
                -1, new byte[0], System.nanoTime(), -1L, tableIdentifier, "SYNC_MARKER");
    }
}
