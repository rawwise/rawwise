package io.github.rawwise.engine.storage.manifest;

public interface CommitResultLog {
    void record(String tableId, long txnId, long snapshotId);

    long getMaxSnapshot(String tableId, long txnId);
}
