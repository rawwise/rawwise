package io.github.rawwise.engine.storage.manifest;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryCommitResultLog implements CommitResultLog {
    private final Map<String, Long> log = new ConcurrentHashMap<>();

    @Override
    public void record(String tableId, long txnId, long snapshotId) {
        log.put(key(tableId, txnId), snapshotId);
    }

    @Override
    public long getMaxSnapshot(String tableId, long txnId) {
        return log.getOrDefault(key(tableId, txnId), -1L);
    }

    private static String key(String tableId, long txnId) {
        return tableId + "/" + txnId;
    }
}
