package io.github.rawwise.engine.storage.manifest;

import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.LogFile;
import io.github.rawwise.model.record.Snapshot;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface SnapshotOperations {
    Optional<Snapshot> latestSnapshot(String tableId) throws IOException;

    List<DataFile> listDataFiles(String tableId, long snapshotId) throws IOException;

    List<Snapshot> getSnapshots(String tableId) throws IOException;

    List<Snapshot> getRecentSnapshots(String tableId, int count) throws IOException;

    Snapshot commitSnapshot(
            String tableId,
            List<DataFile> newDataFiles,
            List<LogFile> newLogFiles,
            long minSeq,
            long maxSeq)
            throws SnapshotCommitConflictException, IOException;
}
