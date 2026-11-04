package io.github.rawwise.engine.compaction.gc;

import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.model.record.Snapshot;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SnapshotRefCollector {

    private final SnapshotManager snapshotManager;
    private final SnapshotRetentionPolicy retentionPolicy;

    public SnapshotRefCollector(
            SnapshotManager snapshotManager, SnapshotRetentionPolicy retentionPolicy) {
        this.snapshotManager = snapshotManager;
        this.retentionPolicy = retentionPolicy;
    }

    public Set<String> buildLiveFileSet(String tableId) throws IOException {
        List<Snapshot> allSnapshots = snapshotManager.getSnapshots(tableId);
        List<Snapshot> retained = retentionPolicy.retain(allSnapshots);

        Set<String> liveFiles = new HashSet<>();
        for (Snapshot s : retained) {
            try {
                List<DataFile> dataFiles = snapshotManager.listDataFiles(tableId, s.snapshotId());
                for (DataFile df : dataFiles) {
                    liveFiles.add(df.filePath());
                }
            } catch (IOException e) {
                log.warn(
                        "Failed to list data files for snapshot {} in table {}, skipping",
                        s.snapshotId(),
                        tableId,
                        e);
            }
        }

        log.debug(
                "Live file set for table {}: {} snapshots retained (of {}), {} live files",
                tableId,
                retained.size(),
                allSnapshots.size(),
                liveFiles.size());
        return liveFiles;
    }
}
