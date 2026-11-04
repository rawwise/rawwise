package io.github.rawwise.engine.storage.manifest;

import io.github.rawwise.engine.storage.DataFile;
import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ManifestCompactor {

    private static final int DEFAULT_MANIFEST_COMPACTION_THRESHOLD = 50;

    private final SnapshotManager snapshotManager;
    private final int threshold;

    public ManifestCompactor(SnapshotManager snapshotManager) {
        this(snapshotManager, DEFAULT_MANIFEST_COMPACTION_THRESHOLD);
    }

    public ManifestCompactor(SnapshotManager snapshotManager, int threshold) {
        this.snapshotManager = snapshotManager;
        this.threshold = threshold;
    }

    public boolean shouldCompact(String tableId) throws IOException {
        return snapshotManager
                .latestSnapshot(tableId)
                .filter(s -> s.manifestList() != null && !s.manifestList().isEmpty())
                .map(s -> s.manifestList().size() > threshold)
                .orElse(false);
    }

    public int compact(String tableId) throws IOException {
        return snapshotManager
                .latestSnapshot(tableId)
                .filter(s -> s.manifestList() != null && !s.manifestList().isEmpty())
                .map(
                        s -> {
                            List<String> mids = s.manifestList();
                            if (mids.size() <= threshold) {
                                return 0;
                            }
                            try {
                                List<DataFile> allFiles =
                                        snapshotManager.listDataFiles(tableId, s.snapshotId());
                                long minSeq =
                                        allFiles.stream()
                                                .mapToLong(DataFile::minSequenceNumber)
                                                .min()
                                                .orElse(0L);
                                long maxSeq =
                                        allFiles.stream()
                                                .mapToLong(DataFile::maxSequenceNumber)
                                                .max()
                                                .orElse(0L);
                                snapshotManager.commitCompactionSnapshot(
                                        tableId, List.of(), allFiles, minSeq, maxSeq);
                                log.info(
                                        "Manifest compaction for table {}: {} manifests consolidated, {} data files",
                                        tableId,
                                        mids.size(),
                                        allFiles.size());
                                return mids.size();
                            } catch (IOException e) {
                                log.error("Manifest compaction failed for table {}", tableId, e);
                                return 0;
                            }
                        })
                .orElse(0);
    }

    public int threshold() {
        return threshold;
    }
}
