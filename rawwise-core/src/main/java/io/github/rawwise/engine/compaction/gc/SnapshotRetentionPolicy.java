package io.github.rawwise.engine.compaction.gc;

import io.github.rawwise.model.record.Snapshot;
import java.util.List;

public interface SnapshotRetentionPolicy {

    List<Snapshot> retain(List<Snapshot> snapshots);
}
