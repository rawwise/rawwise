package io.github.rawwise.engine.txn;

import io.github.rawwise.model.record.DynamicTableInfo;
import io.github.rawwise.model.record.RawWiseTable;
import io.github.rawwise.model.record.Snapshot;
import io.github.rawwise.model.record.TableMeta;
import java.util.List;

/**
 * Lightweight RawWiseTable for internal write paths where only the uniqId is meaningful. Used by
 * DynamicMmapStore for write operations instead of private inner classes.
 *
 * <p>All methods except {@link #uniqId()} return defaults/null. This is intentional: the write path
 * only needs the table identifier for routing, not full metadata.
 *
 * @param uniqId the table identifier used for routing
 */
public record SimpleTableRef(String uniqId) implements RawWiseTable {
    @Override
    public void close() {}

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void refresh() {}

    @Override
    public TableMeta meta() {
        throw new UnsupportedOperationException("SimpleTableRef does not support meta()");
    }

    @Override
    public DynamicTableInfo dynamicInfo() {
        throw new UnsupportedOperationException("SimpleTableRef does not support dynamicInfo()");
    }

    @Override
    public Snapshot currentSnapshot() {
        throw new UnsupportedOperationException(
                "SimpleTableRef does not support currentSnapshot()");
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        throw new UnsupportedOperationException("SimpleTableRef does not support snapshot(long)");
    }

    @Override
    public List<Snapshot> snapshots() {
        return List.of();
    }
}
