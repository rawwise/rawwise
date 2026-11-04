package io.github.rawwise.frontend.ir.executor;

import io.github.rawwise.engine.storage.ColumnStatistics;
import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.frontend.ir.MetadataQueryIR;
import io.github.rawwise.frontend.ir.MetadataResult;
import io.github.rawwise.frontend.ir.SnapshotStatistics;
import io.github.rawwise.frontend.ir.codec.ColumnStatCodec;
import io.github.rawwise.model.record.Snapshot;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class MetadataQueryExecutor {

    private final SnapshotManager snapshotManager;

    public MetadataQueryExecutor(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }

    public MetadataResult execute(MetadataQueryIR ir) throws IOException {
        long snapshotId = ir.snapshotId();
        if (snapshotId < 0) {
            if (snapshotManager != null) {
                try {
                    snapshotId =
                            snapshotManager
                                    .latestSnapshot(ir.tableMeta().tableIdentifier())
                                    .map(Snapshot::snapshotId)
                                    .orElse(-1L);
                } catch (Exception e) {
                    throw new MetadataExecutionException(
                            "Failed to resolve snapshot for " + ir.tableMeta().tableIdentifier(),
                            e);
                }
            }
            if (snapshotId < 0) {
                throw new MetadataExecutionException(
                        "Cannot resolve snapshot for "
                                + ir.tableMeta().tableIdentifier()
                                + ", metadata-only path requires valid snapshot");
            }
        }
        return executeInternal(ir, snapshotId);
    }

    private MetadataResult executeInternal(MetadataQueryIR ir, long snapshotId) throws IOException {
        String tableId = ir.tableMeta().tableIdentifier();

        return switch (ir) {
            case MetadataQueryIR.CountAll ca -> executeCountAll(tableId, snapshotId);
            case MetadataQueryIR.CountColumn cc ->
                    executeCountColumn(tableId, snapshotId, cc.column());
            case MetadataQueryIR.MinValue mv -> executeMinValue(tableId, snapshotId, mv.column());
            case MetadataQueryIR.MaxValue mx -> executeMaxValue(tableId, snapshotId, mx.column());
            case MetadataQueryIR.TableInfo ti -> executeTableInfo(tableId, snapshotId);
        };
    }

    private MetadataResult executeCountAll(String tableId, long snapshotId) throws IOException {
        SnapshotStatistics stats = snapshotManager.getOrComputeStatistics(tableId, snapshotId);
        return new MetadataResult.ScalarResult("count", stats.effectiveRowCount());
    }

    private MetadataResult executeCountColumn(String tableId, long snapshotId, String column)
            throws IOException {
        List<DataFile> dataFiles = snapshotManager.listDataFiles(tableId, snapshotId);
        long count = 0;
        for (DataFile df : dataFiles) {
            ColumnStatistics cs = df.columnStatistics();
            if (cs == null) continue;
            ColumnStatistics.ColumnStat col = cs.getColumn(column);
            if (col == null) continue;
            count += Math.max(0, col.valueCount() - col.nullCount());
        }
        return new MetadataResult.ScalarResult("count_" + column, count);
    }

    private MetadataResult executeMinValue(String tableId, long snapshotId, String column)
            throws IOException {
        return executeMinMax(tableId, snapshotId, column, true);
    }

    private MetadataResult executeMaxValue(String tableId, long snapshotId, String column)
            throws IOException {
        return executeMinMax(tableId, snapshotId, column, false);
    }

    private MetadataResult executeMinMax(
            String tableId, long snapshotId, String column, boolean isMin) throws IOException {
        List<DataFile> dataFiles = snapshotManager.listDataFiles(tableId, snapshotId);
        Comparable<?> best = null;
        for (DataFile df : dataFiles) {
            ColumnStatistics cs = df.columnStatistics();
            if (cs == null) continue;
            ColumnStatistics.ColumnStat col = cs.getColumn(column);
            if (col == null) continue;
            byte[] raw = isMin ? col.minBytes() : col.maxBytes();
            if (raw == null || raw.length == 0) continue;
            Comparable<?> decoded = ColumnStatCodec.decode(raw, col.logicalType());
            if (decoded == null) continue;
            if (best == null) {
                best = decoded;
                continue;
            }
            @SuppressWarnings("unchecked")
            int cmp = ((Comparable) best).compareTo(decoded);
            if (isMin && cmp > 0) {
                best = decoded;
            } else if (!isMin && cmp < 0) {
                best = decoded;
            }
        }
        if (best == null) {
            throw new MetadataExecutionException(
                    "Column '" + column + "' has no non-null values in table '" + tableId + "'");
        }
        String label = isMin ? "min_" : "max_";
        return new MetadataResult.ScalarResult(label + column, best);
    }

    private MetadataResult executeTableInfo(String tableId, long snapshotId) throws IOException {
        SnapshotStatistics stats = snapshotManager.getOrComputeStatistics(tableId, snapshotId);
        List<DataFile> dataFiles = snapshotManager.listDataFiles(tableId, snapshotId);
        List<KeyRange> keyRanges = new ArrayList<>();
        for (DataFile df : dataFiles) {
            KeyRange kr = df.keyRange();
            if (kr != null) {
                keyRanges.add(kr);
            }
        }
        return new MetadataResult.TableInfoResult(
                stats.totalRows(), stats.totalBytes(), stats.fileCount(), keyRanges);
    }
}
