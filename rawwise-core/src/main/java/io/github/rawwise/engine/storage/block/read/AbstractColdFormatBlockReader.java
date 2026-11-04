package io.github.rawwise.engine.storage.block.read;

import io.github.rawwise.engine.storage.QueryCapability;
import io.github.rawwise.engine.storage.block.ColdDataBlock;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.metrics.ReaderErrorType;
import io.github.rawwise.engine.storage.metrics.ReaderMetrics;
import io.github.rawwise.engine.storage.reader.AbstractReader;
import io.github.rawwise.model.op.Find;
import io.github.rawwise.model.op.Scan;
import java.io.IOException;
import java.util.List;
import java.util.Set;

abstract class AbstractColdFormatBlockReader extends AbstractReader implements DataBlockReader {
    private final DataBlock.Format fileFormat;

    protected AbstractColdFormatBlockReader(DataBlock.Format fileFormat) {
        this.fileFormat = fileFormat;
    }

    protected AbstractColdFormatBlockReader(DataBlock.Format fileFormat, ReaderMetrics metrics) {
        super(metrics);
        this.fileFormat = fileFormat;
    }

    @Override
    public boolean supports(DataBlock block) {
        return block instanceof ColdDataBlock coldBlock
                && coldBlock.layer() == DataBlock.Layer.COLD
                && coldBlock.fileFormat() == fileFormat;
    }

    @Override
    public final Find.Result find(Find find, List<DataBlock> blocks) throws IOException {
        ensureOpen();
        long startTime = System.nanoTime();
        try {
            List<ColdDataBlock> visibleBlocks =
                    ColdBlockFilters.visibleBlocks(blocks, find.snapshotId(), find.filter());
            if (visibleBlocks.isEmpty()) {
                return Find.Result.notFound();
            }
            return findVisible(find, visibleBlocks);
        } catch (IOException e) {
            metrics().recordError(ReaderErrorType.CORRUPTION, e, "find");
            throw e;
        } finally {
            metrics().recordRead("find", 0, blocks.size(), System.nanoTime() - startTime);
        }
    }

    @Override
    public final Scan.Result scan(Scan scan, List<DataBlock> blocks) throws IOException {
        ensureOpen();
        long startTime = System.nanoTime();
        try {
            List<ColdDataBlock> visibleBlocks =
                    ColdBlockFilters.visibleBlocks(
                            blocks,
                            ColdBlockFilters.requestedSnapshotId(scan),
                            scan.getPushedPredicate());
            if (visibleBlocks.isEmpty()) {
                return Scan.Result.ok(List.of(), false);
            }
            return scanVisible(scan, visibleBlocks);
        } catch (IOException e) {
            metrics().recordError(ReaderErrorType.CORRUPTION, e, "scan");
            throw e;
        } finally {
            metrics().recordRead("scan", 0, blocks.size(), System.nanoTime() - startTime);
        }
    }

    @Override
    public Set<QueryCapability> capabilities() {
        return Set.of(
                QueryCapability.POINT_LOOKUP,
                QueryCapability.RANGE_SCAN,
                QueryCapability.PREDICATE_PUSHDOWN,
                QueryCapability.COLUMN_PROJECTION,
                QueryCapability.STATISTICS_BASED_PRUNING,
                QueryCapability.LIMIT_PUSHDOWN,
                QueryCapability.PARTITION_PRUNING);
    }

    protected abstract Find.Result findVisible(Find find, List<ColdDataBlock> blocks)
            throws IOException;

    protected abstract Scan.Result scanVisible(Scan scan, List<ColdDataBlock> blocks)
            throws IOException;
}
