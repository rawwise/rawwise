package io.github.rawwise.engine.storage.block.read;

import io.github.rawwise.engine.storage.QueryCapability;
import io.github.rawwise.engine.storage.block.ColdDataBlock;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.reader.AbstractReader;
import io.github.rawwise.model.op.Find;
import io.github.rawwise.model.op.Scan;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class ColdDataBlockReader extends AbstractReader implements DataBlockReader {
    private final List<DataBlockReader> readers;

    public ColdDataBlockReader(List<DataBlockReader> readers) {
        this.readers = List.copyOf(readers);
    }

    @Override
    public boolean supports(DataBlock block) {
        return block instanceof ColdDataBlock;
    }

    @Override
    public Find.Result find(Find find, List<DataBlock> blocks) throws IOException {
        ensureOpen();
        List<ColdDataBlock> visibleBlocks =
                ColdBlockFilters.visibleBlocks(blocks, find.snapshotId(), find.filter());
        if (visibleBlocks.isEmpty()) {
            return Find.Result.notFound();
        }
        boolean supported = false;
        for (DataBlockReader reader : readers) {
            List<DataBlock> routed =
                    visibleBlocks.stream()
                            .filter(reader::supports)
                            .map(DataBlock.class::cast)
                            .toList();
            if (routed.isEmpty()) {
                continue;
            }
            supported = true;
            Find.Result result = reader.find(find, routed);
            if (!result.success()) {
                return result;
            }
            if (result.foundRecord().isPresent()) {
                return result;
            }
        }
        if (supported) {
            return Find.Result.notFound();
        }
        return Find.Result.error("No available cold reader for requested file format", null);
    }

    @Override
    public Scan.Result scan(Scan scan, List<DataBlock> blocks) throws IOException {
        ensureOpen();
        List<ColdDataBlock> visibleBlocks =
                ColdBlockFilters.visibleBlocks(
                        blocks,
                        ColdBlockFilters.requestedSnapshotId(scan),
                        scan.getPushedPredicate());
        if (visibleBlocks.isEmpty()) {
            return Scan.Result.ok(List.of(), false);
        }
        boolean supported = false;
        for (DataBlockReader reader : readers) {
            List<DataBlock> routed =
                    visibleBlocks.stream()
                            .filter(reader::supports)
                            .map(DataBlock.class::cast)
                            .toList();
            if (routed.isEmpty()) {
                continue;
            }
            supported = true;
            Scan.Result result = reader.scan(scan, routed);
            if (!result.success()) {
                return result;
            }
            if (!result.data().isEmpty()) {
                return result;
            }
        }
        if (supported) {
            return Scan.Result.ok(List.of(), false);
        }
        return Scan.Result.error("No available cold reader for requested file format", null);
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
}
