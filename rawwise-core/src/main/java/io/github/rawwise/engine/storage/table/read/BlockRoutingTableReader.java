package io.github.rawwise.engine.storage.table.read;

import io.github.rawwise.engine.storage.QueryCapability;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.block.index.PrimaryKeyIndex;
import io.github.rawwise.engine.storage.block.read.DataBlockReader;
import io.github.rawwise.engine.storage.reader.AbstractReader;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.model.op.Find;
import io.github.rawwise.model.op.Read;
import io.github.rawwise.model.op.Scan;
import io.github.rawwise.model.record.Record;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class BlockRoutingTableReader extends AbstractReader implements TableReader {
    private final TableStore tableStore;
    private final List<DataBlockReader> readers;

    public BlockRoutingTableReader(TableStore tableStore, List<DataBlockReader> readers) {
        this.tableStore = tableStore;
        this.readers = List.copyOf(readers);
    }

    @Override
    public Read.Result read(Read read) throws IOException {
        ensureOpen();
        if (read instanceof Find find) {
            return find(find);
        }
        if (read instanceof Scan scan) {
            return scan(scan);
        }
        return Scan.Result.error("Unsupported read type: " + read.getClass().getName(), null);
    }

    @Override
    public Find.Result find(Find find) throws IOException {
        ensureOpen();
        List<DataBlock> blocks = plannedBlocks(find.getDataBlocks());
        List<DataBlock> pkFilteredBlocks = filterByPrimaryKeyIndex(find, blocks);
        boolean sawUnsupported = false;
        for (DataBlockReader reader : readers) {
            List<DataBlock> routed = blocksFor(reader, pkFilteredBlocks);
            if (routed.isEmpty()) {
                continue;
            }
            Find.Result result = reader.find(find, routed);
            if (!result.success()) {
                sawUnsupported = true;
                continue;
            }
            if (result.foundRecord().isPresent()) {
                return result;
            }
        }
        if (sawUnsupported) {
            return Find.Result.error("No available reader can complete the find request", null);
        }
        return Find.Result.notFound();
    }

    private List<DataBlock> filterByPrimaryKeyIndex(Find find, List<DataBlock> blocks) {
        if (find.primaryKey().values() == null || find.primaryKey().values().isEmpty()) {
            return blocks;
        }
        PrimaryKeyIndex pkIndex = tableStore.getPrimaryKeyIndex();
        if (pkIndex.isEmpty()) {
            return blocks;
        }
        try {
            byte[] encodedKey =
                    tableStore.getKeyCodec().encodePrimaryKey(find.primaryKey().values());
            List<PrimaryKeyIndex.PKLocation> locations = pkIndex.lookup(encodedKey);
            if (locations.isEmpty()) {
                return blocks;
            }
            Set<String> matchingBlockIds =
                    locations.stream()
                            .map(PrimaryKeyIndex.PKLocation::blockId)
                            .collect(Collectors.toSet());
            return blocks.stream()
                    .filter(b -> matchingBlockIds.contains(b.blockId()))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            return blocks;
        }
    }

    @Override
    public Scan.Result scan(Scan scan) throws IOException {
        ensureOpen();
        List<DataBlock> blocks = plannedBlocks(scan.getDataBlocks());
        List<Record> rows = new ArrayList<>();
        for (DataBlockReader reader : readers) {
            List<DataBlock> routed = blocksFor(reader, blocks);
            if (routed.isEmpty()) {
                continue;
            }
            Scan.Result result = reader.scan(scan, routed);
            if (!result.success()) {
                return result;
            }
            rows.addAll(result.data());
        }
        return Scan.Result.ok(rows, false);
    }

    @Override
    public Set<QueryCapability> capabilities() {
        return Set.of(
                QueryCapability.POINT_LOOKUP,
                QueryCapability.RANGE_SCAN,
                QueryCapability.FULL_READ,
                QueryCapability.PREDICATE_PUSHDOWN,
                QueryCapability.COLUMN_PROJECTION,
                QueryCapability.STATISTICS_BASED_PRUNING,
                QueryCapability.LIMIT_PUSHDOWN,
                QueryCapability.PARTITION_PRUNING);
    }

    @Override
    protected void doClose() throws IOException {
        for (DataBlockReader reader : readers) {
            try {
                reader.close();
            } catch (IOException ignored) {
            }
        }
    }

    private List<DataBlock> plannedBlocks(List<DataBlock> blocks) {
        if (blocks == null || blocks.isEmpty()) {
            return tableStore.currentDataBlocks();
        }
        return blocks;
    }

    private List<DataBlock> blocksFor(DataBlockReader reader, List<DataBlock> blocks) {
        return blocks.stream().filter(reader::supports).toList();
    }
}
