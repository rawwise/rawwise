package io.github.rawwise.model.op;

import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.table.read.TableReader;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.record.RawWiseTable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseRead implements Read {

    protected final RawWiseTable table;

    protected final List<DataBlock> dataBlocks;

    protected final Expression pushedPredicate;

    protected final boolean primaryKeyLookup;

    protected final TableReader reader;

    protected final ReadExecutor readExecutor;

    protected BaseRead(
            RawWiseTable table,
            List<DataBlock> dataBlocks,
            Expression pushedPredicate,
            boolean primaryKeyLookup,
            TableReader reader) {
        this(table, dataBlocks, pushedPredicate, primaryKeyLookup, reader, null);
    }

    protected BaseRead(
            RawWiseTable table,
            List<DataBlock> dataBlocks,
            Expression pushedPredicate,
            boolean primaryKeyLookup,
            TableReader reader,
            ReadExecutor readExecutor) {
        this.table = Objects.requireNonNull(table, "table cannot be null");
        this.dataBlocks = dataBlocks;
        this.pushedPredicate = pushedPredicate;
        this.primaryKeyLookup = primaryKeyLookup;
        this.reader = reader;
        this.readExecutor = readExecutor;
        if (reader == null && readExecutor == null) {
            throw new IllegalArgumentException("Either reader or readExecutor must be provided");
        }
    }

    public RawWiseTable getTable() {
        return table;
    }

    public TableReader getReader() {
        return reader;
    }

    @Override
    public List<DataBlock> getDataBlocks() {
        return dataBlocks != null ? dataBlocks : List.of();
    }

    @Override
    public long estimatedSize() {
        return dataBlocks != null ? dataBlocks.stream().mapToLong(DataBlock::getSize).sum() : 0L;
    }

    @Override
    public Expression getPushedPredicate() {
        return pushedPredicate;
    }

    @Override
    public boolean isPrimaryKeyLookup() {
        return primaryKeyLookup;
    }

    protected void ensureOpen() {
        if (table.isClosed()) {
            throw new IllegalStateException(
                    "Table is closed: "
                            + table.getTableIdentifier()
                            + " (operation: "
                            + this.getClass().getSimpleName()
                            + ")");
        }
        if (log.isDebugEnabled()) {
            log.debug(
                    "Table {} is open for operation: {}",
                    table.getTableIdentifier(),
                    this.getClass().getSimpleName());
        }
    }

    protected Read.Result executeRead(Read read) throws IOException {
        if (readExecutor != null) {
            return readExecutor.execute(read);
        }
        if (read instanceof Find f) {
            return reader.find(f);
        }
        if (read instanceof Scan s) {
            return reader.scan(s);
        }
        throw new IllegalStateException(
                "No ReadExecutor and unsupported Read type: " + read.getClass().getName());
    }
}
