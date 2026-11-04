package io.github.rawwise.model.op;

import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.table.read.TableReader;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.record.RawWiseTable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KvScan extends BaseRead implements Scan {
    private final long snapshotId;
    private final KeyRange keyRange;
    private final List<String> projections;

    public KvScan(
            RawWiseTable table,
            List<DataBlock> dataBlocks,
            Expression pushedPredicate,
            long snapshotId,
            KeyRange keyRange,
            List<String> projections,
            TableReader reader,
            ReadExecutor readExecutor) {
        super(table, dataBlocks, pushedPredicate, true, reader, readExecutor);
        this.snapshotId = snapshotId;
        this.keyRange = keyRange;
        this.projections = projections != null ? List.copyOf(projections) : List.of();
    }

    @Override
    public List<DataBlock> plan() {
        return getDataBlocks();
    }

    @Override
    public Expression getPushedPredicate() {
        return pushedPredicate;
    }

    @Override
    public Scan.Result exec() {
        ensureOpen();

        try {
            log.debug(
                    "Executing KvScan for table: {}, keyRange: {}, projections: {}",
                    table.getTableIdentifier(),
                    keyRange,
                    projections);

            Scan.Result result = (Scan.Result) executeRead(this);

            if (result.success()) {
                log.info(
                        "KvScan completed for table: {}, records: {}",
                        table.getTableIdentifier(),
                        result.recordCount());
            } else {
                log.error(
                        "KvScan failed for table: {}, error: {}",
                        table.getTableIdentifier(),
                        result.message().orElse("Unknown error"));
            }

            return result;

        } catch (IOException e) {
            log.error("IO error during KvScan for table: {}", table.getTableIdentifier(), e);
            return Scan.Result.error("IO error during kvscan: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error(
                    "Unexpected error during KvScan for table: {}", table.getTableIdentifier(), e);
            return Scan.Result.error("Unexpected kvscan error: " + e.getMessage(), e);
        }
    }

    @Override
    public OpType type() {
        return OpType.KV_SCAN;
    }

    public long snapshotId() {
        return snapshotId;
    }

    @Override
    public KeyRange keyRange() {
        return keyRange;
    }

    @Override
    public List<String> projections() {
        return projections;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements ReadBuilder<KvScan, Builder> {
        private RawWiseTable table;
        private List<DataBlock> dataBlocks;
        private Expression pushedPredicate;
        private long snapshotId = -1;
        private KeyRange keyRange;
        private List<String> projections;
        private TableReader reader;
        private ReadExecutor readExecutor;

        @Override
        public Builder withTable(RawWiseTable table) {
            this.table = table;
            return this;
        }

        @Override
        public Builder withDataBlocks(List<DataBlock> dataBlocks) {
            this.dataBlocks = dataBlocks;
            return this;
        }

        @Override
        public Builder withPredicate(Expression predicate) {
            this.pushedPredicate = predicate;
            return this;
        }

        @Override
        public RawWiseTable table() {
            return table;
        }

        public Builder withSnapshotId(long snapshotId) {
            this.snapshotId = snapshotId;
            return this;
        }

        public Builder withKeyRange(KeyRange keyRange) {
            this.keyRange = keyRange;
            return this;
        }

        public Builder withProjections(List<String> projections) {
            this.projections = projections;
            return this;
        }

        public Builder withReader(TableReader reader) {
            this.reader = Objects.requireNonNull(reader, "reader cannot be null");
            return this;
        }

        public Builder withReadExecutor(ReadExecutor readExecutor) {
            this.readExecutor = Objects.requireNonNull(readExecutor, "readExecutor cannot be null");
            return this;
        }

        public KvScan build() {
            Objects.requireNonNull(table, "table is required");
            if (readExecutor == null) {
                Objects.requireNonNull(
                        reader, "reader is required - use withReader() to inject TableReader");
            }

            log.debug(
                    "Building KvScan: table={}, hasReader={}, hasReadExecutor={}",
                    table.getTableIdentifier(),
                    reader != null,
                    readExecutor != null);

            return new KvScan(
                    table,
                    dataBlocks,
                    pushedPredicate,
                    snapshotId,
                    keyRange,
                    projections,
                    reader,
                    readExecutor);
        }
    }
}
