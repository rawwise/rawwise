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
public class BatchScan extends BaseRead implements Scan {
    private final List<String> projections;

    public BatchScan(
            RawWiseTable table,
            List<DataBlock> dataBlocks,
            Expression pushedPredicate,
            List<String> projections,
            TableReader reader,
            ReadExecutor readExecutor) {
        super(table, dataBlocks, pushedPredicate, false, reader, readExecutor);
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
        // 1. pre-state check
        ensureOpen();

        // 2. verify Data Blocks
        if (dataBlocks == null || dataBlocks.isEmpty()) {
            log.debug(
                    "BatchScan executed with empty DataBlocks for table: {}",
                    table.getTableIdentifier());
            return Scan.Result.ok(List.of(), false);
        }

        try {
            // 3. ✅ Phase 1:delegate to Reader for execution(forbid direct access to Table Store!)
            log.debug(
                    "Executing BatchScan for table: {}, blocks: {}, projections: {}",
                    table.getTableIdentifier(),
                    dataBlocks.size(),
                    projections);

            Scan.Result result = (Scan.Result) executeRead(this);

            // 4. log results
            if (result.success()) {
                log.info(
                        "BatchScan completed successfully for table: {}, records: {}",
                        table.getTableIdentifier(),
                        result.recordCount());
            } else {
                log.error(
                        "BatchScan failed for table: {}, error: {}",
                        table.getTableIdentifier(),
                        result.message().orElse("Unknown error"));
            }

            return result;

        } catch (IOException e) {
            log.error("IO error during BatchScan for table: {}", table.getTableIdentifier(), e);
            return Scan.Result.error("IO error during scan: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error(
                    "Unexpected error during BatchScan for table: {}",
                    table.getTableIdentifier(),
                    e);
            return Scan.Result.error("Unexpected scan error: " + e.getMessage(), e);
        }
    }

    @Override
    public OpType type() {
        return OpType.BATCH_SCAN;
    }

    @Override
    public List<String> projections() {
        return projections;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements ReadBuilder<BatchScan, Builder> {
        private RawWiseTable table;
        private List<DataBlock> dataBlocks;
        private Expression pushedPredicate;
        private List<String> projections;
        private TableReader reader; // Phase 1: added Reader injection
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

        public BatchScan build() {
            Objects.requireNonNull(table, "table is required");
            if (readExecutor == null) {
                Objects.requireNonNull(
                        reader, "reader is required - use withReader() to inject TableReader");
            }

            log.debug(
                    "Building BatchScan: table={}, blocks={}, hasReader={}, hasReadExecutor={}",
                    table.getTableIdentifier(),
                    dataBlocks != null ? dataBlocks.size() : "null",
                    reader != null,
                    readExecutor != null);

            return new BatchScan(
                    table, dataBlocks, pushedPredicate, projections, reader, readExecutor);
        }
    }
}
