package io.github.rawwise.model.op;

import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.table.read.TableReader;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.record.PrimaryKeyValue;
import io.github.rawwise.model.record.RawWiseTable;
import io.github.rawwise.model.record.Record;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class Find extends BaseRead {
    private static final OpType TYPE = OpType.FIND;

    private final PrimaryKeyValue primaryKey;
    private final List<String> projection;
    private final long snapshotId;
    private final boolean caseSensitive;

    private Find(
            RawWiseTable table,
            List<DataBlock> dataBlocks,
            Expression pushedPredicate,
            PrimaryKeyValue primaryKey,
            List<String> projection,
            long snapshotId,
            boolean caseSensitive,
            TableReader reader,
            ReadExecutor readExecutor) {
        super(table, dataBlocks, pushedPredicate, true, reader, readExecutor);
        this.primaryKey = Objects.requireNonNull(primaryKey, "primaryKey");
        this.projection = projection != null ? List.copyOf(projection) : List.of();
        this.snapshotId = snapshotId;
        this.caseSensitive = caseSensitive;
    }

    public Expression filter() {
        return pushedPredicate;
    }

    public List<String> projection() {
        return projection;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public boolean caseSensitive() {
        return caseSensitive;
    }

    public long limit() {
        return 1L;
    }

    public PrimaryKeyValue primaryKey() {
        return primaryKey;
    }

    public ReadContext asContext() {
        return new DefaultReadContext(filter(), snapshotId(), caseSensitive(), limit());
    }

    @Override
    public Find.Result exec() {
        ensureOpen();
        try {
            Find.Result result = (Find.Result) executeRead(this);
            if (result.success()) {
                if (result.foundRecord().isPresent()) {
                    log.info(
                            "Find succeeded: record found for key: {} on table: {}",
                            primaryKey,
                            table.getTableIdentifier());
                } else {
                    log.debug(
                            "Find completed: record not found for key: {} on table: {}",
                            primaryKey,
                            table.getTableIdentifier());
                }
            } else {
                log.error(
                        "Find failed for key: {} on table: {}, error: {}",
                        primaryKey,
                        table.getTableIdentifier(),
                        result.message().orElse("Unknown error"));
            }
            return result;
        } catch (IOException e) {
            log.error(
                    "IO error during Find for key: {} on table: {}",
                    primaryKey,
                    table.getTableIdentifier(),
                    e);
            return Find.Result.error("IO error during find: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error(
                    "Unexpected error during Find for key: {} on table: {}",
                    primaryKey,
                    table.getTableIdentifier(),
                    e);
            return Find.Result.error("Unexpected find error: " + e.getMessage(), e);
        }
    }

    @Override
    public OpType type() {
        return TYPE;
    }

    @Override
    public List<DataBlock> getDataBlocks() {
        return dataBlocks;
    }

    @Override
    public long estimatedSize() {
        return dataBlocks.stream().mapToLong(DataBlock::getSize).sum();
    }

    @Override
    public Expression getPushedPredicate() {
        return pushedPredicate;
    }

    @Override
    public boolean isPrimaryKeyLookup() {
        return true;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements ReadBuilder<Find, Builder> {
        private RawWiseTable table;
        private List<DataBlock> dataBlocks;
        private Expression pushedPredicate;
        private PrimaryKeyValue primaryKey;
        private List<String> projection;
        private Long snapshotId;
        private boolean caseSensitive = true;
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

        public Builder withPrimaryKey(PrimaryKeyValue pk) {
            this.primaryKey = pk;
            return this;
        }

        public Builder withPrimaryKey(Map<String, Comparable> pkValues) {
            this.primaryKey =
                    PrimaryKeyValue.of(
                            io.github.rawwise.model.record.PrimaryKey.of(
                                    new ArrayList<>(pkValues.keySet())),
                            pkValues);
            return this;
        }

        public Builder withProjection(List<String> projection) {
            this.projection = projection;
            return this;
        }

        public Builder useSnapshot(long snapshotId) {
            this.snapshotId = snapshotId;
            return this;
        }

        public Builder caseSensitive(boolean caseSensitive) {
            this.caseSensitive = caseSensitive;
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

        public Find build() {
            if (primaryKey == null || primaryKey.isEmpty()) {
                throw new IllegalArgumentException("Find requires primary key values");
            }
            Objects.requireNonNull(table, "table is required");
            if (readExecutor == null) {
                Objects.requireNonNull(
                        reader, "reader is required - use withReader() to inject TableReader");
            }
            log.debug(
                    "Building Find: table={}, key={}, hasReader={}, hasReadExecutor={}",
                    table.getTableIdentifier(),
                    primaryKey,
                    reader != null,
                    readExecutor != null);
            return new Find(
                    table,
                    dataBlocks,
                    pushedPredicate,
                    primaryKey,
                    projection,
                    snapshotId != null ? snapshotId : -1L,
                    caseSensitive,
                    reader,
                    readExecutor);
        }
    }

    public interface ReadContext {
        Expression filter();

        long snapshotId();

        boolean caseSensitive();

        long limit();
    }

    public record DefaultReadContext(
            Expression filter, long snapshotId, boolean caseSensitive, long limit)
            implements ReadContext {}

    public static final class Result implements Read.Result {
        private final boolean success;
        private final String message;
        private final Throwable error;
        private final long timestamp;
        private final Record foundData;
        private final byte[] rawNullableValue;

        public Result(
                boolean success,
                String message,
                Throwable error,
                long timestamp,
                Record foundData,
                byte[] rawNullableValue) {
            this.success = success;
            this.message = message;
            this.error = error;
            this.timestamp = timestamp;
            this.foundData = foundData;
            this.rawNullableValue = rawNullableValue != null ? rawNullableValue.clone() : null;
        }

        @Override
        public boolean success() {
            return success;
        }

        @Override
        public Optional<String> message() {
            return Optional.ofNullable(message);
        }

        @Override
        public Optional<Throwable> error() {
            return Optional.ofNullable(error);
        }

        @Override
        public long timestamp() {
            return timestamp;
        }

        @Override
        public long recordCount() {
            return foundData != null ? 1 : 0;
        }

        @Override
        public long bytesRead() {
            return 0L;
        }

        @Override
        public List<Record> data() {
            return foundData != null ? List.of(foundData) : List.of();
        }

        @Override
        public Optional<Object> singleResult() {
            return Optional.ofNullable(foundData);
        }

        @Override
        public boolean hasMore() {
            return false;
        }

        public boolean isFound() {
            return success && foundData != null;
        }

        public Optional<Record> foundRecord() {
            return isFound() ? Optional.of(foundData) : Optional.empty();
        }

        public byte[] rawNullableValue() {
            return rawNullableValue != null ? rawNullableValue.clone() : null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Result r)) return false;
            return success == r.success
                    && timestamp == r.timestamp
                    && java.util.Objects.equals(message, r.message)
                    && java.util.Objects.equals(error, r.error)
                    && java.util.Objects.equals(foundData, r.foundData)
                    && Arrays.equals(rawNullableValue, r.rawNullableValue);
        }

        @Override
        public int hashCode() {
            int result = java.util.Objects.hash(success, message, error, timestamp, foundData);
            result = 31 * result + Arrays.hashCode(rawNullableValue);
            return result;
        }

        @Override
        public String toString() {
            return "Find.Result{success="
                    + success
                    + ", isFound="
                    + isFound()
                    + ", timestamp="
                    + timestamp
                    + '}';
        }

        public static Result found(Record data, byte[] rawNullableValue) {
            return new Result(
                    true, "Record found", null, System.currentTimeMillis(), data, rawNullableValue);
        }

        public static Result found(Record data) {
            return found(data, null);
        }

        public static Result notFound() {
            return new Result(
                    true, "Record not found", null, System.currentTimeMillis(), null, null);
        }

        public static Result error(String msg, Throwable throwable) {
            return new Result(false, msg, throwable, System.currentTimeMillis(), null, null);
        }
    }
}
