package io.github.rawwise.model.op;

import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.record.Record;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public interface Scan extends Read {

    List<DataBlock> plan();

    default List<String> projections() {
        return List.of();
    }

    default KeyRange keyRange() {
        return null;
    }

    @Override
    default List<DataBlock> getDataBlocks() {
        return plan();
    }

    @Override
    default Expression getPushedPredicate() {
        return null;
    }

    @Override
    default boolean isPrimaryKeyLookup() {
        return false;
    }

    final class Result implements Read.Result {
        private final boolean success;
        private final String message;
        private final Throwable error;
        private final long timestamp;
        private final long recordCount;
        private final long bytesRead;
        private final List<Record> data;
        private final boolean hasMore;

        public Result(
                boolean success,
                String message,
                Throwable error,
                long timestamp,
                long recordCount,
                long bytesRead,
                List<Record> data,
                boolean hasMore) {
            this.success = success;
            this.message = message;
            this.error = error;
            this.timestamp = timestamp;
            this.recordCount = recordCount;
            this.bytesRead = bytesRead;
            this.data = data;
            this.hasMore = hasMore;
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
            return recordCount;
        }

        @Override
        public long bytesRead() {
            return bytesRead;
        }

        @Override
        public List<Record> data() {
            return data;
        }

        @Override
        public Optional<Object> singleResult() {
            return Optional.empty();
        }

        @Override
        public boolean hasMore() {
            return hasMore;
        }

        public static Result ok(List<Record> data, boolean hasMore) {
            return new Result(
                    true,
                    "Scan completed",
                    null,
                    System.currentTimeMillis(),
                    data.size(),
                    0L,
                    data,
                    hasMore);
        }

        public static Result ok(String message, List<Record> data, boolean hasMore) {
            return new Result(
                    true,
                    message,
                    null,
                    System.currentTimeMillis(),
                    data.size(),
                    0L,
                    data,
                    hasMore);
        }

        public static Result error(String msg, Throwable throwable) {
            return new Result(
                    false, msg, throwable, System.currentTimeMillis(), 0, 0L, List.of(), false);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Result r)) return false;
            return success == r.success
                    && recordCount == r.recordCount
                    && bytesRead == r.bytesRead
                    && hasMore == r.hasMore
                    && Objects.equals(message, r.message)
                    && Objects.equals(error, r.error)
                    && Objects.equals(data, r.data);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    success, message, error, timestamp, recordCount, bytesRead, data, hasMore);
        }

        @Override
        public String toString() {
            return "Scan.Result{success="
                    + success
                    + ", recordCount="
                    + recordCount
                    + ", hasMore="
                    + hasMore
                    + '}';
        }
    }
}
