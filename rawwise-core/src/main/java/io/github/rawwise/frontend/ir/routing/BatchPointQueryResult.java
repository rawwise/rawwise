package io.github.rawwise.frontend.ir.routing;

import java.util.List;

public sealed interface BatchPointQueryResult {

    List<byte[]> rows();

    boolean isSuccess();

    record Success(List<byte[]> rows) implements BatchPointQueryResult {
        @Override
        public boolean isSuccess() {
            return true;
        }
    }

    record PartialFailure(List<byte[]> rows, List<ShardFailure> failures)
            implements BatchPointQueryResult {
        @Override
        public boolean isSuccess() {
            return false;
        }
    }

    static BatchPointQueryResult success(List<byte[]> rows) {
        return new Success(rows);
    }

    static BatchPointQueryResult partialFailure(List<byte[]> rows, List<ShardFailure> failures) {
        return new PartialFailure(rows, failures);
    }
}
