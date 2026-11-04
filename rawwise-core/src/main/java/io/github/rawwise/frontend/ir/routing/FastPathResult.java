package io.github.rawwise.frontend.ir.routing;

import java.util.List;

public sealed interface FastPathResult {

    record Hit(byte[] data) implements FastPathResult {}

    record BatchHit(List<PointQueryResult> results) implements FastPathResult {
        public BatchHit {
            results = List.copyOf(results);
        }
    }

    record Miss() implements FastPathResult {}

    record StaleEpoch(int shardId, long expectedEpoch, long actualEpoch)
            implements FastPathResult {}

    record Unreachable(String executorId) implements FastPathResult {}

    record FallbackRequired(String reason) implements FastPathResult {}

    record Updated(int rowsAffected) implements FastPathResult {}

    record Deleted(int rowsAffected) implements FastPathResult {}

    record WriteConflict(String reason) implements FastPathResult {}

    record NotFound() implements FastPathResult {}

    static FastPathResult hit(byte[] data) {
        return new Hit(data);
    }

    static FastPathResult batchHit(List<PointQueryResult> results) {
        return new BatchHit(results);
    }

    static FastPathResult miss() {
        return new Miss();
    }

    static FastPathResult staleEpoch(int shardId, long expectedEpoch, long actualEpoch) {
        return new StaleEpoch(shardId, expectedEpoch, actualEpoch);
    }

    static FastPathResult unreachable(String executorId) {
        return new Unreachable(executorId);
    }

    static FastPathResult fallbackRequired(String reason) {
        return new FallbackRequired(reason);
    }

    static FastPathResult updated(int rowsAffected) {
        return new Updated(rowsAffected);
    }

    static FastPathResult deleted(int rowsAffected) {
        return new Deleted(rowsAffected);
    }

    static FastPathResult writeConflict(String reason) {
        return new WriteConflict(reason);
    }

    static FastPathResult notFound() {
        return new NotFound();
    }
}
