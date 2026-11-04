package io.github.rawwise.engine.txn;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

public sealed interface TxnResult {

    record CommitResult(long txnId, long commitTs, List<String> writeTypes) implements TxnResult {
        public static CommitResult ok(long txnId, long commitTs, List<String> writeTypes) {
            return new CommitResult(txnId, commitTs, writeTypes);
        }
    }

    record ConflictResult(
            long txnId,
            ConflictReport.ConflictType conflictType,
            ByteBuffer conflictingKey,
            long baseVersion,
            long currentVersion,
            String description)
            implements TxnResult {
        public static ConflictResult conflict(
                long txnId,
                ConflictReport.ConflictType type,
                ByteBuffer key,
                long baseV,
                long currentV,
                String desc) {
            return new ConflictResult(txnId, type, key, baseV, currentV, desc);
        }
    }

    record ErrorResult(long txnId, String errorMessage) implements TxnResult {
        public static ErrorResult error(long txnId, String message) {
            return new ErrorResult(txnId, message);
        }
    }

    default boolean success() {
        return this instanceof CommitResult;
    }

    default boolean isConflict() {
        return this instanceof ConflictResult;
    }

    default boolean isError() {
        return this instanceof ErrorResult;
    }

    default Optional<String> message() {
        return switch (this) {
            case CommitResult cr -> Optional.empty();
            case ConflictResult cr -> Optional.of(cr.description());
            case ErrorResult er -> Optional.of(er.errorMessage());
        };
    }
}
