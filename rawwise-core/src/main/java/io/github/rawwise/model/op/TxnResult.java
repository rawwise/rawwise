package io.github.rawwise.model.op;

import io.github.rawwise.engine.txn.ConflictReport;
import java.util.List;
import java.util.Optional;

/**
 * transaction operation result:encapsulates transaction commit,abort or conflict detection return
 * info.
 *
 * <p>three result types:
 *
 * <ul>
 *   <li>{@link Commit Result} - transaction commit success,carries txn Id and commit Ts
 *   <li>{@link Abort Result} - transaction abort,carries abort reason
 *   <li>{@link Conflict Result} - transaction conflict failure,carries conflict report
 * </ul>
 */
public interface TxnResult extends OpResult {

    OpType operationType();

    long transactionId();

    List<String> affectedOperations();

    long commitTimestamp();

    /** transaction commit success. */
    final class CommitResult implements TxnResult {
        private final boolean success;
        private final String message;
        private final Throwable error;
        private final long timestamp;
        private final long transactionId;
        private final long commitTs;
        private final long newSnapshotId;
        private final List<String> operations;

        public CommitResult(
                boolean success,
                String message,
                Throwable error,
                long timestamp,
                long transactionId,
                long commitTs,
                long newSnapshotId,
                List<String> operations) {
            this.success = success;
            this.message = message;
            this.error = error;
            this.timestamp = timestamp;
            this.transactionId = transactionId;
            this.commitTs = commitTs;
            this.newSnapshotId = newSnapshotId;
            this.operations = operations;
        }

        public CommitResult(
                boolean success,
                String message,
                Throwable error,
                long timestamp,
                long transactionId,
                long commitTs,
                List<String> operations) {
            this(success, message, error, timestamp, transactionId, commitTs, -1L, operations);
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
        public OpType operationType() {
            return OpType.COMMIT;
        }

        @Override
        public long transactionId() {
            return transactionId;
        }

        @Override
        public List<String> affectedOperations() {
            return operations;
        }

        @Override
        public long commitTimestamp() {
            return commitTs;
        }

        public long commitTs() {
            return commitTs;
        }

        public long newSnapshotId() {
            return newSnapshotId;
        }

        public static CommitResult ok(long txnId, long commitTs, List<String> ops) {
            return new CommitResult(
                    true,
                    "Transaction committed successfully",
                    null,
                    System.currentTimeMillis(),
                    txnId,
                    commitTs,
                    -1L,
                    ops);
        }

        public static CommitResult ok(
                long txnId, long commitTs, long newSnapshotId, List<String> ops) {
            return new CommitResult(
                    true,
                    "Transaction committed successfully",
                    null,
                    System.currentTimeMillis(),
                    txnId,
                    commitTs,
                    newSnapshotId,
                    ops);
        }

        /**
         * create commit failure result(non-conflict reason,e.g. internal error).
         *
         * @param error Message error message
         * @return failed commit result
         */
        public static CommitResult error(String errorMessage) {
            return new CommitResult(
                    false, errorMessage, null, System.currentTimeMillis(), -1, -1, List.of());
        }
    }

    /** transaction abort. */
    final class AbortResult implements TxnResult {
        private final boolean success;
        private final String message;
        private final Throwable error;
        private final long timestamp;
        private final long transactionId;
        private final String abortReason;

        public AbortResult(
                boolean success,
                String message,
                Throwable error,
                long timestamp,
                long transactionId,
                String abortReason) {
            this.success = success;
            this.message = message;
            this.error = error;
            this.timestamp = timestamp;
            this.transactionId = transactionId;
            this.abortReason = abortReason;
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
        public OpType operationType() {
            return OpType.ABORT;
        }

        @Override
        public long transactionId() {
            return transactionId;
        }

        @Override
        public List<String> affectedOperations() {
            return List.of();
        }

        @Override
        public long commitTimestamp() {
            return 0L;
        }

        public String abortReason() {
            return abortReason;
        }

        public static AbortResult ok(long txnId, String reason) {
            return new AbortResult(
                    true,
                    "Transaction aborted: " + reason,
                    null,
                    System.currentTimeMillis(),
                    txnId,
                    reason);
        }
    }

    /**
     * transaction conflict result:returned when OCC conflict detection fails.
     *
     * <p>carries complete{@link Conflict Report},contains conflict type,conflict key, version
     * number and other details,for client to decide retry or fallback strategy.
     */
    final class ConflictResult implements TxnResult {
        private final long txnId;
        private final ConflictReport conflictReport;
        private final ConflictReport.ConflictType conflictType;
        private final byte[] conflictKey;
        private final long myVersion;
        private final long conflictingVersion;
        private final String description;
        private final long timestamp;

        /**
         * create conflict result(carries complete Conflict Report).
         *
         * @param txn Id transaction ID
         * @param conflict Report conflict detection report
         */
        public ConflictResult(long txnId, ConflictReport conflictReport) {
            this.txnId = txnId;
            this.conflictReport = conflictReport;
            this.conflictType = conflictReport.conflictType();
            this.conflictKey = conflictReport.conflictKey();
            this.myVersion = conflictReport.myVersion();
            this.conflictingVersion = conflictReport.conflictingVersion();
            this.description = conflictReport.description();
            this.timestamp = System.currentTimeMillis();
        }

        /**
         * create conflict result method.
         *
         * @param txn Id transaction ID
         * @param conflict Type conflict type
         * @param conflict Key conflicting key
         * @param conflicting Version conflict version
         * @param description conflict description
         * @return conflict result
         */
        public static ConflictResult conflict(
                long txnId,
                ConflictReport.ConflictType conflictType,
                byte[] conflictKey,
                long conflictingVersion,
                String description) {
            ConflictReport report =
                    switch (conflictType) {
                        case WRITE_WRITE ->
                                ConflictReport.writeWrite(conflictKey, -1, conflictingVersion, -1);
                        case READ_WRITE ->
                                ConflictReport.readWrite(conflictKey, -1, conflictingVersion, -1);
                        case PHANTOM -> ConflictReport.phantom(conflictingVersion, -1, description);
                    };
            return new ConflictResult(txnId, report);
        }

        public long txnId() {
            return txnId;
        }

        public ConflictReport conflictReport() {
            return conflictReport;
        }

        public ConflictReport.ConflictType conflictType() {
            return conflictType;
        }

        public byte[] conflictKey() {
            return conflictKey != null ? conflictKey.clone() : null;
        }

        public long myVersion() {
            return myVersion;
        }

        public long conflictingVersion() {
            return conflictingVersion;
        }

        @Override
        public boolean success() {
            return false;
        }

        @Override
        public Optional<String> message() {
            return Optional.of(description);
        }

        @Override
        public Optional<Throwable> error() {
            return Optional.empty();
        }

        @Override
        public long timestamp() {
            return timestamp;
        }

        @Override
        public OpType operationType() {
            return OpType.COMMIT;
        }

        @Override
        public long transactionId() {
            return txnId;
        }

        @Override
        public List<String> affectedOperations() {
            return List.of();
        }

        @Override
        public long commitTimestamp() {
            return 0L;
        }

        @Override
        public String toString() {
            return "ConflictResult{txnId="
                    + txnId
                    + ", conflictType="
                    + conflictType
                    + ", myVersion="
                    + myVersion
                    + ", conflictingVersion="
                    + conflictingVersion
                    + ", description='"
                    + description
                    + "'}";
        }
    }
}
