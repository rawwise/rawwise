package io.github.rawwise.model.op;

import java.util.Optional;

public interface DdlResult extends OpResult {

    OpType operationType();

    Optional<String> previousState();

    Optional<String> newState();

    final class CreateResult implements DdlResult {
        private final boolean success;
        private final String message;
        private final Throwable error;
        private final long timestamp;
        private final String tableName;

        public CreateResult(
                boolean success,
                String message,
                Throwable error,
                long timestamp,
                String tableName) {
            this.success = success;
            this.message = message;
            this.error = error;
            this.timestamp = timestamp;
            this.tableName = tableName;
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
            return OpType.CREATE_TABLE;
        }

        @Override
        public Optional<String> previousState() {
            return Optional.empty();
        }

        @Override
        public Optional<String> newState() {
            return Optional.of("Table created: " + tableName);
        }

        public String tableName() {
            return tableName;
        }

        public static CreateResult ok(String tableName) {
            return new CreateResult(
                    true,
                    "Table created successfully",
                    null,
                    System.currentTimeMillis(),
                    tableName);
        }
    }

    final class DropResult implements DdlResult {
        private final boolean success;
        private final String message;
        private final Throwable error;
        private final long timestamp;
        private final String tableName;

        public DropResult(
                boolean success,
                String message,
                Throwable error,
                long timestamp,
                String tableName) {
            this.success = success;
            this.message = message;
            this.error = error;
            this.timestamp = timestamp;
            this.tableName = tableName;
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
            return OpType.DROP_TABLE;
        }

        @Override
        public Optional<String> previousState() {
            return Optional.of("Table existed: " + tableName);
        }

        @Override
        public Optional<String> newState() {
            return Optional.of("Table dropped");
        }

        public String tableName() {
            return tableName;
        }

        public static DropResult ok(String tableName) {
            return new DropResult(
                    true,
                    "Table dropped successfully",
                    null,
                    System.currentTimeMillis(),
                    tableName);
        }
    }

    final class AlterSchemaResult implements DdlResult {
        private final boolean success;
        private final String message;
        private final Throwable error;
        private final long timestamp;
        private final String previousSchemaVersion;
        private final String newSchemaVersion;

        public AlterSchemaResult(
                boolean success,
                String message,
                Throwable error,
                long timestamp,
                String previousSchemaVersion,
                String newSchemaVersion) {
            this.success = success;
            this.message = message;
            this.error = error;
            this.timestamp = timestamp;
            this.previousSchemaVersion = previousSchemaVersion;
            this.newSchemaVersion = newSchemaVersion;
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
            return OpType.ALTER_SCHEMA;
        }

        @Override
        public Optional<String> previousState() {
            return Optional.of("Schema version: " + previousSchemaVersion);
        }

        @Override
        public Optional<String> newState() {
            return Optional.of("Schema version: " + newSchemaVersion);
        }

        public String previousSchemaVersion() {
            return previousSchemaVersion;
        }

        public String newSchemaVersion() {
            return newSchemaVersion;
        }

        public static AlterSchemaResult ok(String oldVersion, String newVersion) {
            return new AlterSchemaResult(
                    true,
                    "Schema altered successfully",
                    null,
                    System.currentTimeMillis(),
                    oldVersion,
                    newVersion);
        }
    }

    final class RefreshResult implements DdlResult {
        private final boolean success;
        private final String message;
        private final Throwable error;
        private final long timestamp;
        private final long snapshotIdBefore;
        private final long snapshotIdAfter;

        public RefreshResult(
                boolean success,
                String message,
                Throwable error,
                long timestamp,
                long snapshotIdBefore,
                long snapshotIdAfter) {
            this.success = success;
            this.message = message;
            this.error = error;
            this.timestamp = timestamp;
            this.snapshotIdBefore = snapshotIdBefore;
            this.snapshotIdAfter = snapshotIdAfter;
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
            return OpType.REFRESH;
        }

        @Override
        public Optional<String> previousState() {
            return Optional.of("Snapshot ID: " + snapshotIdBefore);
        }

        @Override
        public Optional<String> newState() {
            return Optional.of("Snapshot ID: " + snapshotIdAfter);
        }

        public long snapshotIdBefore() {
            return snapshotIdBefore;
        }

        public long snapshotIdAfter() {
            return snapshotIdAfter;
        }

        public static RefreshResult ok(long before, long after) {
            return new RefreshResult(
                    true,
                    "Refresh completed successfully",
                    null,
                    System.currentTimeMillis(),
                    before,
                    after);
        }

        public static RefreshResult error(String message, Throwable error) {
            return new RefreshResult(false, message, error, System.currentTimeMillis(), -1, -1);
        }
    }
}
