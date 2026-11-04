package io.github.rawwise.model.record;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record Snapshot(
        long snapshotId,
        List<String> manifestList,
        long timestampMillis,
        String summary,
        String operation,
        Schema schema,
        long schemaVersion,
        long indexVersion)
        implements Serializable {

    public static final long UNASSIGNED_SNAPSHOT_ID = -1L;

    public Snapshot {
        Objects.requireNonNull(operation, "operation is required");
        manifestList = manifestList != null ? List.copyOf(manifestList) : List.of();
        if (schema != null && schemaVersion == 0) {
            schemaVersion = schema.version();
        }
    }

    public static Snapshot of(long snapshotId, String operation) {
        return new Snapshot(
                snapshotId, null, System.currentTimeMillis(), null, operation, null, 0, 0);
    }

    public static Snapshot of(long snapshotId, String operation, Schema schema) {
        return new Snapshot(
                snapshotId, null, System.currentTimeMillis(), null, operation, schema, 0, 0);
    }

    public static Snapshot of(
            long snapshotId,
            List<String> manifestList,
            long timestampMillis,
            String summary,
            String operation,
            Schema schema) {
        return new Snapshot(
                snapshotId, manifestList, timestampMillis, summary, operation, schema, 0, 0);
    }

    public static Snapshot of(
            long snapshotId,
            List<String> manifestList,
            long timestampMillis,
            String summary,
            String operation,
            Schema schema,
            long schemaVersion,
            long indexVersion) {
        return new Snapshot(
                snapshotId,
                manifestList,
                timestampMillis,
                summary,
                operation,
                schema,
                schemaVersion,
                indexVersion);
    }

    public boolean hasManifestList() {
        return !manifestList.isEmpty();
    }

    public boolean hasSchema() {
        return schema != null;
    }

    public Instant timestampInstant() {
        return Instant.ofEpochMilli(timestampMillis);
    }

    public Optional<String> summaryOptional() {
        return Optional.ofNullable(summary);
    }
}
