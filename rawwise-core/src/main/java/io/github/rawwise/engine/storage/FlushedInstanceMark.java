package io.github.rawwise.engine.storage;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public record FlushedInstanceMark(
        String tableId,
        String instanceId,
        long flushedSnapshotId,
        long flushedAtMs,
        String parquetPath,
        FlushStatus status) {
    public enum FlushStatus {
        FLUSHED,
        DELETING,
        DELETED
    }

    public FlushedInstanceMark {
        Objects.requireNonNull(tableId, "tableId");
        Objects.requireNonNull(instanceId, "instanceId");
        Objects.requireNonNull(status, "status");
    }

    public FlushedInstanceMark withStatus(FlushStatus newStatus) {
        return new FlushedInstanceMark(
                tableId, instanceId, flushedSnapshotId, flushedAtMs, parquetPath, newStatus);
    }

    public byte[] toBytes() {
        String s =
                String.join(
                        "\t",
                        tableId,
                        instanceId,
                        String.valueOf(flushedSnapshotId),
                        String.valueOf(flushedAtMs),
                        parquetPath == null ? "" : parquetPath,
                        status.name());
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static FlushedInstanceMark fromBytes(byte[] data) {
        String s = new String(data, StandardCharsets.UTF_8);
        String[] parts = s.split("\t", -1);
        if (parts.length < 6) {
            throw new IllegalArgumentException(
                    "Invalid FlushedInstanceMark: expected 6 fields, got " + parts.length);
        }
        return new FlushedInstanceMark(
                parts[0],
                parts[1],
                Long.parseLong(parts[2]),
                Long.parseLong(parts[3]),
                parts[4].isEmpty() ? null : parts[4],
                FlushStatus.valueOf(parts[5]));
    }
}
