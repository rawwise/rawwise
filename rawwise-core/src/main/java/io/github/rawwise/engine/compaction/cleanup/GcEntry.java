package io.github.rawwise.engine.compaction.cleanup;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;

public record GcEntry(
        String filePath,
        String tableId,
        long obsoleteAfterSnapshotId,
        int state,
        long createdAtMs,
        int retryCount,
        int maxRetries,
        String reason,
        long deadLetterTtlMs)
        implements Serializable {

    public static final int STATE_PENDING = 0;
    public static final int STATE_DELETING = 1;
    public static final int STATE_DELETED = 2;
    public static final int STATE_DEAD_LETTER = 3;

    public GcEntry {
        if (filePath == null) throw new IllegalArgumentException("filePath must not be null");
        if (tableId == null) throw new IllegalArgumentException("tableId must not be null");
        if (maxRetries < 1) maxRetries = 3;
        if (deadLetterTtlMs < 0) deadLetterTtlMs = 604800000L;
    }

    public static GcEntry pending(
            String filePath, String tableId, long obsoleteAfterSnapshotId, long now) {
        return new GcEntry(
                filePath,
                tableId,
                obsoleteAfterSnapshotId,
                STATE_PENDING,
                now,
                0,
                3,
                null,
                604800000L);
    }

    public GcEntry withIncrementedRetry() {
        return new GcEntry(
                filePath,
                tableId,
                obsoleteAfterSnapshotId,
                state,
                createdAtMs,
                retryCount + 1,
                maxRetries,
                reason,
                deadLetterTtlMs);
    }

    public GcEntry asDeadLetter(String reason) {
        return new GcEntry(
                filePath,
                tableId,
                obsoleteAfterSnapshotId,
                STATE_DEAD_LETTER,
                System.currentTimeMillis(),
                retryCount,
                maxRetries,
                reason,
                deadLetterTtlMs);
    }

    public byte[] toBytes() {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeUTF(filePath);
            dos.writeUTF(tableId);
            dos.writeLong(obsoleteAfterSnapshotId);
            dos.writeInt(state);
            dos.writeLong(createdAtMs);
            dos.writeInt(retryCount);
            dos.writeInt(maxRetries);
            dos.writeUTF(reason == null ? "" : reason);
            dos.writeLong(deadLetterTtlMs);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static GcEntry fromBytes(byte[] data) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            String fp = dis.readUTF();
            String tid = dis.readUTF();
            long snapId = dis.readLong();
            int st = dis.readInt();
            long createdAt = dis.readLong();
            int retry = dis.readInt();
            int maxRetry = dis.readInt();
            String rsn = dis.readUTF();
            long ttl = dis.readLong();
            return new GcEntry(
                    fp,
                    tid,
                    snapId,
                    st,
                    createdAt,
                    retry,
                    maxRetry,
                    rsn.isEmpty() ? null : rsn,
                    ttl);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
