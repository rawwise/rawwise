package io.github.rawwise.backend.lazyupdate;

import io.github.rawwise.engine.storage.LogFile;
import io.github.rawwise.engine.storage.RawWiseFile;
import java.util.Arrays;

public record UpdateLog(
        String filePath,
        RawWiseFile.FileFormat format,
        long fileSizeBytes,
        long createdAtMillis,
        String tableId,
        long snapshotId,
        long fromSequence,
        long toSequence,
        byte[] serializedPredicate,
        byte[] serializedUpdates,
        long transactionId,
        String entryId)
        implements LogFile {

    public UpdateLog {
        if (serializedPredicate != null) {
            serializedPredicate = serializedPredicate.clone();
        }
        if (serializedUpdates != null) {
            serializedUpdates = serializedUpdates.clone();
        }
    }

    public UpdateLog(
            String filePath,
            FileFormat format,
            long fileSizeBytes,
            long createdAtMillis,
            String tableId,
            long snapshotId,
            long fromSequence,
            long toSequence) {
        this(
                filePath,
                format,
                fileSizeBytes,
                createdAtMillis,
                tableId,
                snapshotId,
                fromSequence,
                toSequence,
                null,
                null,
                0L,
                null);
    }

    @Override
    public LogType logType() {
        return LogType.UPDATE_LOG;
    }

    @Override
    public long rowCount() {
        return toSequence() - fromSequence() + 1;
    }

    @Override
    public long tombstoneCount() {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o
                instanceof
                UpdateLog(
                        String path,
                        FileFormat format1,
                        long sizeBytes,
                        long atMillis,
                        String id,
                        long snapshotId1,
                        long sequence,
                        long toSequence1,
                        byte[] predicate,
                        byte[] updates,
                        long transactionId1,
                        String entryId1))) return false;
        return fileSizeBytes == sizeBytes
                && createdAtMillis == atMillis
                && snapshotId == snapshotId1
                && fromSequence == sequence
                && toSequence == toSequence1
                && transactionId == transactionId1
                && java.util.Objects.equals(filePath, path)
                && format == format1
                && java.util.Objects.equals(tableId, id)
                && Arrays.equals(serializedPredicate, predicate)
                && Arrays.equals(serializedUpdates, updates)
                && java.util.Objects.equals(entryId, entryId1);
    }

    @Override
    public int hashCode() {
        int result =
                java.util.Objects.hash(
                        filePath,
                        format,
                        fileSizeBytes,
                        createdAtMillis,
                        tableId,
                        snapshotId,
                        fromSequence,
                        toSequence,
                        transactionId,
                        entryId);
        result = 31 * result + Arrays.hashCode(serializedPredicate);
        result = 31 * result + Arrays.hashCode(serializedUpdates);
        return result;
    }
}
