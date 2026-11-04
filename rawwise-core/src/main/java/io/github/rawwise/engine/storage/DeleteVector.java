package io.github.rawwise.engine.storage;

public record DeleteVector(
        String filePath,
        FileFormat format,
        long fileSizeBytes,
        long createdAtMillis,
        String tableId,
        long snapshotId,
        long fromSequence,
        long toSequence)
        implements LogFile {

    @Override
    public LogType logType() {
        return LogType.DELETE_VECTOR;
    }

    @Override
    public long rowCount() {
        return toSequence() - fromSequence() + 1;
    }

    @Override
    public long tombstoneCount() {
        return rowCount();
    }
}
