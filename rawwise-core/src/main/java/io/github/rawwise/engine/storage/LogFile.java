package io.github.rawwise.engine.storage;

public interface LogFile extends RawWiseFile {

    LogType logType();

    String tableId();

    long snapshotId();

    long fromSequence();

    long toSequence();

    /** Number of entries in this log file. */
    long rowCount();

    /**
     * Number of tombstone entries in this log file. For DELETE_VECTOR type, all entries are
     * tombstones. For UPDATE_LOG type, this returns 0 since update log entries are modifications,
     * not deletions.
     */
    long tombstoneCount();

    enum LogType {
        UPDATE_LOG,
        DELETE_VECTOR
    }
}
