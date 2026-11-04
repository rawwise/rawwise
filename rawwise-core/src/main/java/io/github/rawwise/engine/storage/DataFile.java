package io.github.rawwise.engine.storage;

import java.util.Map;

public interface DataFile extends RawWiseFile {

    long UNASSIGNED_SNAPSHOT_ID = -1L;

    int UNASSIGNED_LEVEL = -1;

    String tableId();

    long snapshotId();

    KeyRange keyRange();

    long rowCount();

    long schemaVersion();

    long minSequenceNumber();

    long maxSequenceNumber();

    Map<String, String> partitionValues();

    ColumnStatistics columnStatistics();

    int compactionLevel();
}
