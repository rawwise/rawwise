package io.github.rawwise.backend.routing;

public enum RecoveryFailureType {
    WAL_HDFS_DOWNLOAD_FAILURE,
    WAL_READ_CORRUPTION,
    WAL_REPLAY_FAILURE,
    PARQUET_HDFS_DOWNLOAD_FAILURE,
    LMDB_INIT_FAILURE,
    EPOCH_CONFLICT,
    TIMEOUT,
    UNKNOWN
}
