package io.github.rawwise.backend.wal;

public record WALConfig(
        String logDir,
        String logFilePrefix,
        long segmentSize,
        boolean syncOnWrite,
        int bufferSize,
        String compressionCodec,
        String storeId,
        String executorId,
        String hdfsBasePath,
        long syncIntervalMs,
        long appendTimeoutMs,
        boolean groupCommitEnabled,
        int groupCommitBatchSize,
        long groupCommitFlushIntervalMs) {
    public static final WALConfig DEFAULT =
            new WALConfig(
                    "wal_logs",
                    "wal",
                    1024 * 1024 * 100,
                    true,
                    8192,
                    "NONE",
                    "default",
                    "local",
                    "",
                    1000L,
                    30000L,
                    false,
                    1000,
                    100L);

    public WALConfig(
            String logDir,
            String logFilePrefix,
            long segmentSize,
            boolean syncOnWrite,
            int bufferSize) {
        this(
                logDir,
                logFilePrefix,
                segmentSize,
                syncOnWrite,
                bufferSize,
                "NONE",
                "default",
                "local",
                "",
                1000L,
                30000L,
                false,
                1000,
                100L);
    }

    public WALConfig(
            String logDir,
            String logFilePrefix,
            long segmentSize,
            boolean syncOnWrite,
            int bufferSize,
            String compressionCodec) {
        this(
                logDir,
                logFilePrefix,
                segmentSize,
                syncOnWrite,
                bufferSize,
                compressionCodec,
                "default",
                "local",
                "",
                1000L,
                30000L,
                false,
                1000,
                100L);
    }

    public WALConfig(
            String logDir,
            String logFilePrefix,
            long segmentSize,
            boolean syncOnWrite,
            int bufferSize,
            String compressionCodec,
            String storeId,
            String executorId,
            String hdfsBasePath,
            long syncIntervalMs) {
        this(
                logDir,
                logFilePrefix,
                segmentSize,
                syncOnWrite,
                bufferSize,
                compressionCodec,
                storeId,
                executorId,
                hdfsBasePath,
                syncIntervalMs,
                30000L,
                false,
                1000,
                100L);
    }

    public WALConfig(
            String logDir,
            String logFilePrefix,
            long segmentSize,
            boolean syncOnWrite,
            int bufferSize,
            String compressionCodec,
            String storeId,
            String executorId,
            String hdfsBasePath,
            long syncIntervalMs,
            long appendTimeoutMs) {
        this(
                logDir,
                logFilePrefix,
                segmentSize,
                syncOnWrite,
                bufferSize,
                compressionCodec,
                storeId,
                executorId,
                hdfsBasePath,
                syncIntervalMs,
                appendTimeoutMs,
                false,
                1000,
                100L);
    }

    public boolean isHdfsEnabled() {
        return hdfsBasePath != null && !hdfsBasePath.isBlank();
    }

    public WALConfig {
        if (groupCommitEnabled) {
            if (groupCommitBatchSize <= 0) {
                throw new IllegalArgumentException(
                        "groupCommitBatchSize must be > 0 when groupCommit is enabled");
            }
            if (groupCommitFlushIntervalMs <= 0) {
                throw new IllegalArgumentException(
                        "groupCommitFlushIntervalMs must be > 0 when groupCommit is enabled");
            }
            if (groupCommitFlushIntervalMs > appendTimeoutMs) {
                throw new IllegalArgumentException(
                        "groupCommitFlushIntervalMs must be <= appendTimeoutMs");
            }
        }
    }
}
