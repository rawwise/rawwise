package io.github.rawwise.engine.storage.ingestion;

import io.github.rawwise.engine.storage.manifest.SnapshotOperations;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import java.io.IOException;
import java.time.Clock;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public final class IngestionDataSinkFactory {

    private static final int DEFAULT_PAGE_SIZE = 64 * 1024;
    private static final short DEFAULT_HDFS_REPLICATION = 3;
    private static final int DEFAULT_RECENT_SNAPSHOT_SCAN_COUNT = 10;

    private IngestionDataSinkFactory() {}

    public static IngestionDataSink create(
            RawWiseConfig config,
            IngestionWriteOperations lmdbWriteOps,
            SnapshotOperations snapshotOps)
            throws SinkFatalException {
        Objects.requireNonNull(config, "config must not be null");
        String sinkType = config.get(RawWiseOptions.INGESTION_SINK_TYPE);
        if (sinkType == null || sinkType.isBlank()) {
            throw new IllegalArgumentException("INGESTION_SINK_TYPE must be configured");
        }
        if ("PARQUET".equalsIgnoreCase(sinkType)) {
            Objects.requireNonNull(
                    snapshotOps, "snapshotOps must not be null when sink type is PARQUET");
            return createParquetSink(config, snapshotOps);
        } else if ("LMDB".equalsIgnoreCase(sinkType)) {
            Objects.requireNonNull(
                    lmdbWriteOps, "lmdbWriteOps must not be null when sink type is LMDB");
            return createLmdbSink(config, lmdbWriteOps);
        } else {
            throw new IllegalArgumentException(
                    "Unknown INGESTION_SINK_TYPE: " + sinkType + ", must be LMDB or PARQUET");
        }
    }

    public static LmdbIngestionDataSink createLmdbSink(
            RawWiseConfig config, IngestionWriteOperations writeOps) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(writeOps, "writeOps must not be null");
        int maxPendingInstances =
                config.get(RawWiseOptions.INGESTION_SINK_LMDB_MAX_PENDING_INSTANCES);
        long instanceMappedBytes =
                config.get(RawWiseOptions.INGESTION_SINK_LMDB_INSTANCE_MAPPED_BYTES);
        return new LmdbIngestionDataSink(writeOps, maxPendingInstances, instanceMappedBytes);
    }

    public static ParquetIngestionDataSink createParquetSink(
            RawWiseConfig config, SnapshotOperations snapshotOps) throws SinkFatalException {
        try {
            Configuration hadoopConf = new Configuration();
            FileSystem hdfs = FileSystem.get(hadoopConf);
            return createParquetSink(config, hadoopConf, hdfs, snapshotOps, Clock.systemUTC());
        } catch (IOException e) {
            throw new SinkFatalException(
                    "Failed to create ParquetIngestionDataSink: " + e.getMessage(), e);
        }
    }

    public static ParquetIngestionDataSink createParquetSink(
            RawWiseConfig config,
            Configuration hadoopConf,
            FileSystem hdfs,
            SnapshotOperations snapshotOps,
            Clock clock) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(hadoopConf, "hadoopConf must not be null");
        Objects.requireNonNull(hdfs, "hdfs must not be null");
        Objects.requireNonNull(snapshotOps, "snapshotOps must not be null");
        Objects.requireNonNull(clock, "clock must not be null");

        String hdfsBasePath = config.get(RawWiseOptions.INGESTION_SINK_PARQUET_HDFS_BASE_PATH);
        long rowGroupSize = config.get(RawWiseOptions.INGESTION_SINK_PARQUET_ROW_GROUP_SIZE_BYTES);
        String compressionCodec =
                config.get(RawWiseOptions.INGESTION_SINK_PARQUET_COMPRESSION_CODEC);
        long maxPendingBytes = config.get(RawWiseOptions.INGESTION_SINK_PARQUET_MAX_PENDING_BYTES);
        long maxBatchBytes = config.get(RawWiseOptions.INGESTION_SINK_PARQUET_MAX_BATCH_BYTES);
        long minBatchBytes = config.get(RawWiseOptions.INGESTION_SINK_PARQUET_MIN_BATCH_BYTES);
        long orphanAgeThresholdMs =
                config.get(RawWiseOptions.INGESTION_SINK_PARQUET_ORPHAN_AGE_THRESHOLD_MS);

        if (minBatchBytes > maxBatchBytes) {
            throw new IllegalArgumentException(
                    "INGESTION_SINK_PARQUET_MIN_BATCH_BYTES ("
                            + minBatchBytes
                            + ") must not exceed INGESTION_SINK_PARQUET_MAX_BATCH_BYTES ("
                            + maxBatchBytes
                            + ")");
        }
        if (maxBatchBytes > maxPendingBytes) {
            throw new IllegalArgumentException(
                    "INGESTION_SINK_PARQUET_MAX_BATCH_BYTES ("
                            + maxBatchBytes
                            + ") must not exceed INGESTION_SINK_PARQUET_MAX_PENDING_BYTES ("
                            + maxPendingBytes
                            + ")");
        }

        return new ParquetIngestionDataSink(
                hdfsBasePath,
                hadoopConf,
                hdfs,
                snapshotOps,
                compressionCodec,
                rowGroupSize,
                DEFAULT_PAGE_SIZE,
                DEFAULT_HDFS_REPLICATION,
                maxBatchBytes,
                maxPendingBytes,
                orphanAgeThresholdMs,
                DEFAULT_RECENT_SNAPSHOT_SCAN_COUNT,
                clock);
    }
}
