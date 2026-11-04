package io.github.rawwise.engine.storage.ingestion;

import io.github.rawwise.engine.storage.ColumnStatistics;
import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.engine.storage.ParquetDataFile;
import io.github.rawwise.engine.storage.manifest.SnapshotCommitConflictException;
import io.github.rawwise.engine.storage.manifest.SnapshotOperations;
import io.github.rawwise.engine.storage.writer.DefaultParquetWriterBuilder;
import io.github.rawwise.model.record.Snapshot;
import io.github.rawwise.model.record.TableId;
import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;

@Slf4j
public class ParquetIngestionDataSink extends AbstractIngestionDataSink {

    private static final MessageType INGESTION_KV_SCHEMA =
            new MessageType(
                    "ingestion_kv",
                    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "key"),
                    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "value"),
                    new PrimitiveType(
                            Repetition.REQUIRED, PrimitiveTypeName.INT64, "sequence_number"),
                    new PrimitiveType(
                            Repetition.REQUIRED, PrimitiveTypeName.INT64, "commit_timestamp"),
                    new PrimitiveType(
                            Repetition.REQUIRED, PrimitiveTypeName.BOOLEAN, "is_tombstone"));

    record PendingParquetFile(
            String pendingId,
            Path hdfsPath,
            long fileSizeBytes,
            KeyRange keyRange,
            ColumnStatistics columnStats,
            long rowCount,
            long minSeq,
            long maxSeq,
            long schemaVersion) {}

    private final String hdfsBasePath;
    private final Configuration hadoopConf;
    private final FileSystem hdfs;
    private final SnapshotOperations snapshotOps;
    private final String compressionCodec;
    private final long rowGroupSize;
    private final int pageSize;
    private final short hdfsReplication;
    private final long maxBatchBytes;
    private final long maxPendingBytes;
    private final long orphanAgeThresholdMs;
    private final int recentSnapshotScanCount;
    private final Clock clock;
    private final MessageType parquetMessageType;
    private final ConcurrentHashMap<PendingBatchKey, ConcurrentLinkedQueue<PendingParquetFile>>
            pendingFiles = new ConcurrentHashMap<>();
    private final AtomicLong pendingBytesCount = new AtomicLong(0);

    public ParquetIngestionDataSink(
            String hdfsBasePath,
            Configuration baseConf,
            FileSystem hdfs,
            SnapshotOperations snapshotOps,
            String compressionCodec,
            long rowGroupSize,
            int pageSize,
            short hdfsReplication,
            long maxBatchBytes,
            long maxPendingBytes,
            long orphanAgeThresholdMs,
            int recentSnapshotScanCount,
            Clock clock) {
        if (hdfsBasePath == null || hdfsBasePath.isBlank()) {
            throw new IllegalArgumentException("hdfsBasePath must not be null or blank");
        }
        if (hdfs == null) throw new IllegalArgumentException("hdfs must not be null");
        if (snapshotOps == null) throw new IllegalArgumentException("snapshotOps must not be null");
        if (clock == null) throw new IllegalArgumentException("clock must not be null");
        this.hdfsBasePath = hdfsBasePath;
        this.hadoopConf = createOptimizedConf(baseConf, rowGroupSize, hdfsReplication);
        this.hdfs = hdfs;
        this.snapshotOps = snapshotOps;
        this.compressionCodec = compressionCodec;
        this.rowGroupSize = rowGroupSize;
        this.pageSize = pageSize;
        this.hdfsReplication = hdfsReplication;
        this.maxBatchBytes = maxBatchBytes;
        this.maxPendingBytes = maxPendingBytes;
        this.orphanAgeThresholdMs = orphanAgeThresholdMs;
        this.recentSnapshotScanCount = recentSnapshotScanCount;
        this.clock = clock;
        this.parquetMessageType = INGESTION_KV_SCHEMA;
    }

    @Override
    public FlushResult flushBatch(
            String tableId,
            long txnId,
            List<byte[]> keys,
            List<byte[]> values,
            long schemaVersion,
            long minSeq,
            long maxSeq)
            throws SinkRetryableException, SinkFatalException {
        return withReadLock(
                () -> {
                    String pendingId = generatePendingId(tableId, txnId);
                    return flushBatchInternal(
                            tableId, txnId, keys, values, schemaVersion, minSeq, maxSeq, pendingId);
                });
    }

    @Override
    public FlushResult flushBatch(
            String tableId,
            long txnId,
            List<byte[]> keys,
            List<byte[]> values,
            long schemaVersion,
            long minSeq,
            long maxSeq,
            long checkpointId,
            int subTaskIndex,
            long writerInstanceId)
            throws SinkRetryableException, SinkFatalException {
        return withReadLock(
                () -> {
                    String pendingId =
                            generateDeterministicPendingId(
                                    tableId,
                                    checkpointId,
                                    subTaskIndex,
                                    writerInstanceId,
                                    minSeq,
                                    maxSeq);
                    return flushBatchInternal(
                            tableId, txnId, keys, values, schemaVersion, minSeq, maxSeq, pendingId);
                });
    }

    private FlushResult flushBatchInternal(
            String tableId,
            long txnId,
            List<byte[]> keys,
            List<byte[]> values,
            long schemaVersion,
            long minSeq,
            long maxSeq,
            String pendingId)
            throws SinkRetryableException, SinkFatalException {
        validateFlushArgs(keys, values);
        long batchBytes =
                keys.stream().mapToLong(b -> b.length).sum()
                        + values.stream().mapToLong(b -> b.length).sum();
        if (maxBatchBytes > 0 && batchBytes > maxBatchBytes) {
            throw new SinkFatalException(
                    "Batch size " + batchBytes + " exceeds maxBatchBytes " + maxBatchBytes);
        }
        if (pendingBytesCount.get() > maxPendingBytes) {
            throw new SinkRetryableException(
                    "Backpressure (soft limit): pending bytes "
                            + pendingBytesCount.get()
                            + " exceed limit "
                            + maxPendingBytes
                            + ". Under concurrent flush, actual pending bytes may exceed this limit.");
        }

        var tablePath = TableId.parseLenient(tableId).toPath();
        int[] sortedIndices = sortIndicesByKey(keys);
        List<byte[]> sortedKeys = reorder(keys, sortedIndices);
        List<byte[]> sortedValues = reorder(values, sortedIndices);

        Path tmpPath;
        try {
            tmpPath =
                    writeParquetToHdfs(
                            tablePath,
                            pendingId,
                            sortedKeys,
                            sortedValues,
                            schemaVersion,
                            minSeq,
                            maxSeq);
        } catch (IOException e) {
            if (isRetryableError(e)) {
                throw new SinkRetryableException(
                        "Retryable error writing Parquet for table " + tableId, e);
            }
            throw new SinkFatalException("Fatal error writing Parquet for table " + tableId, e);
        }

        long fileSize;
        ColumnStatistics columnStats;
        try {
            fileSize = hdfs.getFileStatus(tmpPath).getLen();
            columnStats = validateAndCollectStats(tmpPath, sortedKeys.size());
        } catch (SinkFatalException e) {
            silentlyDelete(tmpPath);
            throw e;
        } catch (IOException e) {
            silentlyDelete(tmpPath);
            if (isRetryableError(e)) {
                throw new SinkRetryableException(
                        "Retryable error validating Parquet file for table " + tableId, e);
            }
            throw new SinkFatalException("Failed to validate Parquet file", e);
        }

        Path pendingDir = new Path(hdfsBasePath, tablePath + "/pending");
        Path pendingPath = new Path(pendingDir, pendingId + ".parquet");
        try {
            if (hdfs.exists(pendingPath)) {
                log.info("Pending file already exists (idempotent): {}, reusing", pendingPath);
                silentlyDelete(tmpPath);
                PendingParquetFile existing = rebuildPendingFile(pendingId, pendingPath, tablePath);
                pendingFiles
                        .computeIfAbsent(
                                PendingBatchKey.of(tableId, txnId),
                                k -> new ConcurrentLinkedQueue<>())
                        .add(existing);
                pendingBytesCount.addAndGet(existing.fileSizeBytes());
                return new FlushResult(List.of(pendingId));
            }
            hdfs.mkdirs(pendingDir);
            if (!hdfs.rename(tmpPath, pendingPath)) {
                throw new IOException("Failed to rename " + tmpPath + " to " + pendingPath);
            }
        } catch (IOException e) {
            silentlyDelete(tmpPath);
            if (isRetryableError(e)) {
                throw new SinkRetryableException(
                        "Retryable error moving Parquet file to pending for table " + tableId, e);
            }
            throw new SinkFatalException(
                    "Fatal error moving Parquet file to pending for table " + tableId, e);
        }

        KeyRange keyRange = computeKeyRange(sortedKeys);
        PendingParquetFile pendingFile =
                new PendingParquetFile(
                        pendingId,
                        pendingPath,
                        fileSize,
                        keyRange,
                        columnStats,
                        sortedKeys.size(),
                        minSeq,
                        maxSeq,
                        schemaVersion);

        pendingFiles
                .computeIfAbsent(
                        PendingBatchKey.of(tableId, txnId), k -> new ConcurrentLinkedQueue<>())
                .add(pendingFile);
        pendingBytesCount.addAndGet(fileSize);

        return new FlushResult(List.of(pendingId));
    }

    @Override
    public void prepare(String tableId, List<String> pendingIds, long txnId)
            throws SinkRetryableException, SinkFatalException {
        Set<String> pendingIdSet = new HashSet<>(pendingIds);
        withReadLock(
                () -> {
                    ConcurrentLinkedQueue<PendingParquetFile> files =
                            pendingFiles.get(PendingBatchKey.of(tableId, txnId));
                    if (files == null || files.isEmpty()) {
                        throw new SinkFatalException(
                                "No pending files found for tableId="
                                        + tableId
                                        + ", txnId="
                                        + txnId);
                    }

                    List<PendingParquetFile> toPrepare =
                            files.stream()
                                    .filter(f -> pendingIdSet.contains(f.pendingId()))
                                    .toList();
                    if (toPrepare.isEmpty()) {
                        throw new SinkFatalException("No matching pending files for prepare");
                    }

                    for (PendingParquetFile file : toPrepare) {
                        try {
                            if (!hdfs.exists(file.hdfsPath())) {
                                throw new SinkFatalException(
                                        "Pending file not found: " + file.hdfsPath());
                            }
                        } catch (IOException e) {
                            if (isRetryableError(e)) {
                                throw new SinkRetryableException(
                                        "Retryable error verifying pending file existence", e);
                            }
                            throw new SinkFatalException(
                                    "Failed to verify pending file existence", e);
                        }
                    }

                    try {
                        Optional<Snapshot> latestSnapshot = snapshotOps.latestSnapshot(tableId);
                        if (latestSnapshot.isPresent()) {
                            List<DataFile> committedFiles =
                                    snapshotOps.listDataFiles(
                                            tableId, latestSnapshot.get().snapshotId());
                            for (PendingParquetFile pendingFile : toPrepare) {
                                if (pendingFile.keyRange() == null) continue;
                                for (DataFile committedFile : committedFiles) {
                                    if (committedFile.keyRange() != null
                                            && pendingFile
                                                    .keyRange()
                                                    .overlaps(committedFile.keyRange())) {
                                        throw new SinkRetryableException(
                                                "KeyRange conflict detected for pending file: "
                                                        + pendingFile.pendingId());
                                    }
                                }
                            }
                        }
                    } catch (IOException e) {
                        if (isRetryableError(e)) {
                            throw new SinkRetryableException(
                                    "Retryable error checking KeyRange conflicts for table "
                                            + tableId,
                                    e);
                        }
                        throw new SinkFatalException(
                                "Failed to check KeyRange conflicts for table " + tableId, e);
                    }
                });
    }

    @Override
    public void commit(String tableId, List<String> pendingIds, long txnId, long commitTs)
            throws SinkRetryableException, SinkFatalException {
        Set<String> pendingIdSet = new HashSet<>(pendingIds);
        withWriteLock(
                () -> {
                    var tablePath = TableId.parseLenient(tableId).toPath();
                    ConcurrentLinkedQueue<PendingParquetFile> files =
                            pendingFiles.get(PendingBatchKey.of(tableId, txnId));
                    if (files == null || files.isEmpty()) {
                        throw new SinkFatalException("No pending files found for commit");
                    }

                    List<PendingParquetFile> toCommit =
                            files.stream()
                                    .filter(f -> pendingIdSet.contains(f.pendingId()))
                                    .toList();
                    if (toCommit.isEmpty()) {
                        throw new SinkFatalException("No matching pending files for commit");
                    }

                    renameAndCommitViaManifest(tableId, tablePath, toCommit, commitTs);

                    long bytesFreed = 0;
                    for (PendingParquetFile file : toCommit) {
                        files.remove(file);
                        bytesFreed += file.fileSizeBytes();
                    }
                    if (files.isEmpty()) {
                        pendingFiles.remove(PendingBatchKey.of(tableId, txnId));
                    }
                    pendingBytesCount.addAndGet(-bytesFreed);
                });
    }

    @Override
    public void releasePrepared(String tableId, List<String> pendingIds, long txnId) {
        Set<String> pendingIdSet = new HashSet<>(pendingIds);
        try {
            withWriteLock(
                    () -> {
                        ConcurrentLinkedQueue<PendingParquetFile> files =
                                pendingFiles.get(PendingBatchKey.of(tableId, txnId));
                        if (files == null || files.isEmpty()) return;

                        long bytesFreed = 0;
                        var iterator = files.iterator();
                        while (iterator.hasNext()) {
                            PendingParquetFile file = iterator.next();
                            if (pendingIdSet.contains(file.pendingId())) {
                                iterator.remove();
                                bytesFreed += file.fileSizeBytes();
                            }
                        }
                        if (files.isEmpty()) {
                            pendingFiles.remove(PendingBatchKey.of(tableId, txnId));
                        }
                        if (bytesFreed > 0) {
                            pendingBytesCount.addAndGet(-bytesFreed);
                        }
                    });
        } catch (SinkRetryableException | SinkFatalException e) {
            log.warn(
                    "Failed to release prepared bytes for tableId={}, txnId={}", tableId, txnId, e);
        }
    }

    @Override
    public void rollback(String tableId, List<String> pendingIds, long txnId)
            throws SinkFatalException {
        Set<String> pendingIdSet = new HashSet<>(pendingIds);
        try {
            withWriteLock(
                    () -> {
                        ConcurrentLinkedQueue<PendingParquetFile> files =
                                pendingFiles.get(PendingBatchKey.of(tableId, txnId));
                        if (files == null) return;

                        List<PendingParquetFile> toRollback =
                                files.stream()
                                        .filter(f -> pendingIdSet.contains(f.pendingId()))
                                        .toList();

                        long bytesFreed = 0;
                        for (PendingParquetFile file : toRollback) {
                            boolean deleted = false;
                            try {
                                deleted = hdfs.delete(file.hdfsPath(), false);
                            } catch (IOException e) {
                                log.error(
                                        "Failed to delete pending file during rollback: {}",
                                        file.hdfsPath(),
                                        e);
                            }
                            if (deleted) {
                                files.remove(file);
                                bytesFreed += file.fileSizeBytes();
                            }
                        }

                        if (files.isEmpty()) {
                            pendingFiles.remove(PendingBatchKey.of(tableId, txnId));
                        }
                        pendingBytesCount.addAndGet(-bytesFreed);
                    });
        } catch (SinkRetryableException e) {
            throw new SinkFatalException("Unexpected retryable error during rollback", e);
        }
    }

    @Override
    public RecoveryResult recoverPending(String tableId)
            throws SinkRetryableException, SinkFatalException {
        return withWriteLock(
                () -> {
                    var tablePath = TableId.parseLenient(tableId).toPath();
                    int recoveredCount = 0;
                    int cleanedCount = 0;
                    List<String> errors = new ArrayList<>();

                    Path tmpDir = new Path(hdfsBasePath, tablePath + "/tmp");
                    try {
                        if (hdfs.exists(tmpDir)) {
                            for (FileStatus status : hdfs.listStatus(tmpDir)) {
                                try {
                                    hdfs.delete(status.getPath(), false);
                                    cleanedCount++;
                                } catch (IOException e) {
                                    errors.add("Failed to delete tmp file: " + status.getPath());
                                }
                            }
                        }
                    } catch (IOException e) {
                        errors.add("Failed to scan tmp directory: " + e.getMessage());
                    }

                    Set<String> referencedPaths;
                    try {
                        referencedPaths = getManifestReferencedPaths(tableId);
                    } catch (IOException e) {
                        if (isRetryableError(e)) {
                            throw new SinkRetryableException(
                                    "Retryable error getting manifest-referenced paths", e);
                        }
                        throw new SinkFatalException("Failed to get manifest-referenced paths", e);
                    }

                    Path pendingDir = new Path(hdfsBasePath, tablePath + "/pending");
                    try {
                        if (hdfs.exists(pendingDir)) {
                            for (FileStatus status : hdfs.listStatus(pendingDir)) {
                                Path pendingPath = status.getPath();
                                String fileName = pendingPath.getName();
                                Path dataPath =
                                        new Path(hdfsBasePath, tablePath + "/data/" + fileName);

                                if (referencedPaths.contains(dataPath.toString())) {
                                    try {
                                        if (!validateParquetFile(pendingPath)) {
                                            errors.add(
                                                    "Skipping corrupt pending file during recovery: "
                                                            + pendingPath);
                                            continue;
                                        }
                                        hdfs.mkdirs(dataPath.getParent());
                                        if (hdfs.rename(pendingPath, dataPath)) {
                                            recoveredCount++;
                                        } else {
                                            errors.add(
                                                    "Failed to recover pending file: "
                                                            + pendingPath);
                                        }
                                    } catch (IOException e) {
                                        errors.add(
                                                "Failed to recover pending file: "
                                                        + pendingPath
                                                        + ": "
                                                        + e.getMessage());
                                    }
                                } else if (cleanOrphanDataFile(status)) {
                                    try {
                                        hdfs.delete(pendingPath, false);
                                        cleanedCount++;
                                    } catch (IOException e) {
                                        errors.add(
                                                "Failed to delete orphan file: "
                                                        + pendingPath
                                                        + ": "
                                                        + e.getMessage());
                                    }
                                }
                            }
                        }
                    } catch (IOException e) {
                        errors.add("Failed to scan pending directory: " + e.getMessage());
                    }

                    Path dataDir = new Path(hdfsBasePath, tablePath + "/data");
                    try {
                        if (hdfs.exists(dataDir)) {
                            for (FileStatus status : hdfs.listStatus(dataDir)) {
                                Path dataFilePath = status.getPath();
                                if (!referencedPaths.contains(dataFilePath.toString())) {
                                    if (clock.millis() - status.getModificationTime()
                                            > orphanAgeThresholdMs) {
                                        try {
                                            hdfs.delete(dataFilePath, false);
                                            cleanedCount++;
                                        } catch (IOException ex) {
                                            errors.add(
                                                    "Failed to delete orphan data file: "
                                                            + dataFilePath
                                                            + ": "
                                                            + ex.getMessage());
                                        }
                                    }
                                }
                            }
                        }
                    } catch (IOException e) {
                        errors.add("Failed to scan data directory for orphans: " + e.getMessage());
                    }

                    return new RecoveryResult(recoveredCount, cleanedCount, errors);
                });
    }

    @Override
    public String sinkType() {
        return "parquet";
    }

    public void commitByScanning(String tableId, List<String> pendingIds, long txnId, long commitTs)
            throws SinkRetryableException, SinkFatalException {
        Set<String> pendingIdSet = new HashSet<>(pendingIds);
        withWriteLock(
                () -> {
                    var tablePath = TableId.parseLenient(tableId).toPath();
                    Path pendingDir = new Path(hdfsBasePath, tablePath + "/pending");

                    List<PendingParquetFile> toCommit = new ArrayList<>();
                    try {
                        if (hdfs.exists(pendingDir)) {
                            for (FileStatus status : hdfs.listStatus(pendingDir)) {
                                String fileName = status.getPath().getName();
                                String pendingIdFromFile = fileName.replace(".parquet", "");
                                if (pendingIdSet.contains(pendingIdFromFile)) {
                                    PendingParquetFile pf =
                                            rebuildPendingFile(
                                                    pendingIdFromFile, status.getPath(), tablePath);
                                    toCommit.add(pf);
                                }
                            }
                        }
                    } catch (IOException e) {
                        if (isRetryableError(e)) {
                            throw new SinkRetryableException(
                                    "Retryable error scanning pending directory", e);
                        }
                        throw new SinkFatalException(
                                "Failed to scan pending directory for commit", e);
                    }

                    if (toCommit.isEmpty()) {
                        Path dataDir = new Path(hdfsBasePath, tablePath + "/data");
                        Set<String> referencedPaths;
                        try {
                            referencedPaths = getManifestReferencedPaths(tableId);
                        } catch (IOException e) {
                            throw new SinkRetryableException(
                                    "Retryable error getting manifest references for idempotent commit",
                                    e);
                        }

                        List<PendingParquetFile> recoveredDataFiles = new ArrayList<>();
                        for (String pendingId : pendingIdSet) {
                            Path dataPath = new Path(dataDir, pendingId + ".parquet");
                            try {
                                if (!hdfs.exists(dataPath)) {
                                    throw new SinkRetryableException(
                                            "Pending file not in pending/ or data/: "
                                                    + pendingId
                                                    + " — data not yet committed to HDFS, will retry");
                                }
                            } catch (IOException e) {
                                throw new SinkRetryableException(
                                        "Retryable error checking data file existence for idempotent commit: "
                                                + pendingId,
                                        e);
                            }
                            if (referencedPaths.contains(dataPath.toString())) {
                                continue;
                            }
                            log.warn(
                                    "File in data/ but not in manifest: {}, will rebuild and re-commit",
                                    dataPath);
                            try {
                                PendingParquetFile pf =
                                        rebuildPendingFile(pendingId, dataPath, tablePath);
                                recoveredDataFiles.add(pf);
                            } catch (IOException e) {
                                throw new SinkRetryableException(
                                        "Retryable error rebuilding data file for idempotent commit: "
                                                + dataPath,
                                        e);
                            }
                        }

                        if (recoveredDataFiles.isEmpty()) {
                            return;
                        }

                        try {
                            commitViaManifest(tableId, recoveredDataFiles, commitTs);
                            return;
                        } catch (SnapshotCommitConflictException e) {
                            throw new SinkRetryableException(
                                    "Snapshot commit conflict during idempotent recovery", e);
                        } catch (IOException e) {
                            if (isRetryableError(e)) {
                                throw new SinkRetryableException(
                                        "Retryable IO error during idempotent commit recovery", e);
                            }
                            throw new SinkFatalException(
                                    "Fatal IO error during idempotent commit recovery", e);
                        }
                    }

                    renameAndCommitViaManifest(tableId, tablePath, toCommit, commitTs);
                });
    }

    private void renameAndCommitViaManifest(
            String tableId, String tablePath, List<PendingParquetFile> toCommit, long commitTs)
            throws SinkRetryableException, SinkFatalException {
        List<PendingParquetFile> renamed = new ArrayList<>();
        try {
            Path dataDir = new Path(hdfsBasePath, tablePath + "/data");
            hdfs.mkdirs(dataDir);
            for (PendingParquetFile file : toCommit) {
                Path dataPath = new Path(dataDir, file.hdfsPath().getName());
                if (!hdfs.rename(file.hdfsPath(), dataPath)) {
                    throw new IOException(
                            "Failed to rename " + file.hdfsPath() + " to " + dataPath);
                }
                renamed.add(
                        new PendingParquetFile(
                                file.pendingId(),
                                dataPath,
                                file.fileSizeBytes(),
                                file.keyRange(),
                                file.columnStats(),
                                file.rowCount(),
                                file.minSeq(),
                                file.maxSeq(),
                                file.schemaVersion()));
            }
        } catch (IOException e) {
            rollbackRenamedFiles(tablePath, renamed);
            if (isRetryableError(e)) {
                throw new SinkRetryableException(
                        "Retryable error renaming pending files to data", e);
            }
            throw new SinkFatalException("Failed to rename pending files to data", e);
        }

        try {
            commitViaManifest(tableId, renamed, commitTs);
        } catch (SnapshotCommitConflictException e) {
            rollbackRenamedFiles(tablePath, renamed);
            throw new SinkRetryableException("Snapshot commit conflict", e);
        } catch (IOException e) {
            rollbackRenamedFiles(tablePath, renamed);
            if (isRetryableError(e)) {
                throw new SinkRetryableException("Retryable IO error during commit", e);
            }
            throw new SinkFatalException("Fatal IO error during commit", e);
        }
    }

    private PendingParquetFile rebuildPendingFile(
            String pendingId, Path pendingPath, String tablePath)
            throws IOException, SinkFatalException {
        long fileSize = hdfs.getFileStatus(pendingPath).getLen();
        ColumnStatistics columnStats = validateAndCollectStats(pendingPath, -1);
        KeyRange keyRange = null;
        long rowCount = 0;
        long minSeq = 0;
        long maxSeq = 0;
        long schemaVersion = 0;

        try (ParquetFileReader reader =
                ParquetFileReader.open(HadoopInputFile.fromPath(pendingPath, hadoopConf))) {
            ParquetMetadata footer = reader.getFooter();
            for (BlockMetaData block : footer.getBlocks()) {
                rowCount += block.getRowCount();
            }
            if (footer.getFileMetaData().getKeyValueMetaData() != null) {
                String sv =
                        footer.getFileMetaData()
                                .getKeyValueMetaData()
                                .get("rawwise.schema.version");
                if (sv != null) {
                    schemaVersion = Long.parseLong(sv);
                }
            }
        }

        ColumnStatistics.ColumnStat keyStats = columnStats.columns().get("key");
        if (keyStats != null && keyStats.minBytes().length > 0 && keyStats.maxBytes().length > 0) {
            keyRange = new KeyRange(keyStats.minBytes(), true, keyStats.maxBytes(), true);
        }

        ColumnStatistics.ColumnStat seqStats = columnStats.columns().get("sequence_number");
        if (seqStats != null
                && seqStats.minBytes().length == Long.BYTES
                && seqStats.maxBytes().length == Long.BYTES) {
            var bb = java.nio.ByteBuffer.wrap(seqStats.minBytes());
            minSeq = bb.getLong();
            bb = java.nio.ByteBuffer.wrap(seqStats.maxBytes());
            maxSeq = bb.getLong();
        }

        return new PendingParquetFile(
                pendingId,
                pendingPath,
                fileSize,
                keyRange,
                columnStats,
                rowCount,
                minSeq,
                maxSeq,
                schemaVersion);
    }

    private Path writeParquetToHdfs(
            String tablePath,
            String pendingId,
            List<byte[]> sortedKeys,
            List<byte[]> sortedValues,
            long schemaVersion,
            long minSeq,
            long maxSeq)
            throws IOException {
        Path tmpDir = new Path(hdfsBasePath, tablePath + "/tmp");
        hdfs.mkdirs(tmpDir);
        Path tmpPath = new Path(tmpDir, pendingId + ".parquet.tmp");

        Configuration writerConf = new Configuration(hadoopConf);
        writerConf.set("parquet.statistics.truncate.length", String.valueOf(Integer.MAX_VALUE));
        writerConf.set("parquet.columnindex.truncate.length", String.valueOf(Integer.MAX_VALUE));
        writerConf.set("rawwise.schema.version", String.valueOf(schemaVersion));

        IngestionKvWriteSupport writeSupport = new IngestionKvWriteSupport(parquetMessageType);

        try (ParquetWriter<IngestionKvRecord> writer =
                new DefaultParquetWriterBuilder<IngestionKvRecord>(tmpPath, writeSupport)
                        .withCompressionCodec(
                                CompressionCodecName.valueOf(compressionCodec.toUpperCase()))
                        .withRowGroupSize(rowGroupSize)
                        .withPageSize(pageSize)
                        .withWriteMode(ParquetFileWriter.Mode.CREATE)
                        .withConf(writerConf)
                        .withDictionaryEncoding(true)
                        .withValidation(true)
                        .withWriterVersion(WriterVersion.PARQUET_2_0)
                        .build()) {

            for (int i = 0; i < sortedKeys.size(); i++) {
                long seqNum = minSeq + i;
                if (seqNum > maxSeq) {
                    throw new SinkFatalException(
                            "Batch size "
                                    + sortedKeys.size()
                                    + " exceeds sequence number range ["
                                    + minSeq
                                    + ", "
                                    + maxSeq
                                    + "]");
                }
                IngestionKvRecord record =
                        new IngestionKvRecord(
                                sortedKeys.get(i), sortedValues.get(i), seqNum, 0L, false, true);
                writer.write(record);
            }
        }

        return tmpPath;
    }

    private ColumnStatistics validateAndCollectStats(Path path, long expectedRowCount)
            throws IOException {
        try (ParquetFileReader reader =
                ParquetFileReader.open(HadoopInputFile.fromPath(path, hadoopConf))) {
            ParquetMetadata footer = reader.getFooter();
            long totalRows = 0;
            Map<String, ColumnStatistics.ColumnStat> columnStats = new LinkedHashMap<>();
            for (BlockMetaData block : footer.getBlocks()) {
                totalRows += block.getRowCount();
                for (ColumnChunkMetaData col : block.getColumns()) {
                    String colName = col.getPath().toDotString();
                    ColumnStatistics.ColumnStat stat =
                            ColumnStatistics.ColumnStat.fromColumnChunkMetaData(col);
                    columnStats.merge(colName, stat, ColumnStatistics.ColumnStat::merge);
                }
            }
            if (expectedRowCount >= 0 && totalRows != expectedRowCount) {
                throw new IOException(
                        "Row count mismatch: expected " + expectedRowCount + ", got " + totalRows);
            }
            ColumnStatistics stats = new ColumnStatistics(columnStats);
            ColumnStatistics.ColumnStat keyStats = stats.columns().get("key");
            if (keyStats == null
                    || keyStats.minBytes().length == 0
                    || keyStats.maxBytes().length == 0) {
                throw new SinkFatalException(
                        "Key column statistics are empty in Parquet file: "
                                + path
                                + ". Check parquet.statistics.truncate.length configuration.");
            }
            return stats;
        }
    }

    private void commitViaManifest(String tableId, List<PendingParquetFile> files, long commitTs)
            throws SnapshotCommitConflictException, IOException {
        List<DataFile> dataFiles = new ArrayList<>();
        long overallMinSeq = Long.MAX_VALUE;
        long overallMaxSeq = Long.MIN_VALUE;

        for (PendingParquetFile file : files) {
            ParquetDataFile dataFile =
                    new ParquetDataFile(
                            file.hdfsPath().toString(),
                            file.fileSizeBytes(),
                            commitTs,
                            tableId,
                            DataFile.UNASSIGNED_SNAPSHOT_ID,
                            file.keyRange(),
                            file.rowCount(),
                            file.schemaVersion(),
                            file.minSeq(),
                            file.maxSeq(),
                            Map.of(),
                            file.columnStats(),
                            0);
            dataFiles.add(dataFile);
            overallMinSeq = Math.min(overallMinSeq, file.minSeq());
            overallMaxSeq = Math.max(overallMaxSeq, file.maxSeq());
        }

        if (overallMinSeq == Long.MAX_VALUE) overallMinSeq = 0;
        if (overallMaxSeq == Long.MIN_VALUE) overallMaxSeq = 0;

        snapshotOps.commitSnapshot(tableId, dataFiles, List.of(), overallMinSeq, overallMaxSeq);
    }

    private void rollbackRenamedFiles(String tablePath, List<PendingParquetFile> renamedFiles) {
        Path pendingDir = new Path(hdfsBasePath, tablePath + "/pending");
        for (PendingParquetFile file : renamedFiles) {
            try {
                Path pendingPath = new Path(pendingDir, file.hdfsPath().getName());
                if (!hdfs.rename(file.hdfsPath(), pendingPath)) {
                    log.warn(
                            "Rollback rename returned false: {} -> {}",
                            file.hdfsPath(),
                            pendingPath);
                }
            } catch (IOException e) {
                log.error("Failed to rollback renamed file: {}", file.hdfsPath(), e);
            }
        }
    }

    private KeyRange computeKeyRange(List<byte[]> sortedKeys) {
        if (sortedKeys.isEmpty()) return null;
        return new KeyRange(sortedKeys.get(0), true, sortedKeys.get(sortedKeys.size() - 1), true);
    }

    private int[] sortIndicesByKey(List<byte[]> keys) {
        Integer[] boxed = new Integer[keys.size()];
        for (int i = 0; i < keys.size(); i++) boxed[i] = i;
        Arrays.sort(boxed, (a, b) -> Arrays.compareUnsigned(keys.get(a), keys.get(b)));
        int[] result = new int[keys.size()];
        for (int i = 0; i < keys.size(); i++) result[i] = boxed[i];
        return result;
    }

    private <T> List<T> reorder(List<T> list, int[] indices) {
        List<T> result = new ArrayList<>(indices.length);
        for (int index : indices) result.add(list.get(index));
        return result;
    }

    private boolean isRetryableError(IOException e) {
        Set<Throwable> visited = new HashSet<>();
        Throwable current = e;
        while (current != null && !visited.contains(current)) {
            visited.add(current);
            if (current instanceof SocketTimeoutException
                    || current instanceof ConnectException
                    || current instanceof EOFException) {
                return true;
            }
            String className = current.getClass().getName();
            if (className.equals("org.apache.hadoop.hdfs.server.namenode.SafeModeException")
                    || className.equals("org.apache.hadoop.ipc.StandbyException")
                    || className.equals("org.apache.hadoop.ipc.RetriableException")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private String generatePendingId(String tableId, long txnId) {
        String uuid = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        return tableId.replace(":", "_") + "-pending-" + txnId + "-" + uuid;
    }

    private String generateDeterministicPendingId(
            String tableId,
            long checkpointId,
            int subTaskIndex,
            long writerInstanceId,
            long minSeq,
            long maxSeq) {
        return tableId.replace(":", "_")
                + ":ckpt-"
                + checkpointId
                + ":sub-"
                + subTaskIndex
                + ":wid-"
                + Long.toHexString(writerInstanceId)
                + ":"
                + minSeq
                + "-"
                + maxSeq;
    }

    private Set<String> getManifestReferencedPaths(String tableId) throws IOException {
        Set<String> paths = new HashSet<>();
        List<Snapshot> snapshots = snapshotOps.getRecentSnapshots(tableId, recentSnapshotScanCount);
        for (Snapshot snapshot : snapshots) {
            List<DataFile> dataFiles = snapshotOps.listDataFiles(tableId, snapshot.snapshotId());
            for (DataFile df : dataFiles) {
                paths.add(df.filePath());
            }
        }
        return paths;
    }

    private boolean cleanOrphanDataFile(FileStatus status) {
        return clock.millis() - status.getModificationTime() > orphanAgeThresholdMs;
    }

    private boolean validateParquetFile(Path path) {
        try (ParquetFileReader reader =
                ParquetFileReader.open(HadoopInputFile.fromPath(path, hadoopConf))) {
            ParquetMetadata footer = reader.getFooter();
            return footer != null && !footer.getBlocks().isEmpty();
        } catch (Exception e) {
            log.warn("Parquet file validation failed for {}: {}", path, e.getMessage());
            return false;
        }
    }

    private Configuration createOptimizedConf(
            Configuration baseConf, long rowGroupSize, short hdfsReplication) {
        Configuration conf = new Configuration(baseConf);
        conf.set("dfs.blocksize", String.valueOf(rowGroupSize));
        conf.set("io.file.buffer.size", "262144");
        conf.set("dfs.replication", String.valueOf(hdfsReplication));
        conf.set("dfs.client.write.replace-datanode-on-failure.enable", "true");
        conf.set("dfs.client.write.replace-datanode-on-failure.policy", "ALWAYS");
        return conf;
    }

    private void silentlyDelete(Path path) {
        try {
            hdfs.delete(path, false);
        } catch (IOException e) {
            log.warn("Failed to delete file silently: {}", path, e);
        }
    }

    @Override
    protected void cleanupPendingData() {
        for (var entry : pendingFiles.entrySet()) {
            PendingBatchKey key = entry.getKey();
            ConcurrentLinkedQueue<PendingParquetFile> files = entry.getValue();
            for (PendingParquetFile file : files) {
                try {
                    hdfs.delete(file.hdfsPath(), false);
                } catch (IOException e) {
                    log.error(
                            "Failed to delete pending file during close: tableId={}, txnId={}, path={}",
                            key.tableId(),
                            key.txnId(),
                            file.hdfsPath(),
                            e);
                }
            }
        }
        pendingFiles.clear();
        pendingBytesCount.set(0);
    }
}
