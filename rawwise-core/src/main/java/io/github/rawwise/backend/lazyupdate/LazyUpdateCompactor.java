package io.github.rawwise.backend.lazyupdate;

import io.github.rawwise.engine.compaction.cleanup.FileCleanupManager;
import io.github.rawwise.engine.compaction.cleanup.FileGarbageCollector;
import io.github.rawwise.engine.compaction.lock.TableLockManager;
import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.ParquetDataFile;
import io.github.rawwise.engine.storage.manifest.SnapshotCommitConflictException;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;

/**
 * Compacts UpdateLog entries into data files.
 *
 * <p>The compactor implements the LSM-Tree compaction equivalent for the LazyUpdate mechanism:
 * UpdateLog entries that have accumulated over time are applied to the underlying Parquet data
 * files, producing new data files. The old UpdateLog entries are then marked as compacted and
 * eventually evicted.
 *
 * <h3>Compaction Flow</h3>
 *
 * <ol>
 *   <li><b>Load</b> all UpdateLog entries for the given snapshot.
 *   <li><b>Identify</b> which DataFiles are affected via {@link FilePruner#pruneFiles(List, List)}.
 *   <li><b>Rewrite</b> each affected DataFile by applying the UpdateLog entries, producing a new
 *       Parquet file (delegated to {@link DataFileRewriter}).
 *   <li><b>Register</b> the new DataFiles in a fresh Snapshot via {@link
 *       SnapshotManager#commitSnapshot}.
 *   <li><b>Cleanup</b>: mark compacted UpdateLog entries via {@link
 *       LazyUpdateManager#markCompacted(List)}.
 * </ol>
 *
 * <h3>MVCC Guarantee</h3>
 *
 * Compaction writes new files into a <em>new</em> Snapshot. Readers holding a reference to the old
 * Snapshot continue to see the old DataFiles and UpdateLogs without interruption. The old files are
 * eligible for garbage collection once all readers have moved to the new Snapshot.
 *
 * <h3>Async Execution</h3>
 *
 * Compaction is executed asynchronously via {@link #compactAsync(String, long)} to avoid blocking
 * the write path.
 */
@Slf4j
public class LazyUpdateCompactor {

    private final LazyUpdateManager lazyUpdateManager;
    private final SnapshotManager snapshotManager;
    private final DataFileRewriter dataFileRewriter;
    private final FileCleanupManager cleanupManager;
    private final FileGarbageCollector garbageCollector;
    private final int maxRetries;
    private final ExecutorService compactionExecutor;

    /**
     * Creates a compactor backed by the given components.
     *
     * @param lazyUpdateManager the manager for loading and marking UpdateLogs
     * @param snapshotManager the snapshot manager for registering new files
     * @param dataFileRewriter the rewriter that produces new DataFiles with updates applied
     */
    public LazyUpdateCompactor(
            LazyUpdateManager lazyUpdateManager,
            SnapshotManager snapshotManager,
            DataFileRewriter dataFileRewriter,
            FileCleanupManager cleanupManager,
            FileGarbageCollector garbageCollector,
            int maxRetries) {
        this.lazyUpdateManager =
                java.util.Objects.requireNonNull(
                        lazyUpdateManager, "lazyUpdateManager must not be null");
        this.snapshotManager =
                java.util.Objects.requireNonNull(
                        snapshotManager, "snapshotManager must not be null");
        this.dataFileRewriter =
                java.util.Objects.requireNonNull(
                        dataFileRewriter, "dataFileRewriter must not be null");
        this.cleanupManager =
                java.util.Objects.requireNonNull(cleanupManager, "cleanupManager must not be null");
        this.garbageCollector =
                java.util.Objects.requireNonNull(
                        garbageCollector, "garbageCollector must not be null");
        this.maxRetries = maxRetries;
        this.compactionExecutor =
                Executors.newSingleThreadExecutor(
                        r -> {
                            Thread t = new Thread(r, "lazy-compactor");
                            t.setDaemon(true);
                            return t;
                        });
    }

    /**
     * Execute compaction synchronously for the given table and snapshot.
     *
     * @param tableId the table identifier
     * @param snapshotId the snapshot whose UpdateLogs to compact
     * @return result describing the compaction outcome
     * @throws IOException if loading data or committing the snapshot fails
     */
    public CompactionResult compact(String tableId, long snapshotId) throws IOException {
        ReentrantLock lock = TableLockManager.getInstance().getLock(tableId);
        lock.lock();
        try {
            return doCompact(tableId, snapshotId);
        } finally {
            lock.unlock();
        }
    }

    private CompactionResult doCompact(String tableId, long snapshotId) throws IOException {
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            long currentSnapshotId =
                    snapshotManager
                            .latestSnapshot(tableId)
                            .map(s -> s.snapshotId())
                            .orElse(snapshotId);
            if (attempt > 0) {
                log.info(
                        "Compaction retry {}: re-reading snapshot for table={}, oldSnapshotId={}, newSnapshotId={}",
                        attempt + 1,
                        tableId,
                        snapshotId,
                        currentSnapshotId);
            }
            snapshotId = currentSnapshotId;

            List<UpdateLogEntry> updateLogs = lazyUpdateManager.loadUpdateLogs(tableId, snapshotId);
            if (updateLogs.isEmpty()) {
                log.info(
                        "Compaction: no UpdateLog entries for table={}, snapshot={}, nothing to compact",
                        tableId,
                        snapshotId);
                return new CompactionResult(0, 0, 0, 0);
            }

            log.info(
                    "Compaction: loaded {} UpdateLog entries for table={}, snapshot={}",
                    updateLogs.size(),
                    tableId,
                    snapshotId);

            List<DataFile> allDataFiles = snapshotManager.listDataFiles(tableId, snapshotId);
            if (allDataFiles.isEmpty()) {
                log.info(
                        "Compaction: no DataFiles for table={}, snapshot={}, skipping compaction",
                        tableId,
                        snapshotId);
                return new CompactionResult(updateLogs.size(), 0, 0, 0);
            }

            List<DataFile> affectedFiles =
                    FilePruner.pruneFiles(updateLogs, allDataFiles, (String) null);
            log.debug("FilePruner called without keyColumn, using conservative mode");

            if (affectedFiles.isEmpty()) {
                log.info(
                        "Compaction: no DataFiles affected by {} UpdateLogs for table={}",
                        updateLogs.size(),
                        tableId);
                List<String> entryIds = extractEntryIds(updateLogs);
                lazyUpdateManager.markCompacted(entryIds);
                return new CompactionResult(updateLogs.size(), 0, 0, allDataFiles.size());
            }

            log.info(
                    "Compaction: {}/{} DataFiles affected for table={}",
                    affectedFiles.size(),
                    allDataFiles.size(),
                    tableId);

            List<DataFile> newDataFiles = new ArrayList<>();
            List<String> rewrittenPaths = new ArrayList<>();
            int totalRowsRewritten = 0;
            long totalNewBytes = 0;

            try {
                for (DataFile oldFile : affectedFiles) {
                    DataFileRewriter.RewriteResult rewriteResult =
                            dataFileRewriter.rewrite(tableId, oldFile, updateLogs);

                    String newFilePath = rewriteResult.newFilePath();
                    long newFileSize = rewriteResult.newFileSizeBytes();
                    long newRowCount = rewriteResult.newRowCount();

                    Path filePathObj = Path.of(newFilePath);
                    String fileName = filePathObj.getFileName().toString();

                    DataFile newFile =
                            new ParquetDataFile(
                                    newFilePath,
                                    newFileSize,
                                    System.currentTimeMillis(),
                                    tableId,
                                    0L,
                                    oldFile.keyRange(),
                                    newRowCount,
                                    oldFile.schemaVersion(),
                                    oldFile.minSequenceNumber(),
                                    oldFile.maxSequenceNumber(),
                                    oldFile.partitionValues(),
                                    oldFile.columnStatistics(),
                                    oldFile.compactionLevel());

                    newDataFiles.add(newFile);
                    rewrittenPaths.add(newFilePath);
                    totalRowsRewritten += (int) newRowCount;
                    totalNewBytes += newFileSize;

                    log.info(
                            "Compaction: rewrote DataFile {} -> {} (rows={}, bytes={})",
                            oldFile.filePath(),
                            newFilePath,
                            newRowCount,
                            newFileSize);
                }

                long minSeq =
                        updateLogs.stream()
                                .mapToLong(UpdateLogEntry::sequenceNumber)
                                .min()
                                .orElse(0);
                long maxSeq =
                        updateLogs.stream()
                                .mapToLong(UpdateLogEntry::sequenceNumber)
                                .max()
                                .orElse(0);

                snapshotManager.commitCompactionSnapshot(
                        tableId, newDataFiles, affectedFiles, minSeq, maxSeq);

                log.info(
                        "Compaction: registered {} new DataFiles in new Snapshot for table={}",
                        newDataFiles.size(),
                        tableId);

                List<String> entryIds = extractEntryIds(updateLogs);
                lazyUpdateManager.markCompacted(entryIds);

                log.info(
                        "Compaction: marked {} UpdateLog entries as compacted for table={}",
                        entryIds.size(),
                        tableId);

                garbageCollector.scheduleGc(
                        tableId,
                        affectedFiles.stream().map(DataFile::filePath).toList(),
                        snapshotId);

                return new CompactionResult(
                        updateLogs.size(),
                        affectedFiles.size(),
                        affectedFiles.size(),
                        allDataFiles.size());
            } catch (SnapshotCommitConflictException e) {
                log.warn(
                        "Compaction attempt {} failed for table={} (OCC conflict)",
                        attempt + 1,
                        tableId,
                        e);
                cleanupManager.immediateCleanup(rewrittenPaths);
                if (attempt == maxRetries - 1) {
                    throw new IOException(
                            "Compaction failed after "
                                    + (attempt + 1)
                                    + " attempts due to OCC conflict",
                            e);
                }
            } catch (IOException e) {
                log.error("Compaction: failed to rewrite DataFile, aborting compaction", e);
                cleanupManager.immediateCleanup(rewrittenPaths);
                throw e;
            }
        }
        throw new IOException("Compaction failed after " + maxRetries + " attempts");
    }

    /**
     * Execute compaction asynchronously. Returns a future that completes when compaction finishes.
     *
     * @param tableId the table identifier
     * @param snapshotId the snapshot whose UpdateLogs to compact
     * @return a future that completes with the compaction result
     */
    public CompletableFuture<CompactionResult> compactAsync(String tableId, long snapshotId) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return compact(tableId, snapshotId);
                    } catch (IOException e) {
                        log.error(
                                "Async compaction failed for table={}, snapshot={}",
                                tableId,
                                snapshotId,
                                e);
                        throw new RuntimeException("Compaction failed: " + e.getMessage(), e);
                    }
                },
                compactionExecutor);
    }

    /** Shutdown the compaction executor, waiting for in-flight compactions. */
    public void shutdown() {
        compactionExecutor.shutdown();
        log.info("CompactionExecutor shutdown initiated");
    }

    // --- Accessors ---

    public LazyUpdateManager getLazyUpdateManager() {
        return lazyUpdateManager;
    }

    public SnapshotManager getSnapshotManager() {
        return snapshotManager;
    }

    public DataFileRewriter getDataFileRewriter() {
        return dataFileRewriter;
    }

    // --- Helpers ---

    private static List<String> extractEntryIds(List<UpdateLogEntry> updateLogs) {
        return updateLogs.stream().map(UpdateLogEntry::entryId).toList();
    }

    // ========================================================================
    // Inner types
    // ========================================================================

    /**
     * Rewrites a DataFile by applying UpdateLog entries to its rows, producing a new Parquet file.
     *
     * <p>Implementations are responsible for:
     *
     * <ol>
     *   <li>Reading all rows from the old DataFile
     *   <li>Applying UpdateLog entries in sequence order
     *   <li>Writing the updated rows to a new Parquet file
     *   <li>Returning the new file's path, size, and row count
     * </ol>
     */
    @FunctionalInterface
    public interface DataFileRewriter {

        /**
         * Rewrite a data file with UpdateLog updates applied.
         *
         * @param tableId the table identifier
         * @param oldDataFile the original data file to rewrite
         * @param updateLogs UpdateLog entries to apply, in sequence order
         * @return metadata for the newly written file
         * @throws IOException if reading or writing fails
         */
        RewriteResult rewrite(String tableId, DataFile oldDataFile, List<UpdateLogEntry> updateLogs)
                throws IOException;

        /** The result of rewriting a single data file. */
        record RewriteResult(String newFilePath, long newFileSizeBytes, long newRowCount) {
            public RewriteResult {
                java.util.Objects.requireNonNull(newFilePath, "newFilePath must not be null");
                if (newFileSizeBytes < 0) {
                    throw new IllegalArgumentException(
                            "newFileSizeBytes must be non-negative: " + newFileSizeBytes);
                }
                if (newRowCount < 0) {
                    throw new IllegalArgumentException(
                            "newRowCount must be non-negative: " + newRowCount);
                }
            }
        }
    }

    /** The result of a compaction operation. */
    public record CompactionResult(
            long compactedUpdateLogCount,
            long affectedFileCount,
            long rewrittenFileCount,
            long totalFileCount) {
        public boolean isEmpty() {
            return compactedUpdateLogCount == 0;
        }

        @Override
        public String toString() {
            return String.format(
                    "CompactionResult[updateLogs=%d, affected=%d, rewritten=%d, total=%d]",
                    compactedUpdateLogCount, affectedFileCount, rewrittenFileCount, totalFileCount);
        }
    }
}
