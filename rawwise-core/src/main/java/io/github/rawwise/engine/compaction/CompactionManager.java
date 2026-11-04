package io.github.rawwise.engine.compaction;

import io.github.rawwise.common.PlanState;
import io.github.rawwise.engine.compaction.cleanup.FileCleanupManager;
import io.github.rawwise.engine.compaction.cleanup.FileGarbageCollector;
import io.github.rawwise.engine.compaction.cleanup.StoragePathResolver;
import io.github.rawwise.engine.compaction.lock.TableLockManager;
import io.github.rawwise.engine.compaction.merger.MergeFunction;
import io.github.rawwise.engine.compaction.merger.MultiWayMerger;
import io.github.rawwise.engine.compaction.merger.MultiWayMergerFactory;
import io.github.rawwise.engine.compaction.merger.SortedFileReader;
import io.github.rawwise.engine.compaction.planner.CompactionPlan;
import io.github.rawwise.engine.compaction.recovery.CompactionRecoveryManager;
import io.github.rawwise.engine.compaction.strategy.CompactionStrategy;
import io.github.rawwise.engine.storage.ColumnStatistics;
import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.engine.storage.ParquetDataFile;
import io.github.rawwise.engine.storage.manifest.SnapshotCommitConflictException;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.storage.reader.ReaderConfig;
import io.github.rawwise.engine.storage.reader.ReaderContext;
import io.github.rawwise.engine.storage.reader.ReaderFactoryBootstrap;
import io.github.rawwise.engine.storage.reader.ReaderFactoryRegistry;
import io.github.rawwise.engine.storage.reader.ReaderType;
import io.github.rawwise.engine.storage.spi.Storage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompactionManager {

    private final SnapshotManager snapshotManager;
    private final FileCleanupManager cleanupManager;
    private final CompactionRecoveryManager recoveryManager;
    private final FileGarbageCollector garbageCollector;
    private final Storage storage;
    private final int maxRetries;

    public CompactionManager(
            SnapshotManager snapshotManager,
            FileCleanupManager cleanupManager,
            CompactionRecoveryManager recoveryManager,
            FileGarbageCollector garbageCollector,
            Storage storage,
            int maxRetries) {
        this.snapshotManager = snapshotManager;
        this.cleanupManager = cleanupManager;
        this.recoveryManager = recoveryManager;
        this.garbageCollector = garbageCollector;
        this.storage = storage;
        this.maxRetries = maxRetries;
    }

    public CompactionResult compact(
            String tableId, CompactionStrategy strategy, MergeFunction mergeFn) throws IOException {
        ReentrantLock lock = TableLockManager.getInstance().getLock(tableId);
        lock.lock();
        try {
            return doCompact(tableId, strategy, mergeFn);
        } finally {
            lock.unlock();
        }
    }

    private CompactionResult doCompact(
            String tableId, CompactionStrategy strategy, MergeFunction mergeFn) throws IOException {
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            long snapshotId =
                    snapshotManager
                            .latestSnapshot(tableId)
                            .map(s -> s.snapshotId())
                            .orElseThrow(
                                    () ->
                                            new CompactionFatalException(
                                                    "No snapshot for table " + tableId));

            Map<Integer, List<DataFile>> layout =
                    snapshotManager.getLevelLayout(tableId, snapshotId);
            List<DataFile> l0 = layout.getOrDefault(0, List.of());
            List<DataFile> l1 = layout.getOrDefault(1, List.of());
            List<DataFile> l2 = layout.getOrDefault(2, List.of());

            List<CompactionStrategy.SourceGroup> sourceGroups = strategy.plan(l0, l1, l2);
            if (sourceGroups.isEmpty()) {
                return new CompactionResult(0, 0);
            }

            Map<String, DataFile> dataFileByPath = new HashMap<>();
            for (DataFile df : l0) dataFileByPath.put(df.filePath(), df);
            for (DataFile df : l1) dataFileByPath.put(df.filePath(), df);
            for (DataFile df : l2) dataFileByPath.put(df.filePath(), df);

            long inputBytes = 0;
            long inputRows = 0;
            for (CompactionStrategy.SourceGroup sg : sourceGroups) {
                for (String path : sg.sourceFilePaths()) {
                    DataFile df = dataFileByPath.get(path);
                    if (df != null) {
                        inputBytes += df.fileSizeBytes();
                        inputRows += df.rowCount();
                    }
                }
            }
            long estimatedOutputBytes = (long) (inputBytes * 0.7);

            CompactionPlan plan =
                    new CompactionPlan(
                            UUID.randomUUID().toString(),
                            tableId,
                            snapshotId,
                            strategy.strategyType(),
                            sourceGroups.stream()
                                    .map(
                                            g -> {
                                                long groupBytes = 0;
                                                long groupRows = 0;
                                                for (String path : g.sourceFilePaths()) {
                                                    DataFile df = dataFileByPath.get(path);
                                                    if (df != null) {
                                                        groupBytes += df.fileSizeBytes();
                                                        groupRows += df.rowCount();
                                                    }
                                                }
                                                return new CompactionPlan.SourceGroup(
                                                        g.targetLevel(),
                                                        g.sourceFilePaths(),
                                                        groupBytes,
                                                        groupRows);
                                            })
                                    .toList(),
                            inputBytes,
                            estimatedOutputBytes,
                            System.currentTimeMillis(),
                            PlanState.PENDING);

            CompactionRecoveryManager.RecoveryResult recovery =
                    recoveryManager.recoverOrphanedFiles(plan);

            List<String> newFilePaths = new ArrayList<>();
            List<String> recoveryFilePaths = new ArrayList<>();
            List<ActualOutputInfo> outputInfos = new ArrayList<>();
            try {
                plan = plan.withState(PlanState.RUNNING);

                if (recovery.state() == CompactionRecoveryManager.RecoveryState.COMMIT) {
                    recoveryFilePaths.addAll(recovery.validNewFiles());
                    outputInfos = recoverOutputInfos(recovery.validNewFiles());
                    if (outputInfos.size() != recovery.validNewFiles().size()) {
                        log.warn(
                                "Recovery COMMIT: only {}/{} files recovered successfully, aborting",
                                outputInfos.size(),
                                recovery.validNewFiles().size());
                        cleanupManager.immediateCleanup(recoveryFilePaths);
                        throw new CompactionFatalException(
                                "Recovery COMMIT failed: incomplete file metadata recovery");
                    }
                    log.info(
                            "Compaction recovery committable: plan={}, files={}",
                            plan.planId(),
                            recovery.validNewFiles().size());
                } else {
                    outputInfos = executeCompact(plan, mergeFn, newFilePaths);
                }

                List<DataFile> newDataFiles =
                        buildDataFiles(plan, outputInfos, tableId, dataFileByPath);

                List<DataFile> sourceDataFiles = new ArrayList<>();
                for (String path : plan.collectSourceFilePaths()) {
                    DataFile df = dataFileByPath.get(path);
                    if (df != null) sourceDataFiles.add(df);
                }

                long minSeq =
                        newDataFiles.stream()
                                .mapToLong(DataFile::minSequenceNumber)
                                .min()
                                .orElse(0);
                long maxSeq =
                        newDataFiles.stream()
                                .mapToLong(DataFile::maxSequenceNumber)
                                .max()
                                .orElse(0);
                snapshotManager.commitCompactionSnapshot(
                        tableId, newDataFiles, sourceDataFiles, minSeq, maxSeq);
                garbageCollector.scheduleGc(tableId, plan.collectSourceFilePaths(), snapshotId);

                plan = plan.withState(PlanState.COMPLETED);
                long totalRows =
                        outputInfos.stream().mapToLong(ActualOutputInfo::writtenRows).sum();
                log.info("Compaction committed: plan={}, rows={}", plan.planId(), totalRows);
                return new CompactionResult(sourceGroups.size(), totalRows);
            } catch (CompactionFatalException e) {
                plan = plan.withState(PlanState.FAILED);
                log.warn("Compaction fatal error for plan {}", plan.planId(), e);
                cleanupManager.immediateCleanup(newFilePaths);
                throw e;
            } catch (SnapshotCommitConflictException e) {
                plan = plan.withState(PlanState.FAILED);
                log.warn(
                        "Compaction attempt {} failed for plan {} (OCC conflict)",
                        attempt + 1,
                        plan.planId(),
                        e);
                cleanupManager.immediateCleanup(newFilePaths);
                newFilePaths.clear();
                if (attempt == maxRetries - 1) {
                    throw new CompactionRetryableException(
                            "Compaction failed after "
                                    + (attempt + 1)
                                    + " attempts due to OCC conflict",
                            e);
                }
            } catch (CompactionRetryableException e) {
                plan = plan.withState(PlanState.FAILED);
                log.warn(
                        "Compaction attempt {} failed for plan {} (retryable)",
                        attempt + 1,
                        plan.planId(),
                        e);
                cleanupManager.immediateCleanup(newFilePaths);
                newFilePaths.clear();
                if (attempt == maxRetries - 1) {
                    throw e;
                }
            } catch (Exception e) {
                plan = plan.withState(PlanState.FAILED);
                log.warn("Compaction attempt {} failed for plan {}", attempt + 1, plan.planId(), e);
                cleanupManager.immediateCleanup(newFilePaths);
                newFilePaths.clear();
                if (attempt == maxRetries - 1) {
                    throw new CompactionFatalException(
                            "Compaction failed after " + (attempt + 1) + " attempts", e);
                }
            }
        }
        throw new CompactionFatalException("Compaction failed after " + maxRetries + " attempts");
    }

    private List<ActualOutputInfo> recoverOutputInfos(List<String> recoveredFiles) {
        List<ActualOutputInfo> outputInfos = new ArrayList<>();
        for (String path : recoveredFiles) {
            try {
                Storage.StoragePath sp = StoragePathResolver.resolvePath(path);
                long fileSizeBytes = storage.status(sp).length();
                long rowCount = readParquetRowCountViaStorage(path);
                outputInfos.add(new ActualOutputInfo(path, rowCount, fileSizeBytes));
            } catch (IOException e) {
                log.error("Failed to recover output info for {}, skipping", path, e);
            }
        }
        return outputInfos;
    }

    long readParquetRowCountViaStorage(String path) throws IOException {
        Storage.StoragePath sp = StoragePathResolver.resolvePath(path);
        try (org.apache.parquet.hadoop.ParquetFileReader reader =
                org.apache.parquet.hadoop.ParquetFileReader.open(
                        new StorageInputFile(storage, sp))) {
            return reader.getFooter().getBlocks().stream()
                    .mapToLong(org.apache.parquet.hadoop.metadata.BlockMetaData::getRowCount)
                    .sum();
        }
    }

    private List<ActualOutputInfo> executeCompact(
            CompactionPlan plan, MergeFunction mergeFn, List<String> newFilePaths)
            throws IOException {
        List<ActualOutputInfo> outputInfos = new ArrayList<>();
        for (CompactionPlan.SourceGroup group : plan.sourceGroups()) {
            List<SortedFileReader> readers = new ArrayList<>();
            try {
                for (String path : group.sourceFilePaths()) {
                    readers.add(createSortedFileReader(path));
                }
            } catch (Exception e) {
                closeReaders(readers);
                throw e;
            }
            if (readers.isEmpty()) {
                continue;
            }
            MultiWayMerger merger = MultiWayMergerFactory.create(readers.size());
            String newPath =
                    group.sourceFilePaths().get(0)
                            + ".compacted.L"
                            + group.targetLevel()
                            + "_"
                            + plan.planId().substring(0, 8);
            newFilePaths.add(newPath);
            ParquetOutputWriter output = new ParquetOutputWriter(newPath);
            try {
                merger.merge(readers, mergeFn, output);
                output.close();
            } catch (IOException e) {
                try {
                    Storage.StoragePath sp = StoragePathResolver.resolvePath(newPath);
                    if (storage.exists(sp)) storage.delete(sp);
                } catch (IOException ioe) {
                    log.warn("Failed to delete partial file {}", newPath, ioe);
                }
                newFilePaths.remove(newPath);
                throw e;
            } catch (Exception e) {
                try {
                    Storage.StoragePath sp = StoragePathResolver.resolvePath(newPath);
                    if (storage.exists(sp)) storage.delete(sp);
                } catch (IOException ioe) {
                    log.warn("Failed to delete partial file {}", newPath, ioe);
                }
                newFilePaths.remove(newPath);
                throw new IOException("Compaction merge failed", e);
            }
            long writtenRows = output.writtenRows();
            long fileSizeBytes = storage.status(StoragePathResolver.resolvePath(newPath)).length();
            outputInfos.add(new ActualOutputInfo(newPath, writtenRows, fileSizeBytes));
        }
        return outputInfos;
    }

    private SortedFileReader createSortedFileReader(String path) throws IOException {
        ReaderFactoryBootstrap.ensureInitialized();
        ReaderConfig config =
                ReaderConfig.builder(ReaderType.SORTED_FILE_PARQUET).filePath(path).build();
        return ReaderFactoryRegistry.getInstance()
                .create(
                        ReaderType.SORTED_FILE_PARQUET,
                        ParquetSortedFileReader.class,
                        config,
                        ReaderContext.empty());
    }

    private void closeReaders(List<SortedFileReader> readers) {
        for (SortedFileReader reader : readers) {
            try {
                reader.close();
            } catch (Exception ce) {
                log.warn("Failed to close reader during cleanup", ce);
            }
        }
    }

    private List<DataFile> buildDataFiles(
            CompactionPlan plan,
            List<ActualOutputInfo> outputInfos,
            String tableId,
            Map<String, DataFile> dataFileByPath) {
        List<DataFile> files = new ArrayList<>();
        for (int i = 0; i < plan.sourceGroups().size() && i < outputInfos.size(); i++) {
            CompactionPlan.SourceGroup group = plan.sourceGroups().get(i);
            ActualOutputInfo info = outputInfos.get(i);

            long schemaVersion = 0;
            long minSeq = Long.MAX_VALUE;
            long maxSeq = Long.MIN_VALUE;
            KeyRange mergedRange = null;
            for (String srcPath : group.sourceFilePaths()) {
                DataFile sf = dataFileByPath.get(srcPath);
                if (sf == null) continue;
                schemaVersion = Math.max(schemaVersion, sf.schemaVersion());
                minSeq = Math.min(minSeq, sf.minSequenceNumber());
                maxSeq = Math.max(maxSeq, sf.maxSequenceNumber());
                mergedRange = mergeKeyRange(mergedRange, sf.keyRange());
            }
            if (group.sourceFilePaths().isEmpty() || minSeq == Long.MAX_VALUE) {
                minSeq = 0;
                maxSeq = 0;
            }
            if (mergedRange == null) {
                mergedRange = KeyRange.FULL_RANGE;
            }

            DataFile df =
                    new ParquetDataFile(
                            info.path(),
                            info.fileSizeBytes(),
                            System.currentTimeMillis(),
                            tableId,
                            0L,
                            mergedRange,
                            info.writtenRows(),
                            schemaVersion,
                            minSeq,
                            maxSeq,
                            Map.of(),
                            new ColumnStatistics(Map.of()),
                            group.targetLevel());
            files.add(df);
        }
        return files;
    }

    private KeyRange mergeKeyRange(KeyRange a, KeyRange b) {
        if (a == null) return b;
        if (b == null) return a;
        if (a.isFullRange() || b.isFullRange()) return KeyRange.FULL_RANGE;

        byte[] startKey;
        boolean startInclusive;
        if (a.startKey() == null) {
            startKey = null;
            startInclusive = true;
        } else if (b.startKey() == null) {
            startKey = null;
            startInclusive = true;
        } else {
            int cmp = compareBytes(a.startKey(), b.startKey());
            if (cmp <= 0) {
                startKey = a.startKey();
                startInclusive =
                        cmp < 0 ? a.startInclusive() : a.startInclusive() && b.startInclusive();
            } else {
                startKey = b.startKey();
                startInclusive = b.startInclusive();
            }
        }

        byte[] endKey;
        boolean endInclusive;
        if (a.endKey() == null) {
            endKey = null;
            endInclusive = true;
        } else if (b.endKey() == null) {
            endKey = null;
            endInclusive = true;
        } else {
            int cmp = compareBytes(a.endKey(), b.endKey());
            if (cmp >= 0) {
                endKey = a.endKey();
                endInclusive = cmp > 0 ? a.endInclusive() : a.endInclusive() && b.endInclusive();
            } else {
                endKey = b.endKey();
                endInclusive = b.endInclusive();
            }
        }

        return new KeyRange(startKey, startInclusive, endKey, endInclusive);
    }

    private static int compareBytes(byte[] left, byte[] right) {
        int max = Math.min(left.length, right.length);
        for (int i = 0; i < max; i++) {
            int l = Byte.toUnsignedInt(left[i]);
            int r = Byte.toUnsignedInt(right[i]);
            if (l != r) return l - r;
        }
        return left.length - right.length;
    }

    public RewriteFilesResult rewriteFiles(
            String tableId, MergeFunction mergeFn, List<String> filePaths) throws IOException {
        if (filePaths.isEmpty()) {
            return RewriteFilesResult.empty();
        }
        ReentrantLock lock = TableLockManager.getInstance().getLock(tableId);
        lock.lock();
        try {
            return doRewriteFiles(tableId, mergeFn, filePaths);
        } finally {
            lock.unlock();
        }
    }

    private RewriteFilesResult doRewriteFiles(
            String tableId, MergeFunction mergeFn, List<String> filePaths) throws IOException {
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            long snapshotId =
                    snapshotManager
                            .latestSnapshot(tableId)
                            .map(s -> s.snapshotId())
                            .orElseThrow(
                                    () ->
                                            new CompactionFatalException(
                                                    "No snapshot for table " + tableId));

            Map<Integer, List<DataFile>> layout =
                    snapshotManager.getLevelLayout(tableId, snapshotId);
            Map<String, DataFile> dataFileByPath = new HashMap<>();
            for (List<DataFile> levelFiles : layout.values()) {
                for (DataFile df : levelFiles) {
                    dataFileByPath.put(df.filePath(), df);
                }
            }

            int targetLevel = 0;
            long groupBytes = 0;
            long groupRows = 0;
            for (String path : filePaths) {
                DataFile df = dataFileByPath.get(path);
                if (df != null) {
                    targetLevel = Math.max(targetLevel, df.compactionLevel());
                    groupBytes += df.fileSizeBytes();
                    groupRows += df.rowCount();
                }
            }

            CompactionPlan plan =
                    new CompactionPlan(
                            UUID.randomUUID().toString(),
                            tableId,
                            snapshotId,
                            CompactionStrategy.StrategyType.TIERED,
                            List.of(
                                    new CompactionPlan.SourceGroup(
                                            targetLevel, filePaths, groupBytes, groupRows)),
                            groupBytes,
                            (long) (groupBytes * 0.7),
                            System.currentTimeMillis(),
                            PlanState.PENDING);

            List<String> newFilePaths = new ArrayList<>();
            try {
                plan = plan.withState(PlanState.RUNNING);
                List<ActualOutputInfo> outputInfos = executeCompact(plan, mergeFn, newFilePaths);

                List<DataFile> newDataFiles =
                        buildDataFiles(plan, outputInfos, tableId, dataFileByPath);

                List<DataFile> sourceDataFiles = new ArrayList<>();
                for (String path : filePaths) {
                    DataFile df = dataFileByPath.get(path);
                    if (df != null) sourceDataFiles.add(df);
                }

                long minSeq =
                        newDataFiles.stream()
                                .mapToLong(DataFile::minSequenceNumber)
                                .min()
                                .orElse(0);
                long maxSeq =
                        newDataFiles.stream()
                                .mapToLong(DataFile::maxSequenceNumber)
                                .max()
                                .orElse(0);
                snapshotManager.commitCompactionSnapshot(
                        tableId, newDataFiles, sourceDataFiles, minSeq, maxSeq);
                garbageCollector.scheduleGc(tableId, plan.collectSourceFilePaths(), snapshotId);

                plan = plan.withState(PlanState.COMPLETED);
                long totalBytes =
                        outputInfos.stream().mapToLong(ActualOutputInfo::fileSizeBytes).sum();
                return new RewriteFilesResult(outputInfos.size(), totalBytes);
            } catch (CompactionFatalException e) {
                plan = plan.withState(PlanState.FAILED);
                log.warn("Rewrite fatal error for plan {}", plan.planId(), e);
                cleanupManager.immediateCleanup(newFilePaths);
                throw e;
            } catch (SnapshotCommitConflictException e) {
                plan = plan.withState(PlanState.FAILED);
                log.warn(
                        "Rewrite attempt {} failed for plan {} (OCC conflict)",
                        attempt + 1,
                        plan.planId(),
                        e);
                cleanupManager.immediateCleanup(newFilePaths);
                newFilePaths.clear();
                if (attempt == maxRetries - 1) {
                    throw new CompactionRetryableException(
                            "Rewrite failed after "
                                    + (attempt + 1)
                                    + " attempts due to OCC conflict",
                            e);
                }
            } catch (CompactionRetryableException e) {
                plan = plan.withState(PlanState.FAILED);
                log.warn(
                        "Rewrite attempt {} failed for plan {} (retryable)",
                        attempt + 1,
                        plan.planId(),
                        e);
                cleanupManager.immediateCleanup(newFilePaths);
                newFilePaths.clear();
                if (attempt == maxRetries - 1) {
                    throw e;
                }
            } catch (IOException e) {
                plan = plan.withState(PlanState.FAILED);
                log.warn(
                        "Rewrite attempt {} failed for plan {} (IO error)",
                        attempt + 1,
                        plan.planId(),
                        e);
                cleanupManager.immediateCleanup(newFilePaths);
                newFilePaths.clear();
                if (attempt == maxRetries - 1) {
                    throw new CompactionFatalException(
                            "Rewrite failed after " + (attempt + 1) + " attempts (IO error)", e);
                }
            } catch (Exception e) {
                plan = plan.withState(PlanState.FAILED);
                log.warn(
                        "Rewrite attempt {} failed for plan {} (unexpected)",
                        attempt + 1,
                        plan.planId(),
                        e);
                cleanupManager.immediateCleanup(newFilePaths);
                newFilePaths.clear();
                if (attempt == maxRetries - 1) {
                    throw new CompactionFatalException(
                            "Rewrite failed after " + (attempt + 1) + " attempts", e);
                }
            }
        }
        throw new CompactionFatalException("Rewrite failed after " + maxRetries + " attempts");
    }

    public FileGarbageCollector getGarbageCollector() {
        return garbageCollector;
    }

    public record CompactionResult(int compactedGroupCount, long rewrittenRows) {}

    public record RewriteFilesResult(int rewrittenFileCount, long rewrittenBytes) {
        public static RewriteFilesResult empty() {
            return new RewriteFilesResult(0, 0);
        }
    }

    public record ActualOutputInfo(String path, long writtenRows, long fileSizeBytes) {}
}
