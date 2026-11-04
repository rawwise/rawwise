package io.github.rawwise.backend.lazyupdate;

import io.github.rawwise.engine.storage.RawWiseFile;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.storage.reader.ReaderConfig;
import io.github.rawwise.engine.storage.reader.ReaderContext;
import io.github.rawwise.engine.storage.reader.ReaderFactoryBootstrap;
import io.github.rawwise.engine.storage.reader.ReaderFactoryRegistry;
import io.github.rawwise.engine.storage.reader.ReaderType;
import io.github.rawwise.model.record.Snapshot;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LazyUpdateManager {

    private static final long DEFAULT_ROLLOVER_SIZE_BYTES = 64L * 1024 * 1024;
    private static final long DEFAULT_ROLLOVER_ENTRY_COUNT = 100_000L;

    @Getter private final SnapshotManager snapshotManager;
    @Getter private final UpdateLogReader reader;
    private final AtomicLong sequenceCounter;

    @Setter @Getter private volatile String updateLogBasePath = "/tmp/rawwise/update_logs";

    @Setter @Getter private volatile long rolloverSizeBytes = DEFAULT_ROLLOVER_SIZE_BYTES;

    @Setter @Getter private volatile long rolloverEntryCount = DEFAULT_ROLLOVER_ENTRY_COUNT;

    private final Map<String, ActiveUpdateLogFile> activeFiles = new ConcurrentHashMap<>();
    private final Map<String, ReentrantLock> tableWriteLocks = new ConcurrentHashMap<>();
    private final Map<Long, Long> snapshotActiveMaxSeq = new ConcurrentHashMap<>();
    private final AtomicLong activeMaxSeqByLatest = new AtomicLong(0L);

    public LazyUpdateManager(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
        this.reader = createUpdateLogReader();
        this.sequenceCounter = new AtomicLong(System.currentTimeMillis());
    }

    public LazyUpdateManager(
            io.github.rawwise.metastore.Metastore metastore, SnapshotManager snapshotManager) {
        this(snapshotManager);
    }

    private UpdateLogReader createUpdateLogReader() {
        ReaderFactoryBootstrap.ensureInitialized();
        try {
            return ReaderFactoryRegistry.getInstance()
                    .create(
                            ReaderType.UPDATE_LOG,
                            UpdateLogReader.class,
                            ReaderConfig.builder(ReaderType.UPDATE_LOG).build(),
                            ReaderContext.empty());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create UpdateLogReader via factory", e);
        }
    }

    public WriteResult writeUpdateLog(
            String tableId,
            io.github.rawwise.model.expression.Expression predicate,
            Map<String, Comparable<?>> updates,
            long txnId)
            throws IOException {
        Objects.requireNonNull(tableId, "tableId must not be null");
        ReentrantLock tableLock =
                tableWriteLocks.computeIfAbsent(tableId, k -> new ReentrantLock());
        tableLock.lock();
        try {
            long seqNum = sequenceCounter.incrementAndGet();
            String entryId = UUID.randomUUID().toString();
            UpdateLogEntry entry =
                    new UpdateLogEntry(
                            entryId,
                            tableId,
                            predicate,
                            updates,
                            txnId,
                            seqNum,
                            System.currentTimeMillis());

            ActiveUpdateLogFile active = getOrCreateActiveFile(tableId);
            active.appendEntry(entry);
            activeMaxSeqByLatest.accumulateAndGet(seqNum, Math::max);

            UpdateLog updateLog =
                    new UpdateLog(
                            active.getActivePath().toString(),
                            RawWiseFile.FileFormat.NWL,
                            active.getFileSize(),
                            System.currentTimeMillis(),
                            tableId,
                            0L,
                            seqNum,
                            seqNum,
                            null,
                            null,
                            txnId,
                            entryId);

            log.info(
                    "Appended UpdateLog to active: table={} entryId={} seq={} txn={}",
                    tableId,
                    entryId,
                    seqNum,
                    txnId);
            return new WriteResult(entryId, updateLog);
        } finally {
            tableLock.unlock();
        }
    }

    public WriteResult writeAndCommitUpdateLog(
            String tableId,
            io.github.rawwise.model.expression.Expression predicate,
            Map<String, Comparable<?>> updates,
            long txnId)
            throws IOException {
        WriteResult result = writeUpdateLog(tableId, predicate, updates, txnId);
        appendCommitRecord(tableId, txnId, result.updateLog().toSequence());
        rolloverIfNecessary(tableId);
        return result;
    }

    public void appendCommitRecord(String tableId, long txnId, long txnMaxSeq) throws IOException {
        Objects.requireNonNull(tableId, "tableId must not be null");
        ReentrantLock tableLock =
                tableWriteLocks.computeIfAbsent(tableId, k -> new ReentrantLock());
        tableLock.lock();
        try {
            ActiveUpdateLogFile active = getOrCreateActiveFile(tableId);
            active.appendCommitRecord(txnId);
            active.fsync();
            snapshotActiveMaxSeq.put(txnId, txnMaxSeq);
            activeMaxSeqByLatest.updateAndGet(prev -> Math.max(prev, txnMaxSeq));
            log.info(
                    "Appended commit record: table={} txn={} txnMaxSeq={}",
                    tableId,
                    txnId,
                    txnMaxSeq);
        } finally {
            tableLock.unlock();
        }
    }

    public void appendAbortRecord(String tableId, long txnId) throws IOException {
        Objects.requireNonNull(tableId, "tableId must not be null");
        ReentrantLock tableLock =
                tableWriteLocks.computeIfAbsent(tableId, k -> new ReentrantLock());
        tableLock.lock();
        try {
            ActiveUpdateLogFile active = getOrCreateActiveFile(tableId);
            active.appendAbortRecord(txnId);
            active.fsync();
            log.info("Appended abort record: table={} txn={}", tableId, txnId);
        } finally {
            tableLock.unlock();
        }
    }

    public void rollbackUpdateLog(UpdateLog updateLog) {
        try {
            String tableId = updateLog.tableId();
            if (tableId == null) {
                tableId = inferTableIdFromPath(updateLog.filePath());
            }
            if (tableId != null) {
                appendAbortRecord(tableId, updateLog.transactionId());
                log.info(
                        "Appended abort record for txnId={} tableId={}",
                        updateLog.transactionId(),
                        tableId);
            } else {
                log.warn("Cannot infer tableId for rollback, ignoring: {}", updateLog.filePath());
            }
        } catch (IOException e) {
            log.warn("Failed to append abort record for rollback: {}", updateLog.filePath(), e);
        }
    }

    public record WriteResult(String entryId, UpdateLog updateLog) {}

    public List<UpdateLogEntry> loadUpdateLogs(String tableId, long snapshotId) throws IOException {
        List<UpdateLogEntry> allEntries = new ArrayList<>();
        long maxSeq = resolveActiveMaxSeq(snapshotId);

        Path tableDir = getTableDir(tableId);
        if (!Files.exists(tableDir)) {
            return allEntries;
        }

        try (var files = Files.list(tableDir)) {
            List<Path> sorted = files.filter(p -> p.toString().endsWith(".nwl")).sorted().toList();
            for (Path nwlFile : sorted) {
                UpdateLog fake =
                        new UpdateLog(
                                nwlFile.toString(),
                                RawWiseFile.FileFormat.NWL,
                                0L,
                                0L,
                                tableId,
                                0L,
                                0L,
                                0L);
                try {
                    List<UpdateLogEntry> entries = reader.read(fake, maxSeq);
                    allEntries.addAll(entries);
                } catch (IOException e) {
                    log.warn("Failed to read NWL file during load: {}", nwlFile, e);
                }
            }
        }

        allEntries.sort(Comparator.comparingLong(UpdateLogEntry::sequenceNumber));
        log.info(
                "Loaded {} UpdateLog entries for table={}, snapshot={}, maxSeq={}",
                allEntries.size(),
                tableId,
                snapshotId,
                maxSeq);
        return allEntries;
    }

    public List<UpdateLogEntry> loadUpdateLogsInRange(String tableId, long fromSeq, long toSeq)
            throws IOException {
        long snapshotId =
                snapshotManager.latestSnapshot(tableId).map(Snapshot::snapshotId).orElse(0L);
        List<UpdateLogEntry> allEntries = loadUpdateLogs(tableId, snapshotId);

        List<UpdateLogEntry> filtered =
                allEntries.stream()
                        .filter(e -> e.sequenceNumber() >= fromSeq && e.sequenceNumber() <= toSeq)
                        .toList();

        log.info(
                "Loaded {} UpdateLog entries for table={} in range [{}, {}]",
                filtered.size(),
                tableId,
                fromSeq,
                toSeq);
        return filtered;
    }

    public void markCompacted(List<String> entryIds) {
        // No-op: NWL 文件删除即等价于 markCompacted。Compactor 删除 archive 文件后，entry 自动失效。
        log.debug(
                "markCompacted called for {} entries (no-op, archive file deletion is the source of truth)",
                entryIds.size());
    }

    public boolean needsCompaction(String tableId, long snapshotId) throws IOException {
        Path tableDir = getTableDir(tableId);
        if (!Files.exists(tableDir)) return false;
        long archiveCount = 0;
        long archiveBytes = 0;
        try (var files = Files.list(tableDir)) {
            for (Path p : files.toList()) {
                String name = p.getFileName().toString();
                if (name.startsWith("archive-") && name.endsWith(".nwl")) {
                    archiveCount++;
                    archiveBytes += Files.size(p);
                }
            }
        }
        boolean needs = archiveCount >= 5 || archiveBytes >= 256L * 1024 * 1024;
        if (needs) {
            log.info(
                    "Archive compaction needed for table={}: {} archives, {} bytes",
                    tableId,
                    archiveCount,
                    archiveBytes);
        }
        return needs;
    }

    public void rolloverIfNecessary(String tableId) throws IOException {
        ReentrantLock tableLock =
                tableWriteLocks.computeIfAbsent(tableId, k -> new ReentrantLock());
        tableLock.lock();
        try {
            ActiveUpdateLogFile active = activeFiles.get(tableId);
            if (active == null) return;
            long size = active.getFileSize();
            long count = active.getEntryCount();
            if (size >= rolloverSizeBytes || count >= rolloverEntryCount) {
                Path archive = active.rollover();
                log.info(
                        "Rollover triggered for table={}: size={} count={} archive={}",
                        tableId,
                        size,
                        count,
                        archive);
            }
        } finally {
            tableLock.unlock();
        }
    }

    public void recoverPendingLogs() throws IOException {
        Path basePath = Path.of(updateLogBasePath);
        if (!Files.exists(basePath)) {
            return;
        }
        try (var tableDirs = Files.list(basePath)) {
            for (Path tableDir : tableDirs.toList()) {
                if (!Files.isDirectory(tableDir)) continue;
                String tableId = tableDir.getFileName().toString();
                recoverTable(tableId, tableDir);
            }
        }
    }

    private void recoverTable(String tableId, Path tableDir) throws IOException {
        Map<Long, Long> commitTsByTxn = new HashMap<>();
        Map<Long, Long> abortTsByTxn = new HashMap<>();
        Map<Long, List<UpdateLogEntry>> entriesByTxn = new HashMap<>();
        long maxCommittedSeq = 0L;

        try (var files = Files.list(tableDir)) {
            List<Path> sorted = files.filter(p -> p.toString().endsWith(".nwl")).sorted().toList();
            for (Path nwlFile : sorted) {
                UpdateLogReader.TxnRecoverySummary summary = reader.scanTxnRecords(nwlFile);
                commitTsByTxn.putAll(summary.commitTsByTxn());
                abortTsByTxn.putAll(summary.abortTsByTxn());

                UpdateLog fake =
                        new UpdateLog(
                                nwlFile.toString(),
                                RawWiseFile.FileFormat.NWL,
                                0L,
                                0L,
                                tableId,
                                0L,
                                0L,
                                0L);
                List<UpdateLogEntry> entries = reader.read(fake);
                for (UpdateLogEntry entry : entries) {
                    entriesByTxn
                            .computeIfAbsent(entry.transactionId(), k -> new ArrayList<>())
                            .add(entry);
                }
            }
        }

        for (Map.Entry<Long, List<UpdateLogEntry>> e : entriesByTxn.entrySet()) {
            long txnId = e.getKey();
            if (commitTsByTxn.containsKey(txnId)) {
                for (UpdateLogEntry entry : e.getValue()) {
                    if (entry.sequenceNumber() > maxCommittedSeq) {
                        maxCommittedSeq = entry.sequenceNumber();
                    }
                }
            } else if (!abortTsByTxn.containsKey(txnId)) {
                try {
                    appendAbortRecord(tableId, txnId);
                    log.info(
                            "Recovery: appended abort for uncommitted txn={} table={}",
                            txnId,
                            tableId);
                } catch (IOException ex) {
                    log.warn(
                            "Recovery: failed to append abort for txn={} table={}: {}",
                            txnId,
                            tableId,
                            ex.getMessage());
                }
            }
        }

        activeMaxSeqByLatest.set(maxCommittedSeq);
        log.info(
                "Recovery complete for table={}: maxCommittedSeq={} txnsTracked={}",
                tableId,
                maxCommittedSeq,
                entriesByTxn.size());
    }

    private ActiveUpdateLogFile getOrCreateActiveFile(String tableId) throws IOException {
        ActiveUpdateLogFile existing = activeFiles.get(tableId);
        if (existing != null && !existing.isClosed()) {
            return existing;
        }
        synchronized (activeFiles) {
            existing = activeFiles.get(tableId);
            if (existing != null && !existing.isClosed()) {
                return existing;
            }
            Path activePath = getActivePath(tableId);
            ActiveUpdateLogFile newFile = new ActiveUpdateLogFile(activePath, tableId);
            activeFiles.put(tableId, newFile);
            return newFile;
        }
    }

    private Path getActivePath(String tableId) {
        return Path.of(updateLogBasePath, tableId, "active.nwl");
    }

    private Path getTableDir(String tableId) {
        return Path.of(updateLogBasePath, tableId);
    }

    private long resolveActiveMaxSeq(long snapshotId) {
        if (snapshotId <= 0) {
            return activeMaxSeqByLatest.get();
        }
        Long snapMax = snapshotActiveMaxSeq.get(snapshotId);
        if (snapMax != null) return snapMax;
        return activeMaxSeqByLatest.get();
    }

    private static String inferTableIdFromPath(String filePath) {
        if (filePath == null) return null;
        java.nio.file.Path p = Path.of(filePath);
        java.nio.file.Path parent = p.getParent();
        if (parent == null) return null;
        String dirName = parent.getFileName().toString();
        if ("active.nwl".equals(p.getFileName().toString()) || dirName.startsWith("archive-")) {
            java.nio.file.Path grandParent = parent.getParent();
            if (grandParent != null) {
                return grandParent.getFileName().toString();
            }
        }
        return dirName;
    }
}
