package io.github.rawwise.engine.compaction.cleanup;

import io.github.rawwise.common.catalog.Keys;
import io.github.rawwise.common.collection.iterator.CloseableIterator;
import io.github.rawwise.engine.clean.CleanMetrics;
import io.github.rawwise.engine.compaction.gc.SnapshotRefCollector;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileGarbageCollector {

    private static final long DEFAULT_MAX_GC_ENTRY_AGE_MS = 604800000L;
    private static final int DEFAULT_MAX_PENDING_GC_SIZE = 10000;
    private static final int DEFAULT_MAX_DEAD_LETTER_SIZE = 1000;
    private static final int MAX_GC_RETRIES = 3;
    private static final long DEAD_LETTER_TTL_MS = 604800000L;

    private final Storage storage;
    private final SnapshotManager snapshotManager;
    private final SnapshotRefCollector snapshotRefCollector;
    private final Metastore metastore;
    private final ConcurrentLinkedQueue<PendingGcEntry> pendingGc = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<DeadLetterEntry> deadLetterEntries =
            new ConcurrentLinkedQueue<>();
    private final long maxGcEntryAgeMs;
    private final int maxPendingGcSize;
    private final int maxDeadLetterSize;
    private volatile CleanMetrics metrics;

    public FileGarbageCollector(
            Storage storage, SnapshotManager snapshotManager, Metastore metastore) {
        this(
                storage,
                snapshotManager,
                null,
                metastore,
                DEFAULT_MAX_GC_ENTRY_AGE_MS,
                DEFAULT_MAX_PENDING_GC_SIZE);
    }

    public FileGarbageCollector(
            Storage storage,
            SnapshotManager snapshotManager,
            SnapshotRefCollector snapshotRefCollector,
            Metastore metastore,
            long maxGcEntryAgeMs,
            int maxPendingGcSize) {
        this(
                storage,
                snapshotManager,
                snapshotRefCollector,
                metastore,
                maxGcEntryAgeMs,
                maxPendingGcSize,
                DEFAULT_MAX_DEAD_LETTER_SIZE);
    }

    public FileGarbageCollector(
            Storage storage,
            SnapshotManager snapshotManager,
            SnapshotRefCollector snapshotRefCollector,
            Metastore metastore,
            long maxGcEntryAgeMs,
            int maxPendingGcSize,
            int maxDeadLetterSize) {
        this.storage = storage;
        this.snapshotManager = snapshotManager;
        this.snapshotRefCollector = snapshotRefCollector;
        this.metastore = metastore;
        this.maxGcEntryAgeMs = maxGcEntryAgeMs;
        this.maxPendingGcSize = maxPendingGcSize;
        this.maxDeadLetterSize = maxDeadLetterSize;
    }

    public void setMetrics(CleanMetrics metrics) {
        this.metrics = metrics;
    }

    public void scheduleGc(String tableId, List<String> filePaths, long obsoleteAfterSnapshotId) {
        if (pendingGc.size() >= maxPendingGcSize) {
            log.warn(
                    "GC queue full (size={}), forcing GC cycle before scheduling new entries for table={}",
                    pendingGc.size(),
                    tableId);
            runGcCycle(tableId);
        }
        if (pendingGc.size() >= maxPendingGcSize) {
            long createdAt = System.currentTimeMillis();
            for (String path : filePaths) {
                addDeadLetter(
                        new DeadLetterEntry(
                                path,
                                tableId,
                                obsoleteAfterSnapshotId,
                                createdAt,
                                "GC queue full after forced cycle (size="
                                        + pendingGc.size()
                                        + ")"));
                log.error("GC dead-letter: entry={} table={} reason=GC queue full", path, tableId);
            }
            return;
        }

        long now = System.currentTimeMillis();

        if (metastore != null) {
            List<Mutation> batch = new ArrayList<>();
            for (String path : filePaths) {
                GcEntry entry = GcEntry.pending(path, tableId, obsoleteAfterSnapshotId, now);
                byte[] key = Keys.gcEntryKey(tableId, obsoleteAfterSnapshotId, path);
                batch.add(new Mutation.Put(key, entry.toBytes()));
            }
            if (!batch.isEmpty()) {
                try {
                    metastore.batchWrite(batch);
                } catch (IOException e) {
                    log.error(
                            "Failed to persist GC entries for table={}, {} paths will not survive restart",
                            tableId,
                            filePaths.size(),
                            e);
                }
            }
        }

        for (String path : filePaths) {
            pendingGc.add(new PendingGcEntry(path, tableId, obsoleteAfterSnapshotId, now));
        }
        log.debug(
                "Scheduled GC for {} files in table={}, obsoleteAfterSnapshotId={}",
                filePaths.size(),
                tableId,
                obsoleteAfterSnapshotId);
    }

    public void recoverFromMetastore(String tableId) throws IOException {
        if (metastore == null) {
            log.warn("Metastore not configured, cannot recover GC entries for table={}", tableId);
            return;
        }
        byte[] prefix = Keys.gcEntryPrefix(tableId);
        List<byte[]> expiredKeys = new ArrayList<>();
        try (CloseableIterator<KeyValue> it = metastore.scan(prefix)) {
            while (it.hasNext()) {
                KeyValue kv = it.next();
                try {
                    GcEntry entry = GcEntry.fromBytes(kv.value());
                    long age = System.currentTimeMillis() - entry.createdAtMs();
                    if (entry.state() == GcEntry.STATE_DEAD_LETTER
                            && age > entry.deadLetterTtlMs()) {
                        expiredKeys.add(kv.key());
                        continue;
                    }
                    if (entry.state() == GcEntry.STATE_PENDING) {
                        pendingGc.add(
                                new PendingGcEntry(
                                        entry.filePath(),
                                        entry.tableId(),
                                        entry.obsoleteAfterSnapshotId(),
                                        entry.createdAtMs()));
                    }
                } catch (Exception e) {
                    log.warn("Failed to parse GC entry from Metastore for table={}", tableId, e);
                }
            }
        }
        if (!expiredKeys.isEmpty()) {
            List<Mutation> cleanupBatch = new ArrayList<>();
            for (byte[] key : expiredKeys) {
                cleanupBatch.add(new Mutation.Delete(key));
            }
            metastore.batchWrite(cleanupBatch);
            log.info("Cleaned up {} expired GC entries for table={}", expiredKeys.size(), tableId);
        }
        log.info("Recovered GC entries for table={} from Metastore", tableId);
    }

    public void runGcCycle(String tableId) {
        long latestSnapshotId;
        try {
            latestSnapshotId =
                    snapshotManager.latestSnapshot(tableId).map(s -> s.snapshotId()).orElse(-1L);
        } catch (IOException e) {
            log.error("Failed to get latest snapshot for GC cycle, tableId={}", tableId, e);
            return;
        }
        long now = System.currentTimeMillis();

        pendingGc.removeIf(
                entry -> {
                    if (!entry.tableId.equals(tableId)) return false;
                    if (now - entry.createdAt > maxGcEntryAgeMs) {
                        boolean snapshotSafe = latestSnapshotId > entry.obsoleteAfterSnapshotId + 1;
                        if (snapshotSafe) {
                            return deleteFile(
                                    entry.filePath, entry.tableId, entry.obsoleteAfterSnapshotId);
                        } else {
                            addDeadLetter(
                                    new DeadLetterEntry(
                                            entry.filePath,
                                            entry.tableId,
                                            entry.obsoleteAfterSnapshotId,
                                            entry.createdAt,
                                            "TTL-expired but not snapshot-safe"));
                            log.warn(
                                    "GC: TTL-expired entry {} not snapshot-safe (latestSnapshotId={}, obsoleteAfterSnapshotId={})",
                                    entry.filePath,
                                    latestSnapshotId,
                                    entry.obsoleteAfterSnapshotId);
                        }
                        return true;
                    }
                    if (latestSnapshotId > entry.obsoleteAfterSnapshotId + 1) {
                        return deleteFile(
                                entry.filePath, entry.tableId, entry.obsoleteAfterSnapshotId);
                    }
                    return false;
                });

        deadLetterEntries.removeIf(
                entry -> {
                    if (!entry.tableId().equals(tableId)) return false;
                    if (latestSnapshotId > entry.obsoleteAfterSnapshotId() + 1) {
                        if (deleteFile(
                                entry.filePath(),
                                entry.tableId(),
                                entry.obsoleteAfterSnapshotId())) {
                            log.info("Dead-letter entry auto-cleaned: {}", entry.filePath());
                            return true;
                        }
                    }
                    return false;
                });
    }

    private boolean deleteFile(String filePath, String tableId, long obsoleteAfterSnapshotId) {
        try {
            Storage.StoragePath sp = StoragePathResolver.resolvePath(filePath);
            StoragePathValidator.validate(sp, tableId);
            if (storage.exists(sp)) {
                long fileSize = 0L;
                try {
                    Storage.StorageStatus status = storage.status(sp);
                    fileSize = status.length();
                } catch (IOException e) {
                    fileSize = 4096L;
                }
                storage.delete(sp);
                if (metrics != null) {
                    metrics.addBytesPhysicallyDeleted(fileSize);
                }
                log.debug("GC: deleted {}", filePath);
            }
            if (metastore != null) {
                byte[] key = Keys.gcEntryKey(tableId, obsoleteAfterSnapshotId, filePath);
                metastore.delete(key);
            }
            return true;
        } catch (SecurityException e) {
            log.warn("GC path security violation for table={}: {}", tableId, e.getMessage());
            markDeadLetter(filePath, tableId, obsoleteAfterSnapshotId, e.getMessage());
            return true;
        } catch (IOException e) {
            if (metastore != null) {
                byte[] key = Keys.gcEntryKey(tableId, obsoleteAfterSnapshotId, filePath);
                try {
                    Optional<byte[]> val = metastore.get(key);
                    if (val.isPresent()) {
                        GcEntry entry = GcEntry.fromBytes(val.get());
                        if (entry.retryCount() >= entry.maxRetries()) {
                            markDeadLetter(
                                    filePath, tableId, obsoleteAfterSnapshotId, e.getMessage());
                            return true;
                        }
                        GcEntry updated = entry.withIncrementedRetry();
                        metastore.put(key, updated.toBytes());
                    } else {
                        GcEntry newEntry =
                                GcEntry.pending(
                                        filePath,
                                        tableId,
                                        obsoleteAfterSnapshotId,
                                        System.currentTimeMillis());
                        newEntry = newEntry.withIncrementedRetry();
                        metastore.put(key, newEntry.toBytes());
                    }
                } catch (IOException ex) {
                    log.warn("Failed to update GC retry count for {}", filePath, ex);
                }
            }
            log.warn("GC failed for {}, will retry next cycle", filePath, e);
            return false;
        }
    }

    private void markDeadLetter(
            String filePath, String tableId, long obsoleteAfterSnapshotId, String reason) {
        addDeadLetter(
                new DeadLetterEntry(
                        filePath,
                        tableId,
                        obsoleteAfterSnapshotId,
                        System.currentTimeMillis(),
                        reason));
        if (metastore != null) {
            byte[] key = Keys.gcEntryKey(tableId, obsoleteAfterSnapshotId, filePath);
            try {
                GcEntry entry =
                        GcEntry.pending(
                                        filePath,
                                        tableId,
                                        obsoleteAfterSnapshotId,
                                        System.currentTimeMillis())
                                .asDeadLetter(reason);
                metastore.put(key, entry.toBytes());
            } catch (IOException e) {
                log.error("Failed to persist dead-letter GC entry for {}", filePath, e);
            }
        }
    }

    private void addDeadLetter(DeadLetterEntry entry) {
        deadLetterEntries.add(entry);
        if (metrics != null) {
            metrics.addBytesInDeadLetter(4096L);
        }
        while (deadLetterEntries.size() > maxDeadLetterSize) {
            DeadLetterEntry evicted = deadLetterEntries.poll();
            if (evicted != null) {
                log.warn(
                        "Dead-letter queue overflow, evicting oldest entry: {}",
                        evicted.filePath());
            }
        }
    }

    public void runGcCycleAllTables() {
        pendingGc.stream().map(e -> e.tableId).distinct().forEach(this::runGcCycle);
    }

    public List<DeadLetterEntry> getDeadLetterEntries() {
        return Collections.unmodifiableList(new ArrayList<>(deadLetterEntries));
    }

    public void clearDeadLetterEntries() {
        deadLetterEntries.clear();
    }

    public record DeadLetterEntry(
            String filePath,
            String tableId,
            long obsoleteAfterSnapshotId,
            long createdAt,
            String reason) {}

    private static class PendingGcEntry {
        final String filePath;
        final String tableId;
        final long obsoleteAfterSnapshotId;
        final long createdAt;

        PendingGcEntry(
                String filePath, String tableId, long obsoleteAfterSnapshotId, long createdAt) {
            this.filePath = filePath;
            this.tableId = tableId;
            this.obsoleteAfterSnapshotId = obsoleteAfterSnapshotId;
            this.createdAt = createdAt;
        }
    }
}
