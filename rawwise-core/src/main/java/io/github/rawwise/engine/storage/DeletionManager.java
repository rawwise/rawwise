package io.github.rawwise.engine.storage;

import io.github.rawwise.common.catalog.Keys;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeletionManager implements AutoCloseable {

    private static final int MAX_RETRY_COUNT = 10;

    private final DelayQueue<PendingDeletionItem> deletionQueue = new DelayQueue<>();
    private final Thread deletionThread;
    private final LongConsumer memoryReleaser;
    private final long instanceMappedBytes;
    private volatile Metastore metastore;
    private volatile String basePath;
    private volatile boolean running = true;

    public DeletionManager(
            LongConsumer memoryReleaser,
            long instanceMappedBytes,
            Metastore metastore,
            String basePath) {
        this.memoryReleaser = memoryReleaser;
        this.instanceMappedBytes = instanceMappedBytes;
        this.metastore = metastore;
        this.basePath = basePath;
        this.deletionThread = new Thread(this::processDeletions, "Lmdb-Deletion-Thread");
        this.deletionThread.setDaemon(true);
        this.deletionThread.start();
    }

    public DeletionManager(LongConsumer memoryReleaser, long instanceMappedBytes) {
        this(memoryReleaser, instanceMappedBytes, null, null);
    }

    public void setMetastore(Metastore metastore) {
        this.metastore = metastore;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    public void scheduleDeletion(LmdbInstance instance, long delayMs, FlushedInstanceMark mark) {
        deletionQueue.offer(
                new PendingDeletionItem(instance, System.currentTimeMillis() + delayMs, 0, mark));
    }

    public void recoverPendingDeletions() {
        if (metastore == null || basePath == null) return;
        log.info("Recovering pending instance deletions from metastore...");
        int recovered = 0;
        try {
            for (KeyValue kv : metastore.scanAll(Keys.flushedInstanceScanPrefix())) {
                try {
                    FlushedInstanceMark mark = FlushedInstanceMark.fromBytes(kv.value());
                    if (mark.status() == FlushedInstanceMark.FlushStatus.FLUSHED
                            || mark.status() == FlushedInstanceMark.FlushStatus.DELETING) {
                        Path instanceDir = Paths.get(basePath, mark.tableId(), mark.instanceId());
                        if (Files.exists(instanceDir)) {
                            deleteDirectory(instanceDir);
                            log.info(
                                    "Recovered deletion: removed flushed instance dir: {}",
                                    instanceDir);
                            recovered++;
                        }
                        metastore.put(
                                kv.key(),
                                mark.withStatus(FlushedInstanceMark.FlushStatus.DELETED).toBytes());
                    }
                } catch (Exception e) {
                    log.warn("Failed to recover deletion for flushed instance", e);
                }
            }
        } catch (IOException e) {
            log.warn("Failed to scan flushed instance marks from metastore", e);
        }
        if (recovered > 0) {
            log.info("Recovered and executed {} pending instance deletions", recovered);
            memoryReleaser.accept((long) recovered * instanceMappedBytes);
        }
    }

    public void stop() {
        running = false;
        deletionThread.interrupt();
        try {
            deletionThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        PendingDeletionItem item;
        while ((item = deletionQueue.poll()) != null) {
            try {
                if (item.instance != null) {
                    item.instance.destroy();
                }
                memoryReleaser.accept(instanceMappedBytes);
            } catch (Exception e) {
                log.warn(
                        "Failed to destroy instance during shutdown: {}",
                        item.mark != null ? item.mark.instanceId() : "unknown",
                        e);
            }
        }
    }

    @Override
    public void close() {
        stop();
    }

    public int pendingCount() {
        return deletionQueue.size();
    }

    private void processDeletions() {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                PendingDeletionItem item = deletionQueue.take();
                LmdbInstance inst = item.instance;

                if (inst != null) {
                    try {
                        log.info("Destroying physical files for instance {}", inst.instanceId());
                        inst.destroy();
                        memoryReleaser.accept(instanceMappedBytes);
                        updateMarkAsDeleted(item.mark);
                    } catch (Exception e) {
                        log.error(
                                "Failed to destroy instance {}, re-queuing for retry",
                                inst.instanceId(),
                                e);
                        if (item.retryCount < MAX_RETRY_COUNT) {
                            long delay = Math.min(10_000L * (1L << item.retryCount), 300_000L);
                            deletionQueue.offer(
                                    new PendingDeletionItem(
                                            inst,
                                            System.currentTimeMillis() + delay,
                                            item.retryCount + 1,
                                            item.mark));
                        } else {
                            log.error(
                                    "Instance {} exceeded max deletion retries, releasing memory count",
                                    inst.instanceId());
                            memoryReleaser.accept(instanceMappedBytes);
                            updateMarkAsDeleted(item.mark);
                        }
                    }
                } else if (item.mark != null) {
                    Path instanceDir =
                            Paths.get(basePath, item.mark.tableId(), item.mark.instanceId());
                    if (Files.exists(instanceDir)) {
                        deleteDirectory(instanceDir);
                        log.info("Deleted instance dir (no LmdbInstance): {}", instanceDir);
                    }
                    memoryReleaser.accept(instanceMappedBytes);
                    updateMarkAsDeleted(item.mark);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void updateMarkAsDeleted(FlushedInstanceMark mark) {
        if (mark == null || metastore == null) return;
        try {
            metastore.put(
                    Keys.flushedInstanceKey(mark.tableId(), mark.instanceId()),
                    mark.withStatus(FlushedInstanceMark.FlushStatus.DELETED).toBytes());
        } catch (IOException e) {
            log.warn("Failed to update flushed instance mark to DELETED: {}", mark.instanceId(), e);
        }
    }

    private void deleteDirectory(Path dir) {
        try (Stream<Path> stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder())
                    .forEach(
                            p -> {
                                try {
                                    Files.deleteIfExists(p);
                                } catch (IOException ignored) {
                                }
                            });
        } catch (IOException e) {
            log.warn("Failed to delete directory: {}", dir, e);
        }
    }

    record PendingDeletionItem(
            LmdbInstance instance, long expireTime, int retryCount, FlushedInstanceMark mark)
            implements Delayed {
        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(expireTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            if (o instanceof PendingDeletionItem other) {
                return Long.compare(expireTime, other.expireTime);
            }
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
        }
    }
}
