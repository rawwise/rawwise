package io.github.rawwise.engine.compaction.cleanup;

import io.github.rawwise.engine.storage.spi.Storage;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileCleanupManager {

    private static final long DEFERRED_GC_DELAY_SECONDS = 60;

    private final Storage storage;
    private final DelayQueue<DeferredGcEntry> deferredGcQueue = new DelayQueue<>();
    private final Thread gcDaemon;
    private volatile boolean running = true;

    public FileCleanupManager(Storage storage) {
        this.storage = storage;
        this.gcDaemon = new Thread(this::processDeferredGc, "file-deferred-gc");
        this.gcDaemon.setDaemon(true);
        this.gcDaemon.start();
    }

    public void immediateCleanup(List<String> newFilePaths) {
        for (String path : newFilePaths) {
            try {
                Storage.StoragePath sp = StoragePathResolver.resolvePath(path);
                if (storage.exists(sp)) {
                    storage.delete(sp);
                }
                log.debug("Immediate cleanup: deleted {}", path);
            } catch (IOException e) {
                log.warn("Failed immediate cleanup of {}, will retry later", path, e);
            }
        }
    }

    public void deleteOldFiles(List<String> oldFilePaths) {
        for (String path : oldFilePaths) {
            try {
                Storage.StoragePath sp = StoragePathResolver.resolvePath(path);
                if (storage.exists(sp)) {
                    storage.delete(sp);
                }
                log.debug("Old file GC: deleted {}", path);
            } catch (IOException e) {
                log.warn("Failed old file GC for {}", path, e);
            }
        }
    }

    public void scheduleDeferredGc(List<String> oldFilePaths) {
        for (String path : oldFilePaths) {
            deferredGcQueue.put(
                    new DeferredGcEntry(
                            path,
                            System.currentTimeMillis()
                                    + TimeUnit.SECONDS.toMillis(DEFERRED_GC_DELAY_SECONDS)));
        }
        log.debug("Scheduled deferred GC for {} files", oldFilePaths.size());
    }

    private void processDeferredGc() {
        while (running || !deferredGcQueue.isEmpty()) {
            try {
                DeferredGcEntry entry = deferredGcQueue.poll(1, TimeUnit.SECONDS);
                if (entry == null) continue;

                try {
                    Storage.StoragePath sp = StoragePathResolver.resolvePath(entry.filePath);
                    if (storage.exists(sp)) {
                        storage.delete(sp);
                        log.debug("Deferred GC: deleted {}", entry.filePath);
                    }
                } catch (IOException e) {
                    log.warn("Deferred GC failed for {}, re-queueing", entry.filePath, e);
                    entry.resetDelay(
                            System.currentTimeMillis()
                                    + TimeUnit.SECONDS.toMillis(DEFERRED_GC_DELAY_SECONDS));
                    deferredGcQueue.put(entry);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public void stop() {
        running = false;
        gcDaemon.interrupt();
    }

    private static class DeferredGcEntry implements Delayed {
        final String filePath;
        private long expiryMillis;

        DeferredGcEntry(String filePath, long expiryMillis) {
            this.filePath = filePath;
            this.expiryMillis = expiryMillis;
        }

        void resetDelay(long newExpiryMillis) {
            this.expiryMillis = newExpiryMillis;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(expiryMillis - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(this.expiryMillis, ((DeferredGcEntry) o).expiryMillis);
        }
    }
}
