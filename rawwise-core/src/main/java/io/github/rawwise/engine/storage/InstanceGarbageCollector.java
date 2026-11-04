package io.github.rawwise.engine.storage;

import io.github.rawwise.common.catalog.Keys;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InstanceGarbageCollector implements AutoCloseable {

    private static final long GC_INTERVAL_MINUTES = 30;
    private static final long DELETED_MARK_RETENTION_MS = 7L * 24 * 3600_000L;
    private static final long DELETED_MARK_MIN_AGE_MS = 5 * 60_000L;

    private final String basePath;
    private final Metastore metastore;
    private final SnapshotManager snapshotManager;
    private final ScheduledExecutorService scheduler;

    public InstanceGarbageCollector(
            String basePath, Metastore metastore, SnapshotManager snapshotManager) {
        this.basePath = basePath;
        this.metastore = metastore;
        this.snapshotManager = snapshotManager;
        this.scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "Instance-GC-Thread");
                            t.setDaemon(true);
                            return t;
                        });
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::runGcCycle, 10, GC_INTERVAL_MINUTES, TimeUnit.MINUTES);
        log.info(
                "InstanceGarbageCollector started: interval={}min, basePath={}",
                GC_INTERVAL_MINUTES,
                basePath);
    }

    public void runGcCycle() {
        if (metastore == null || basePath == null) return;
        int deleted = 0;
        int cleaned = 0;
        try {
            for (KeyValue kv : metastore.scanAll(Keys.flushedInstanceScanPrefix())) {
                try {
                    FlushedInstanceMark mark = FlushedInstanceMark.fromBytes(kv.value());
                    switch (mark.status()) {
                        case FLUSHED, DELETING -> {
                            if (isSnapshotSafe(mark)) {
                                Path instanceDir =
                                        Paths.get(basePath, mark.tableId(), mark.instanceId());
                                if (Files.exists(instanceDir)) {
                                    deleteDirectory(instanceDir);
                                    log.info(
                                            "InstanceGC: deleted flushed instance dir: {}",
                                            instanceDir);
                                    deleted++;
                                }
                                metastore.put(
                                        kv.key(),
                                        mark.withStatus(FlushedInstanceMark.FlushStatus.DELETED)
                                                .toBytes());
                            }
                        }
                        case DELETED -> {
                            if (System.currentTimeMillis() - mark.flushedAtMs()
                                    > DELETED_MARK_RETENTION_MS) {
                                metastore.delete(kv.key());
                                cleaned++;
                            }
                        }
                    }
                } catch (Exception e) {
                    log.warn("InstanceGC: failed to process flushed instance mark", e);
                }
            }
        } catch (IOException e) {
            log.warn("InstanceGC: failed to scan flushed instance marks", e);
        }
        if (deleted > 0 || cleaned > 0) {
            log.info(
                    "InstanceGC cycle complete: deleted {} dirs, cleaned {} metastore records",
                    deleted,
                    cleaned);
        }
    }

    private boolean isSnapshotSafe(FlushedInstanceMark mark) {
        if (mark.flushedSnapshotId() == -1L) {
            return System.currentTimeMillis() - mark.flushedAtMs() > DELETED_MARK_MIN_AGE_MS;
        }
        return System.currentTimeMillis() - mark.flushedAtMs() > DELETED_MARK_MIN_AGE_MS;
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
            log.warn("InstanceGC: failed to delete directory: {}", dir, e);
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
