package io.github.rawwise.metastore.backup;

import io.github.rawwise.metastore.RocksdbMetastore;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetastoreBackupManager implements AutoCloseable {
    private final RocksdbMetastore metastore;
    private final String backupPath;
    private final int intervalHours;
    private final ScheduledExecutorService scheduler;

    public MetastoreBackupManager(
            RocksdbMetastore metastore, String backupPath, int intervalHours) {
        this.metastore = metastore;
        this.backupPath = backupPath;
        this.intervalHours = intervalHours;
        this.scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            var t = new Thread(r, "metastore-backup");
                            t.setDaemon(false);
                            return t;
                        });
    }

    public void start() {
        if (backupPath == null || backupPath.isBlank()) {
            log.warn("Metastore backup path not configured, backup disabled");
            return;
        }
        scheduler.scheduleAtFixedRate(
                this::performBackup, intervalHours, intervalHours, TimeUnit.HOURS);
        log.info("Metastore backup scheduled every {} hours to {}", intervalHours, backupPath);
    }

    public void performBackup() {
        try {
            String timestamp =
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
            String targetPath = backupPath + "/checkpoint-" + timestamp;
            log.info("Creating metastore checkpoint at {}", targetPath);
            metastore.createCheckpoint(targetPath);
            log.info("Metastore checkpoint created successfully at {}", targetPath);
        } catch (Exception e) {
            log.error("Metastore backup failed", e);
        }
    }

    @Override
    public void close() {
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
