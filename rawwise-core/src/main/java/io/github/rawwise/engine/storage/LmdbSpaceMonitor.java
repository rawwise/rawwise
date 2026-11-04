package io.github.rawwise.engine.storage;

import io.github.rawwise.engine.spi.FaultInjector;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.*;

@Slf4j
public class LmdbSpaceMonitor {
    private final Env<ByteBuffer> env;
    private long avgEntrySize = 1;
    private final double warnThreshold;
    private final double errorThreshold;
    private final AtomicBoolean spaceExhausted = new AtomicBoolean(false);
    private final FaultInjector faultInjector;

    public LmdbSpaceMonitor(Env<ByteBuffer> env) {
        this(env, 0.8, 0.9);
    }

    public LmdbSpaceMonitor(Env<ByteBuffer> env, double warnThreshold, double errorThreshold) {
        this(env, warnThreshold, errorThreshold, null);
    }

    public LmdbSpaceMonitor(
            Env<ByteBuffer> env,
            double warnThreshold,
            double errorThreshold,
            FaultInjector faultInjector) {
        this.env = env;
        this.warnThreshold = warnThreshold;
        this.errorThreshold = errorThreshold;
        this.faultInjector = faultInjector != null ? faultInjector : FaultInjector.NOOP;
    }

    public record SpaceInfo(
            long mapSizeBytes,
            long usedBytes,
            long freeBytes,
            double usagePercentage,
            long freePages,
            long totalPages,
            boolean hasSpace,
            long entriesPerPage,
            long estimatedRemainingCapacity) {}

    public void refresh(long avgEntrySize) {
        this.avgEntrySize = avgEntrySize;
    }

    public SpaceInfo getSpaceInfo() {
        try (Txn<ByteBuffer> txn = env.txnRead()) {

            EnvInfo envInfo = env.info();
            Stat stat = env.stat();

            long mapSize = envInfo.mapSize;
            long pageSize = stat.pageSize;
            long totalPages = envInfo.lastPageNumber + 1;
            long usedPages = calculateUsedPages(stat);
            long freePages = totalPages - usedPages;
            long entriesPerPage = pageSize / avgEntrySize;
            long usedBytes = usedPages * pageSize;
            long freeBytes = freePages * pageSize;
            double usagePercentage = (double) usedBytes / mapSize * 100;
            double usageRatio = (double) usedBytes / mapSize;
            long estimatedCapacity = freePages * entriesPerPage;

            faultInjector.inject(FaultInjector.InjectionPoint.BEFORE_LMDB_SPACE_CHECK);
            checkThresholds(usageRatio);

            return new SpaceInfo(
                    mapSize,
                    usedBytes,
                    freeBytes,
                    usagePercentage,
                    freePages,
                    totalPages,
                    freePages > 10,
                    entriesPerPage,
                    estimatedCapacity);
        }
    }

    private void checkThresholds(double usageRatio) {
        if (usageRatio >= 0.95) {
            spaceExhausted.set(true);
            log.error(
                    "LMDB space CRITICAL: usage ratio {}% exceeds 95%, write requests will be rejected",
                    String.format("%.2f", usageRatio * 100));
        } else if (usageRatio >= errorThreshold) {
            spaceExhausted.set(false);
            log.error(
                    "LMDB space ERROR: usage ratio {}% exceeds {}% threshold",
                    String.format("%.2f", usageRatio * 100),
                    String.format("%.0f", errorThreshold * 100));
        } else if (usageRatio >= warnThreshold) {
            spaceExhausted.set(false);
            log.warn(
                    "LMDB space WARN: usage ratio {}% exceeds {}% threshold",
                    String.format("%.2f", usageRatio * 100),
                    String.format("%.0f", warnThreshold * 100));
        } else {
            spaceExhausted.set(false);
        }
    }

    public boolean isSpaceExhausted() {
        return spaceExhausted.get();
    }

    private long calculateUsedPages(Stat stat) {
        return stat.branchPages + stat.leafPages + stat.overflowPages;
    }

    public long getPhysicalFileSize(String dbPath) {
        try {
            Path dataFile = Path.of(dbPath);
            if (Files.exists(dataFile)) {
                return Files.size(dataFile);
            }
        } catch (Exception e) {
            log.warn("Error getting physical file size: {}", e.getMessage());
        }
        return 0;
    }
}
