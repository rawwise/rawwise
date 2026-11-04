package io.github.rawwise.engine.clean.strategy;

import io.github.rawwise.engine.clean.CleanAction;
import io.github.rawwise.engine.clean.CleanContext;
import io.github.rawwise.engine.clean.CleanMetrics;
import io.github.rawwise.engine.clean.DeleteFileAction;
import io.github.rawwise.engine.clean.RewriteFileAction;
import io.github.rawwise.engine.compaction.merger.TimestampCodec;
import io.github.rawwise.engine.storage.ColumnStatistics;
import io.github.rawwise.engine.storage.DataFile;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TTLExpiryStrategy implements CleanStrategy {

    private static final long serialVersionUID = 1L;
    private static final long MILLIS_PER_DAY = 86400000L;

    private final int ttlDays;
    private final String expireColumn;
    private final CleanMetrics metrics;

    public TTLExpiryStrategy(int ttlDays, String expireColumn, CleanMetrics metrics) {
        if (ttlDays <= 0) {
            throw new IllegalArgumentException("ttlDays must be positive");
        }
        if (expireColumn == null || expireColumn.isEmpty()) {
            throw new IllegalArgumentException("expireColumn must not be null or empty");
        }
        this.ttlDays = ttlDays;
        this.expireColumn = expireColumn;
        this.metrics = metrics;
    }

    @Override
    public StrategyType strategyType() {
        return StrategyType.TTL_EXPIRY;
    }

    @Override
    public List<CleanAction> plan(CleanContext context) {
        long expiryThresholdMs = context.currentTimestampMs() - (long) ttlDays * MILLIS_PER_DAY;
        List<CleanAction> actions = new ArrayList<>();
        boolean warnedColumnNotFound = false;

        for (DataFile file : context.dataFiles()) {
            ColumnStatistics stats = file.columnStatistics();
            if (stats == null || stats.columns() == null) {
                log.warn("TTL: skipping file {} — no ColumnStatistics", file.filePath());
                continue;
            }
            ColumnStatistics.ColumnStat tsCol = stats.columns().get(expireColumn);
            if (tsCol == null) {
                if (metrics != null) {
                    metrics.recordTTLColumnNotFound();
                }
                if (!warnedColumnNotFound) {
                    log.warn(
                            "TTL: column '{}' not found in ColumnStatistics — TTL may not be taking effect. "
                                    + "First file affected: {}",
                            expireColumn,
                            file.filePath());
                    warnedColumnNotFound = true;
                } else {
                    log.debug(
                            "TTL: column '{}' not found for file {}",
                            expireColumn,
                            file.filePath());
                }
                continue;
            }
            if (tsCol.maxBytes() == null
                    || tsCol.maxBytes().length == 0
                    || tsCol.minBytes() == null
                    || tsCol.minBytes().length == 0) {
                log.warn(
                        "TTL: skipping file {} — empty min/max bytes for column '{}'",
                        file.filePath(),
                        expireColumn);
                continue;
            }
            if (!TimestampCodec.isTimestampType(tsCol.logicalType())) {
                log.warn(
                        "TTL: skipping file {} — logicalType '{}' is not a timestamp type",
                        file.filePath(),
                        tsCol.logicalType());
                continue;
            }
            long maxTs = TimestampCodec.deserializeToMillis(tsCol.maxBytes(), tsCol.logicalType());
            long minTs = TimestampCodec.deserializeToMillis(tsCol.minBytes(), tsCol.logicalType());
            if (maxTs == Long.MIN_VALUE || minTs == Long.MIN_VALUE) {
                log.warn(
                        "TTL: skipping file {} — invalid timestamp (MIN_VALUE indicates corrupt stats)",
                        file.filePath());
                continue;
            }
            if (maxTs < expiryThresholdMs) {
                actions.add(new DeleteFileAction(file.filePath(), file.fileSizeBytes()));
            } else if (minTs < expiryThresholdMs) {
                long estimatedExpiredBytes =
                        (long)
                                (file.fileSizeBytes()
                                        * ((double) (expiryThresholdMs - minTs) / (maxTs - minTs)));
                actions.add(
                        new RewriteFileAction(
                                file.filePath(),
                                expireColumn,
                                tsCol.logicalType(),
                                expiryThresholdMs,
                                estimatedExpiredBytes,
                                file.fileSizeBytes()));
            }
        }
        return actions;
    }

    public int ttlDays() {
        return ttlDays;
    }

    public String expireColumn() {
        return expireColumn;
    }
}
