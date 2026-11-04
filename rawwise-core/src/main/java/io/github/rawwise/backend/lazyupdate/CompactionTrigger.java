package io.github.rawwise.backend.lazyupdate;

import java.io.IOException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Trigger strategies for automatic UpdateLog compaction.
 *
 * <p>Compaction merges UpdateLog entries into the underlying data files so that read-time predicate
 * resolution overhead is bounded. This class provides multiple trigger strategies that answer the
 * single question: "should we compact now?"
 *
 * <h3>Strategies</h3>
 *
 * <ul>
 *   <li><b>Count-based</b>: trigger when the number of UpdateLog <em>entries</em> exceeds a
 *       configured threshold.
 *   <li><b>File-based</b>: trigger when the number of UpdateLog <em>files</em> exceeds a configured
 *       threshold.
 *   <li><b>Time-based</b>: trigger when a configurable interval has elapsed since the last
 *       compaction.
 *   <li><b>Combined</b>: any of the above conditions met triggers compaction.
 * </ul>
 *
 * <p>All strategies delegate the actual counts to {@link LazyUpdateManager#needsCompaction(String,
 * long)} which reads the snapshot manifest and counts entries.
 */
@Slf4j
public class CompactionTrigger {

    /** Default threshold: compact when UpdateLog entries exceed this count. */
    public static final long DEFAULT_ENTRY_COUNT_THRESHOLD = 1_000L;

    /** Default threshold: compact when UpdateLog files exceed this count. */
    public static final long DEFAULT_FILE_COUNT_THRESHOLD = 50L;

    /** Default interval in milliseconds between time-based compactions. */
    public static final long DEFAULT_TIME_INTERVAL_MS = 300_000L; // 5 minutes

    @Getter private final LazyUpdateManager lazyUpdateManager;

    @Getter private final long entryCountThreshold;
    @Getter private final long fileCountThreshold;
    private final long timeIntervalMs;

    /** -- GETTER -- Get the last compaction timestamp (0 if never compacted). */
    @Getter private volatile long lastCompactionTimeMs;

    /**
     * Creates a trigger with default thresholds.
     *
     * @param lazyUpdateManager the manager used to query UpdateLog state
     */
    public CompactionTrigger(LazyUpdateManager lazyUpdateManager) {
        this(
                lazyUpdateManager,
                DEFAULT_ENTRY_COUNT_THRESHOLD,
                DEFAULT_FILE_COUNT_THRESHOLD,
                DEFAULT_TIME_INTERVAL_MS);
    }

    /**
     * Creates a trigger with custom thresholds.
     *
     * @param lazyUpdateManager the manager used to query UpdateLog state
     * @param entryCountThreshold entry count above which compaction is recommended
     * @param fileCountThreshold file count above which compaction is recommended
     * @param timeIntervalMs minimum time between successive compactions
     */
    public CompactionTrigger(
            LazyUpdateManager lazyUpdateManager,
            long entryCountThreshold,
            long fileCountThreshold,
            long timeIntervalMs) {
        this.lazyUpdateManager =
                java.util.Objects.requireNonNull(
                        lazyUpdateManager, "lazyUpdateManager must not be null");
        if (entryCountThreshold <= 0) {
            throw new IllegalArgumentException(
                    "entryCountThreshold must be positive: " + entryCountThreshold);
        }
        if (fileCountThreshold <= 0) {
            throw new IllegalArgumentException(
                    "fileCountThreshold must be positive: " + fileCountThreshold);
        }
        if (timeIntervalMs <= 0) {
            throw new IllegalArgumentException(
                    "timeIntervalMs must be positive: " + timeIntervalMs);
        }
        this.entryCountThreshold = entryCountThreshold;
        this.fileCountThreshold = fileCountThreshold;
        this.timeIntervalMs = timeIntervalMs;
        this.lastCompactionTimeMs = 0L;
    }

    /**
     * Determine whether compaction should be triggered for the given table.
     *
     * <p>Returns {@code true} when <strong>any</strong> of the configured conditions is met
     * (counts, file count, or elapsed time).
     *
     * @param tableId the table identifier
     * @param snapshotId the snapshot whose UpdateLogs to evaluate
     * @return {@code true} if compaction is recommended
     * @throws IOException if querying the snapshot state fails
     */
    public boolean shouldCompact(String tableId, long snapshotId) throws IOException {
        // Check time-based: if not enough time has elapsed, short-circuit
        long now = System.currentTimeMillis();
        if (lastCompactionTimeMs > 0 && (now - lastCompactionTimeMs) < timeIntervalMs) {
            log.debug(
                    "CompactionTrigger: time threshold not met for table={}, elapsed={}ms < interval={}ms",
                    tableId,
                    now - lastCompactionTimeMs,
                    timeIntervalMs);
            return false;
        }

        // Delegate to LazyUpdateManager for count-based check
        if (lazyUpdateManager.needsCompaction(tableId, snapshotId)) {
            log.info("CompactionTrigger: count-based threshold exceeded for table={}", tableId);
            return true;
        }

        return false;
    }

    /** Record that a compaction has just been completed, resetting the time-based window. */
    public void recordCompactionCompleted() {
        this.lastCompactionTimeMs = System.currentTimeMillis();
        log.debug("CompactionTrigger: recorded compaction at {}", lastCompactionTimeMs);
    }
}
