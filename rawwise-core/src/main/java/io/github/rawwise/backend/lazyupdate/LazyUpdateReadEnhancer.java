package io.github.rawwise.backend.lazyupdate;

import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.expression.ExpressionUtils;
import io.github.rawwise.model.expression.ExpressionUtils.ResolvedUpdates;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper for deciding whether a data file scan can be skipped because all matching rows are fully
 * covered by UpdateLog predicates.
 *
 * <p>The production read path for applying UpdateLogs to scanned rows is {@code
 * RawWisePartitionReader.applyUpdateLogsToRows}; this class only handles the row-generation fast
 * path (all-Eq predicates fully covered).
 */
@Slf4j
public record LazyUpdateReadEnhancer(LazyUpdateManager lazyUpdateManager) {

    private static final io.micrometer.core.instrument.Counter SKIP_SCAN_HIT =
            io.micrometer.core.instrument.Metrics.counter("rawwise.skip_scan", "result", "hit");
    private static final io.micrometer.core.instrument.Counter SKIP_SCAN_MISS =
            io.micrometer.core.instrument.Metrics.counter("rawwise.skip_scan", "result", "miss");
    private static final io.micrometer.core.instrument.Counter SKIP_SCAN_MISS_NOT_EQ =
            io.micrometer.core.instrument.Metrics.counter(
                    "rawwise.skip_scan", "result", "miss_not_eq");
    private static final io.micrometer.core.instrument.Counter ROW_GENERATION_COUNT =
            io.micrometer.core.instrument.Metrics.counter(
                    "rawwise.row_generation", "phase", "generated");
    private static final io.micrometer.core.instrument.Counter ROW_GENERATION_FALLBACK =
            io.micrometer.core.instrument.Metrics.counter(
                    "rawwise.row_generation", "phase", "fallback_to_scan");

    public static void recordRowGenerationFallback() {
        ROW_GENERATION_FALLBACK.increment();
    }

    /**
     * Creates a read enhancer backed by the given {@link LazyUpdateManager}.
     *
     * @param lazyUpdateManager the manager used to load UpdateLog entries (must not be null)
     */
    public LazyUpdateReadEnhancer(LazyUpdateManager lazyUpdateManager) {
        this.lazyUpdateManager =
                Objects.requireNonNull(lazyUpdateManager, "lazyUpdateManager must not be null");
    }

    /**
     * Determine whether a data file scan can be completely skipped because all rows matching {@code
     * readCondition} are covered by UpdateLog predicates.
     *
     * @param readCondition the query's read predicate (may be {@code null})
     * @param updateLogs UpdateLog entries (any order; sorted internally)
     * @return {@code true} if no data file scan is needed
     */
    public boolean canSkipScan(Expression readCondition, List<UpdateLogEntry> updateLogs) {
        if (updateLogs == null || updateLogs.isEmpty()) {
            return false;
        }
        return ExpressionUtils.canFullyCover(readCondition, updateLogs);
    }

    public boolean canSkipScanWithRowGeneration(
            Expression readCondition, List<UpdateLogEntry> updateLogs) {
        if (!canSkipScan(readCondition, updateLogs)) {
            SKIP_SCAN_MISS.increment();
            return false;
        }
        boolean allEq =
                updateLogs.stream()
                        .allMatch(
                                entry ->
                                        entry.predicate() != null
                                                && ExpressionUtils.isAllEqPredicate(
                                                        entry.predicate()));
        if (!allEq) {
            SKIP_SCAN_MISS_NOT_EQ.increment();
            log.info(
                    "canSkipScanWithRowGeneration: canSkipScan=true but predicates not all Eq, falling back to scan");
            return false;
        }
        SKIP_SCAN_HIT.increment();
        return true;
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Comparable<?>>> generateRowsFromUpdateLogs(
            Expression readCondition,
            List<UpdateLogEntry> updateLogs,
            Map<String, Comparable<?>> schemaDefaults) {
        List<Map<String, Comparable<?>>> result = new ArrayList<>();
        ResolvedUpdates resolved = ExpressionUtils.resolveUpdates(readCondition, updateLogs);
        for (ExpressionUtils.ResolvedSlice slice : resolved.slices()) {
            Map<String, Comparable<?>> row = new LinkedHashMap<>(schemaDefaults);
            UpdateLogDataRow.applyPredicateToRow(slice.applicableCondition(), row);
            row.putAll(slice.updates());
            result.add(row);
        }
        log.debug(
                "generateRowsFromUpdateLogs: generated {} rows from {} slices",
                result.size(),
                resolved.slices().size());
        ROW_GENERATION_COUNT.increment(result.size());
        return result;
    }
}
