package io.github.rawwise.backend.lazyupdate;

import io.github.rawwise.backend.spark.LazyUpdateRewriteRule;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestrator;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnContextHolder;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.expression.ExpressionUtils;
import java.io.IOException;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

/**
 * Driver-side component that intercepts range updates and stores them as UpdateLog entries.
 *
 * <h3>Design</h3>
 *
 * <ul>
 *   <li>PK equality updates route directly to LMDB (handled by Update.doUpdate())
 *   <li>Range updates are intercepted here and stored as lazy UpdateLog entries
 *   <li>The stored UpdateLog entries are applied at query time when reading data
 * </ul>
 *
 * <p>This component runs on the Spark Driver side, intercepting Spark UPDATE plans via {@link
 * LazyUpdateRewriteRule}.
 */
@Slf4j
public record LazyUpdateWriteInterceptor(LazyUpdateManager lazyUpdateManager) {

    public LazyUpdateWriteInterceptor(LazyUpdateManager lazyUpdateManager) {
        this.lazyUpdateManager =
                Objects.requireNonNull(lazyUpdateManager, "lazyUpdateManager must not be null");
    }

    /**
     * Intercept a range update: serialize the predicate, store it as an UpdateLog, and return the
     * entry IDs.
     *
     * @param tableId the table identifier
     * @param predicate the range predicate (WHERE clause)
     * @param updates the SET assignments (column -> value)
     * @param txnId the transaction ID (0 if not in transaction)
     * @return list of entry IDs for the stored UpdateLog
     * @throws IOException if writing the UpdateLog fails
     */
    public List<String> interceptRangeUpdate(
            String tableId, Expression predicate, Map<String, Comparable<?>> updates, long txnId)
            throws IOException {
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(predicate, "predicate must not be null");
        Objects.requireNonNull(updates, "updates must not be null");

        if (updates.isEmpty()) {
            log.warn(
                    "interceptRangeUpdate called with empty updates for table={}, skipping",
                    tableId);
            return List.of();
        }

        boolean autoCommit = !hasActiveTransaction();
        LazyUpdateManager.WriteResult result;
        if (autoCommit) {
            result = lazyUpdateManager.writeAndCommitUpdateLog(tableId, predicate, updates, txnId);
        } else {
            result = lazyUpdateManager.writeUpdateLog(tableId, predicate, updates, txnId);
        }
        String entryId = result.entryId();
        List<String> entryIds = List.of(entryId);

        tryBridgeRecordUpdateLogToOrchestrator(tableId, result.updateLog());

        log.info(
                "Intercepted range update for table={}, entryId={}, predicate={}, updates={}",
                tableId,
                entryId,
                predicate,
                updates);
        return entryIds;
    }

    public List<String> interceptRangeDelete(String tableId, Expression predicate, long txnId)
            throws IOException {
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(predicate, "predicate must not be null");

        boolean autoCommit = !hasActiveTransaction();
        LazyUpdateManager.WriteResult result;
        if (autoCommit) {
            result = lazyUpdateManager.writeAndCommitUpdateLog(tableId, predicate, Map.of(), txnId);
        } else {
            result = lazyUpdateManager.writeUpdateLog(tableId, predicate, Map.of(), txnId);
        }
        String entryId = result.entryId();
        List<String> entryIds = List.of(entryId);

        tryBridgeRecordUpdateLogToOrchestrator(tableId, result.updateLog());

        log.info(
                "Intercepted range delete for table={}, entryId={}, predicate={}",
                tableId,
                entryId,
                predicate);
        return entryIds;
    }

    private void tryBridgeRecordUpdateLogToOrchestrator(String tableId, UpdateLog updateLog) {
        if (!TxnContextHolder.hasActive()) return;
        TxnContext ctx = TxnContextHolder.current();
        TxnOrchestrator orchestrator = LazyUpdateBridge.orchestrator();
        if (orchestrator == null) return;
        try {
            orchestrator.recordUpdateLog(ctx, tableId, updateLog);
            log.debug(
                    "UpdateLog bridged to orchestrator: txnId={} tableId={} filePath={}",
                    ctx.txnId(),
                    tableId,
                    updateLog.filePath());
        } catch (Exception e) {
            log.warn(
                    "Failed to bridge UpdateLog to orchestrator for txnId={}: {}",
                    ctx.txnId(),
                    e.getMessage());
        }
    }

    /**
     * Check if the given expression is a primary key equality update. A primary key equality means
     * every PK column appears in an equality condition (column = value).
     *
     * @param predicate the WHERE condition expression
     * @param primaryKeys the set of primary key column names
     * @return true if the predicate fully specifies a PK equality lookup
     */
    public boolean isPrimaryKeyUpdate(Expression predicate, Set<String> primaryKeys) {
        if (predicate == null || primaryKeys == null || primaryKeys.isEmpty()) {
            return false;
        }
        return ExpressionUtils.isPrimaryKeyEquals(predicate, primaryKeys);
    }

    /**
     * Extract primary key values from an equality expression for direct LMDB routing. For example,
     * {@code WHERE user_id = 2001 AND region = 'us'} with PK columns [user_id, region] returns
     * {@code {user_id: 2001, region: "us"}}.
     *
     * @param predicate the WHERE condition expression (must be PK equality)
     * @param primaryKeys the set of primary key column names
     * @return map of PK column name to value
     */
    public Map<String, Comparable<?>> extractPrimaryKeyValues(
            Expression predicate, Set<String> primaryKeys) {
        if (predicate == null || primaryKeys == null || primaryKeys.isEmpty()) {
            return Map.of();
        }
        return ExpressionUtils.extractPrimaryKeyValues(predicate, primaryKeys);
    }

    private static boolean hasActiveTransaction() {
        return TxnContextHolder.hasActive();
    }
}
