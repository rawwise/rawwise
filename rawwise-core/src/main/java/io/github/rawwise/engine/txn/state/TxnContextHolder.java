package io.github.rawwise.engine.txn.state;

import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.TxnTimeoutProfile;
import io.github.rawwise.engine.txn.clock.AtomicLongClock;
import io.github.rawwise.engine.txn.clock.LogicalClock;
import io.github.rawwise.engine.txn.isolate.ReadSnapshot;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import java.util.Objects;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * ThreadLocal-based holder for the current {@link TxnContext}.
 *
 * <p>Replaces the legacy {@code engine.txn.TransactionContextHolder}. Uses the unified strategy:
 * {@code txnId ≡ commitTs}, allocated via {@code clock.next()} at begin time.
 *
 * <h3>Thread Safety</h3>
 *
 * <p>Each thread maintains its own TxnContext via ThreadLocal. Nested transactions on the same
 * thread are not allowed and will throw {@link IllegalStateException}.
 */
@Slf4j
public final class TxnContextHolder {

    private static final ThreadLocal<TxnContext> CURRENT = new ThreadLocal<>();

    private static final int FALLBACK_SHARD_COUNT = 1024;

    private static volatile LogicalClock clock;

    private TxnContextHolder() {}

    public static TxnContext current() {
        return CURRENT.get();
    }

    public static void set(TxnContext ctx) {
        Objects.requireNonNull(ctx, "TxnContext must not be null; use clear() to remove");
        if (CURRENT.get() != null) {
            throw new IllegalStateException("TxnContext already active: " + CURRENT.get().txnId());
        }
        CURRENT.set(ctx);
    }

    /**
     * Begins a new transaction context and binds it to the current thread.
     *
     * <p>Uses the unified strategy: {@code txnId = clock.next()} (≡ commitTs ≡ startTs). The
     * context starts in BEGINNING state and is immediately transitioned to ACTIVE.
     *
     * @param tableIds the set of table IDs involved in this transaction
     * @param level the isolation level
     * @param config the RawWiseConfig (used to read ROUTING_SHARD_COUNT; may be null)
     * @param taskAttemptId the Spark task attempt ID
     * @return the newly created and bound TxnContext
     * @throws IllegalStateException if a TxnContext is already active on this thread
     */
    public static TxnContext begin(
            Set<String> tableIds,
            IsolationLevel level,
            RawWiseConfig config,
            String taskAttemptId) {
        Objects.requireNonNull(tableIds, "tableIds must not be null");
        Objects.requireNonNull(level, "isolationLevel must not be null");
        if (tableIds.isEmpty()) {
            throw new IllegalArgumentException("tableIds must not be empty");
        }
        if (CURRENT.get() != null) {
            throw new IllegalStateException("TxnContext already active: " + CURRENT.get().txnId());
        }
        if (clock == null) {
            throw new IllegalStateException(
                    "LogicalClock not set. Call TxnContextHolder.setClock() before begin(). "
                            + "The clock must be the same LogicalClock instance shared with "
                            + "ShardVersionTracker for correct OCC detection.");
        }
        long txnId = clock.next();
        String firstTableId =
                tableIds.stream()
                        .sorted()
                        .findFirst()
                        .orElseThrow(
                                () -> new IllegalArgumentException("tableIds must not be empty"));
        int shardCount =
                config != null
                        ? config.get(RawWiseOptions.ROUTING_SHARD_COUNT)
                        : FALLBACK_SHARD_COUNT;
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount must be positive, got: " + shardCount);
        }
        int shardId = (firstTableId.hashCode() & 0x7FFFFFFF) % shardCount;

        TxnContext ctx =
                new TxnContext(
                        txnId,
                        txnId,
                        level,
                        tableIds,
                        shardId,
                        0L,
                        0L,
                        taskAttemptId,
                        TxnTimeoutProfile.INGESTION);

        if (level != IsolationLevel.READ_COMMITTED) {
            ctx.setReadSnapshot(ReadSnapshot.now(0L, clock.now()));
        }

        ctx.transitionTo(TxnState.ACTIVE);
        CURRENT.set(ctx);
        return ctx;
    }

    public static boolean hasActive() {
        return CURRENT.get() != null;
    }

    public static void end() {
        TxnContext ctx = CURRENT.get();
        if (ctx != null && ctx.isActive()) {
            log.warn(
                    "Ending TxnContextHolder while TxnContext {} is still ACTIVE. "
                            + "This may indicate a missing commit/abort call.",
                    ctx.txnId());
        }
        CURRENT.remove();
    }

    public static void clear() {
        CURRENT.remove();
    }

    public static void setClock(LogicalClock c) {
        clock = Objects.requireNonNull(c, "LogicalClock must not be null");
    }

    public static LogicalClock getClock() {
        return clock;
    }

    public static synchronized void ensureClockSet() {
        if (clock == null) {
            clock = new AtomicLongClock(0);
        }
    }

    public static <T> T withTransaction(
            Set<String> tableIds,
            IsolationLevel level,
            RawWiseConfig config,
            String taskAttemptId,
            java.util.concurrent.Callable<T> action)
            throws Exception {
        begin(tableIds, level, config, taskAttemptId);
        try {
            return action.call();
        } finally {
            clear();
        }
    }
}
