package io.github.rawwise.governance.config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class TxnEngineConfig implements java.io.Serializable {
    public enum Engine {
        LEGACY,
        PLAN_A
    }

    /**
     * WAL persistence mode.
     *
     * <ul>
     *   <li>{@link #NONE}: No WAL written. Node-level persistence (LMDB + commit marker), crash
     *       recovery relies entirely on marker. Highest performance, but lowest failure
     *       observability.
     *   <li>{@link #LMDB_LOCAL}: Local LMDB WAL. Can be replayed on this node after crash, cannot
     *       recover across nodes. Default option.
     *   <li>{@link #HDFS}: HDFS shared WAL (recommended for production). After crash, can be taken
     *       over by other nodes for replay, providing highest availability.
     * </ul>
     *
     * <p>Regardless of mode, commit marker is the minimum dependency for ACID. WAL provides failure
     * observability, cross-node replay, and intermediate state diagnostics.
     */
    public enum WalMode {
        NONE,
        LMDB_LOCAL,
        HDFS
    }

    private static final long DEFAULT_TXN_TIMEOUT_MS = 300_000L;
    private static final WalMode DEFAULT_WAL_MODE = WalMode.LMDB_LOCAL;
    private static final int DEFAULT_CLOCK_BATCH_SIZE = 256;

    private final Engine engine;
    private final long txnTimeoutMs;
    private final WalMode walMode;
    private final int clockBatchSize;

    public TxnEngineConfig() {
        this(
                fromSystemProperty(),
                fromTimeoutSystemProperty(),
                fromWalModeSystemProperty(),
                fromClockBatchSizeSystemProperty());
    }

    public TxnEngineConfig(Engine engine) {
        this(engine, DEFAULT_TXN_TIMEOUT_MS, DEFAULT_WAL_MODE, DEFAULT_CLOCK_BATCH_SIZE);
    }

    public TxnEngineConfig(Engine engine, long txnTimeoutMs) {
        this(engine, txnTimeoutMs, DEFAULT_WAL_MODE, DEFAULT_CLOCK_BATCH_SIZE);
    }

    public TxnEngineConfig(Engine engine, long txnTimeoutMs, WalMode walMode) {
        this(engine, txnTimeoutMs, walMode, DEFAULT_CLOCK_BATCH_SIZE);
    }

    public TxnEngineConfig(Engine engine, long txnTimeoutMs, WalMode walMode, int clockBatchSize) {
        this.engine = engine;
        this.txnTimeoutMs = txnTimeoutMs;
        this.walMode = walMode;
        this.clockBatchSize = clockBatchSize;
    }

    public static TxnEngineConfig planA() {
        return new TxnEngineConfig(Engine.PLAN_A);
    }

    public static TxnEngineConfig planAWithWalMode(WalMode walMode) {
        return new TxnEngineConfig(Engine.PLAN_A, DEFAULT_TXN_TIMEOUT_MS, walMode);
    }

    public static TxnEngineConfig legacy() {
        return new TxnEngineConfig(Engine.LEGACY);
    }

    private static Engine fromSystemProperty() {
        String val = System.getProperty("rawwise.txn.engine", "legacy");
        if ("legacy".equalsIgnoreCase(val)) {
            return Engine.LEGACY;
        }
        if ("plan-a".equalsIgnoreCase(val)) {
            return Engine.PLAN_A;
        }
        log.warn(
                "Unknown rawwise.txn.engine value '{}', falling back to LEGACY. Valid values: legacy, plan-a",
                val);
        return Engine.LEGACY;
    }

    private static long fromTimeoutSystemProperty() {
        String val = System.getProperty("rawwise.txn.timeout.ms");
        if (val != null && !val.isBlank()) {
            try {
                return Long.parseLong(val);
            } catch (NumberFormatException e) {
                log.warn(
                        "Invalid rawwise.txn.timeout.ms value '{}', using default {}ms",
                        val,
                        DEFAULT_TXN_TIMEOUT_MS);
            }
        }
        return DEFAULT_TXN_TIMEOUT_MS;
    }

    private static WalMode fromWalModeSystemProperty() {
        String val = System.getProperty("rawwise.wal.mode");
        if (val == null || val.isBlank()) {
            return DEFAULT_WAL_MODE;
        }
        String norm = val.trim().toUpperCase().replace('-', '_');
        try {
            return WalMode.valueOf(norm);
        } catch (IllegalArgumentException e) {
            log.warn(
                    "Unknown rawwise.wal.mode value '{}', falling back to {}. Valid values: NONE, LMDB_LOCAL, HDFS",
                    val,
                    DEFAULT_WAL_MODE);
            return DEFAULT_WAL_MODE;
        }
    }

    private static int fromClockBatchSizeSystemProperty() {
        String val = System.getProperty("rawwise.clock.batch-size");
        if (val == null || val.isBlank()) {
            return DEFAULT_CLOCK_BATCH_SIZE;
        }
        try {
            int n = Integer.parseInt(val.trim());
            if (n >= 64 && n <= 16384) {
                return n;
            }
            log.warn(
                    "rawwise.clock.batch-size={} out of range [64, 16384], using default {}",
                    n,
                    DEFAULT_CLOCK_BATCH_SIZE);
        } catch (NumberFormatException e) {
            log.warn(
                    "Invalid rawwise.clock.batch-size value '{}', using default {}",
                    val,
                    DEFAULT_CLOCK_BATCH_SIZE);
        }
        return DEFAULT_CLOCK_BATCH_SIZE;
    }

    public void logStartupInfo() {
        log.info(
                "TxnEngine active: engine={}, txnTimeoutMs={}, walMode={}, clockBatchSize={}",
                engine,
                txnTimeoutMs,
                walMode,
                clockBatchSize);
        if (engine == Engine.PLAN_A) {
            log.warn(
                    "Plan-A engine is active. Plan-A and Legacy engines MUST use physically isolated tables. "
                            + "Sharing the same table across engines will cause data corruption.");
        }
        if (engine == Engine.LEGACY) {
            log.info("Legacy engine is active (single-node mode, AtomicLongClock).");
        }
        if (walMode == WalMode.NONE) {
            log.warn(
                    "WAL is DISABLED (mode=NONE). Durability relies solely on LMDB commit marker. "
                            + "Crash recovery cannot replay WAL — choose this mode only when LMDB is the sole source of truth.");
        }
    }
}
