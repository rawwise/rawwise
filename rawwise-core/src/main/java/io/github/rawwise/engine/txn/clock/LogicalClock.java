package io.github.rawwise.engine.txn.clock;

/**
 * Unified logical clock interface, provides cross-node globally monotonically increasing
 * transaction identifiers.
 *
 * <p>satisfies:
 *
 * <ul>
 *   <li>INV-003: Global commit order is atomically allocated by Metastore CAS
 *   <li>INV-009: txnId is globally unique and monotonically increasing
 * </ul>
 *
 * <p>commitTs and txnId unified strategy: {@link #next()} the returned value serves simultaneously
 * as txnId and commitTs, ensuring global consistency of transaction identifiers and commit sequence
 * numbers.
 *
 * <p>Implementation choices:
 *
 * <ul>
 *   <li>Multi-node OLTP: {@link MetastoreClockImpl} — ensures cross-node monotonicity via Metastore
 *       CAS
 *   <li>Single-node/testing: can use a simplified AtomicLong-based implementation
 * </ul>
 */
public interface LogicalClock {

    /**
     * Atomically increment and return the new global logical tick. The return value serves as both
     * txnId and commitTs.
     *
     * @return new globally unique incremented logical tick
     */
    long next();

    /**
     * Read current tick (without incrementing), for snapshot reads. Returns the maximum committed
     * logical tick.
     *
     * @return current logical tick
     */
    long now();

    /**
     * Recover: advance to at least minValue, for crash recovery and cross-node clock
     * synchronization.
     *
     * @param minValue minimum tick value
     * @return adjusted current tick (>= minValue)
     */
    long advanceToAtLeast(long minValue);
}
