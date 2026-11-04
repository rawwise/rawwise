package io.github.rawwise.engine.txn;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Per-use-case transaction timeout configuration.
 *
 * <p>Different workload types have fundamentally different transaction duration expectations. This
 * record provides predefined timeout profiles for common use cases and allows custom timeout
 * values.
 *
 * <h3>Predefined Profiles</h3>
 *
 * <table>
 *   <tr><th>Profile</th><th>Timeout</th><th>Typical Use Case</th></tr>
 *   <tr><td>{@link #OLTP}</td><td>60 seconds</td><td>Point reads/writes, low latency</td></tr>
 *   <tr><td>{@link #INGESTION}</td><td>5 minutes</td><td>Mini-batch ingestion, streaming writes</td></tr>
 *   <tr><td>{@link #ANALYTICS}</td><td>30 minutes</td><td>Long-running analytical queries</td></tr>
 * </table>
 *
 * <p>{@link #DEFAULT} is an alias for {@link #ANALYTICS}.
 */
public record TxnTimeoutProfile(String name, long timeoutMs) implements Serializable {

    /**
     * Creates a new timeout profile.
     *
     * @param name human-readable profile name (must not be blank)
     * @param timeoutMs timeout in milliseconds (must be positive)
     * @throws IllegalArgumentException if name is blank or timeoutMs is not positive
     */
    public TxnTimeoutProfile {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeoutMs must be positive, got: " + timeoutMs);
        }
    }

    /** OLTP profile: 60 seconds timeout for low-latency point operations. */
    public static final TxnTimeoutProfile OLTP =
            new TxnTimeoutProfile("OLTP", TimeUnit.SECONDS.toMillis(60));

    /** INGESTION profile: 5 minutes timeout for mini-batch ingestion workloads. */
    public static final TxnTimeoutProfile INGESTION =
            new TxnTimeoutProfile("INGESTION", TimeUnit.MINUTES.toMillis(5));

    /** ANALYTICS profile: 30 minutes timeout for long-running analytical transactions. */
    public static final TxnTimeoutProfile ANALYTICS =
            new TxnTimeoutProfile("ANALYTICS", TimeUnit.MINUTES.toMillis(30));

    /** Default profile (same as ANALYTICS). */
    public static final TxnTimeoutProfile DEFAULT = ANALYTICS;

    /**
     * Creates a custom timeout profile.
     *
     * @param timeoutMs timeout in milliseconds (must be positive)
     * @return a new profile with name "CUSTOM"
     * @throws IllegalArgumentException if timeoutMs is not positive
     */
    public static TxnTimeoutProfile custom(long timeoutMs) {
        return new TxnTimeoutProfile("CUSTOM", timeoutMs);
    }
}
