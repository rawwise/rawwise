package io.github.rawwise.engine.txn;

/**
 * Transaction isolation level enum, defining concurrency control semantics at different levels.
 *
 * <p>Isolation levels from lowest to highest:
 *
 * <ul>
 *   <li>READ_COMMITTED - only prevents write-write conflicts, allows non-repeatable reads
 *   <li>REPEATABLE_READ - prevents read-write and write-write conflicts, guarantees repeatable
 *       reads
 *   <li>SERIALIZABLE - prevents all conflicts, including phantom detection
 * </ul>
 */
public enum IsolationLevel {
    /**
     * Read committed: only checks write-write conflicts (Write-Write). conflict only when two
     * transactions write the same key, allows non-repeatable read phenomenon.
     */
    READ_COMMITTED,

    /**
     * Repeatable read: checks read-write + write-write conflicts (Read-Write + Write-Write). keys
     * read by this transaction cannot be modified by other transactions after startTs, guaranteeing
     * repeatable reads.
     */
    REPEATABLE_READ,

    /**
     * Serializable: checks all conflicts + phantom detection (Phantom Detection). On top of
     * REPEATABLE_READ, additionally detects range query result set changes.
     */
    SERIALIZABLE;

    /**
     * Parse isolation level from string, case-insensitive.
     *
     * @param value isolation level string
     * @return corresponding isolation level
     * @throws IllegalArgumentException if value is invalid
     */
    public static IsolationLevel fromString(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Isolation level must not be blank");
        }
        return valueOf(value.toUpperCase().trim());
    }
}
