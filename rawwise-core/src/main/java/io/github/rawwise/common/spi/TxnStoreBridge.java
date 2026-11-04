package io.github.rawwise.common.spi;

import java.util.function.Function;

/**
 * SPI bridge between storage layer and transaction layer. storage/ layer calls this interface
 * instead of directly importing txn/ types. Implementation in txn/bridge/TxnStoreBridgeImpl uses
 * ThreadLocal to resolve TxnContext.
 *
 * <p>This interface lives in common/spi/ (Layer 0) with zero dependencies on txn/, storage/, or
 * config/ packages, breaking the circular dependency.
 */
public interface TxnStoreBridge {

    /**
     * Record a write operation within the current transaction context.
     *
     * @param key encoded primary key
     * @param value encoded value
     * @param baseVersion base version (commitTs) of the key before this write
     * @param tableId table identifier
     * @return true if the write was recorded (transaction is active), false otherwise
     */
    boolean tryRecordWrite(byte[] key, byte[] value, long baseVersion, String tableId);

    /**
     * Try to resolve a read from the current transaction context (WriteBuffer first, then
     * ReadCache).
     *
     * @param tableId table identifier
     * @param key encoded primary key
     * @return the resolved value if found in transaction context, a zero-length byte array if the
     *     key is deleted within this transaction, null if not resolved (fall through to LMDB)
     */
    byte[] tryResolveRead(String tableId, byte[] key);

    /**
     * Record a delete operation within the current transaction context.
     *
     * @param key encoded primary key
     * @param tableId table identifier
     * @param baseVersion base version (commitTs) of the key before this delete
     * @return true if the delete was recorded (transaction is active), false otherwise
     */
    boolean tryRecordDelete(byte[] key, String tableId, long baseVersion);

    /**
     * Flush the pending batch for WRITE_THROUGH mode.
     *
     * @param tableId table identifier
     */
    void flushPendingBatch(String tableId);

    /**
     * Flush the pending batch for WRITE_THROUGH mode with LMDB instance suppliers. Performs actual
     * batchPut to the pending LMDB instance via the bridge.
     *
     * <p>The Function parameters accept Object to avoid importing LmdbInstance into the SPI layer.
     * The bridge implementation casts them to the correct type.
     *
     * @param tableId table identifier
     * @param pendingInstanceSupplier supplies the current pending LMDB instance (cast to
     *     LmdbInstance)
     * @param newPendingInstanceSupplier supplies a new pending LMDB instance on MAP_FULL (cast to
     *     LmdbInstance)
     * @return array of instance IDs that were written to (for subsequent commitPendingInstances
     *     call)
     */
    String[] flushPendingBatch(
            String tableId,
            Function<String, Object> pendingInstanceSupplier,
            Function<String, Object> newPendingInstanceSupplier);

    /**
     * Flush residual pending batch at commit time (tail batch). Called when the pending batch is
     * not full but a commit is imminent.
     *
     * <p>The Function parameters accept Object to avoid importing LmdbInstance into the SPI layer.
     * The bridge implementation casts them to the correct type.
     *
     * @param tableId table identifier
     * @param pendingInstanceSupplier supplies the current pending LMDB instance (cast to
     *     LmdbInstance)
     * @param newPendingInstanceSupplier supplies a new pending LMDB instance on MAP_FULL (cast to
     *     LmdbInstance)
     * @return array of instance IDs that were written to (for subsequent commitPendingInstances
     *     call)
     */
    String[] commitTailBatch(
            String tableId,
            Function<String, Object> pendingInstanceSupplier,
            Function<String, Object> newPendingInstanceSupplier);

    /**
     * Check if the pending batch in the current transaction is full.
     *
     * @param maxSize maximum number of entries
     * @param maxBytes maximum total bytes
     * @return true if the pending batch exceeds either limit
     */
    boolean isPendingBatchFull(int maxSize, long maxBytes);

    /**
     * Check if a transaction is active on the current thread.
     *
     * @return true if a transaction context is bound and active
     */
    boolean isTransactionActive();
}
