package io.github.rawwise.engine.txn.bridge;

import io.github.rawwise.common.spi.TxnStoreBridge;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.OperationSet;
import io.github.rawwise.engine.txn.TxnReadCache;
import io.github.rawwise.engine.txn.WriteBuffer;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestrator;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnContextHolder;
import java.util.List;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of {@link TxnStoreBridge} that bridges {@link TxnContextHolder} (ThreadLocal-based
 * TxnContext resolution) and {@link TxnOrchestrator} (unified transaction orchestrator).
 *
 * <p>This class lives in txn/bridge/ and has access to both old-style {@code txn.TxnContext} (via
 * TxnContextHolder) and the orchestrator. It translates raw byte[]/String SPIs directly on the
 * legacy TxnContext's WriteBuffer and OperationSet, recording both the buffered value and the OCC
 * baseVersion for conflict detection.
 */
@Slf4j
public class TxnStoreBridgeImpl implements TxnStoreBridge {

    private final TxnOrchestrator orchestrator;

    public TxnStoreBridgeImpl(TxnOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @Override
    public boolean tryRecordWrite(byte[] key, byte[] value, long baseVersion, String tableId) {
        if (key == null) return false;
        TxnContext ctx = TxnContextHolder.current();
        if (ctx == null || !ctx.isActive()) return false;

        ctx.writeBuffer().put(tableId, key, value);

        ctx.operationSet()
                .recordWrite(
                        key,
                        baseVersion == 0
                                ? OperationSet.WriteType.INSERT
                                : OperationSet.WriteType.UPDATE,
                        baseVersion);

        if (ctx.txnMode().isWriteThrough()) {
            ctx.addPendingWrite(key, value);
        }

        log.debug(
                "tryRecordWrite: txn={} table={} keySize={} baseVersion={}",
                ctx.txnId(),
                tableId,
                key.length,
                baseVersion);
        return true;
    }

    @Override
    public byte[] tryResolveRead(String tableId, byte[] key) {
        if (key == null) return null;
        TxnContext ctx = TxnContextHolder.current();
        if (ctx == null || !ctx.isActive()) return null;

        // Tier 1: WriteBuffer (uncommitted writes within this txn)
        WriteBuffer.BufferResult bufResult = ctx.writeBuffer().get(tableId, key);
        if (bufResult.exists()) {
            if (bufResult.entry().type() == WriteBuffer.BufferEntryType.DELETED) {
                return new byte[0]; // DELETED sentinel: key is deleted in this txn
            }
            return bufResult.entry().getValue();
        }

        // Tier 2: TxnReadCache (REPEATABLE_READ/SERIALIZABLE cached reads)
        TxnReadCache readCache = ctx.readCache();
        if (readCache != null) {
            TxnReadCache.CachedRead cached = readCache.get(tableId, key);
            if (cached != null) {
                if (!cached.exists()) {
                    return new byte[0]; // not-found sentinel: cached as not-found
                }
                return cached.getValue();
            }
        }

        return null; // not resolved in txn context — fall through to LMDB
    }

    @Override
    public boolean tryRecordDelete(byte[] key, String tableId, long baseVersion) {
        if (key == null) return false;
        TxnContext ctx = TxnContextHolder.current();
        if (ctx == null || !ctx.isActive()) return false;

        ctx.writeBuffer().delete(tableId, key);

        ctx.operationSet().recordWrite(key, OperationSet.WriteType.DELETE, baseVersion);

        if (ctx.txnMode().isWriteThrough()) {
            ctx.addPendingWrite(key, new byte[0]);
        }

        log.debug(
                "tryRecordDelete: txn={} table={} keySize={} baseVersion={}",
                ctx.txnId(),
                tableId,
                key.length,
                baseVersion);
        return true;
    }

    @Override
    public void flushPendingBatch(String tableId) {
        TxnContext ctx = TxnContextHolder.current();
        if (ctx == null || !ctx.isActive()) return;

        orchestrator.flushPending(ctx, tableId);
    }

    @Override
    public String[] flushPendingBatch(
            String tableId,
            Function<String, Object> pendingInstanceSupplier,
            Function<String, Object> newPendingInstanceSupplier) {
        TxnContext ctx = TxnContextHolder.current();
        if (ctx == null || !ctx.isActive()) return new String[0];
        if (pendingInstanceSupplier == null || newPendingInstanceSupplier == null)
            return new String[0];

        // Cast Object → LmdbInstance (SPI layer uses Object to avoid import)
        @SuppressWarnings("unchecked")
        Function<String, LmdbInstance> pendingSupplier =
                tid -> (LmdbInstance) pendingInstanceSupplier.apply(tid);
        @SuppressWarnings("unchecked")
        Function<String, LmdbInstance> newPendingSupplier =
                tid -> (LmdbInstance) newPendingInstanceSupplier.apply(tid);

        List<String> instanceIds =
                orchestrator.flushPendingWithInstance(
                        ctx, tableId, pendingSupplier, newPendingSupplier);
        return instanceIds.toArray(new String[0]);
    }

    @Override
    public String[] commitTailBatch(
            String tableId,
            Function<String, Object> pendingInstanceSupplier,
            Function<String, Object> newPendingInstanceSupplier) {
        if (pendingInstanceSupplier == null || newPendingInstanceSupplier == null)
            return new String[0];

        // Cast Object → LmdbInstance (SPI layer uses Object to avoid import)
        @SuppressWarnings("unchecked")
        Function<String, LmdbInstance> pendingSupplier =
                tid -> (LmdbInstance) pendingInstanceSupplier.apply(tid);
        @SuppressWarnings("unchecked")
        Function<String, LmdbInstance> newPendingSupplier =
                tid -> (LmdbInstance) newPendingInstanceSupplier.apply(tid);

        List<String> instanceIds =
                orchestrator.commitTailBatch(tableId, pendingSupplier, newPendingSupplier);
        return instanceIds.toArray(new String[0]);
    }

    @Override
    public boolean isPendingBatchFull(int maxSize, long maxBytes) {
        TxnContext ctx = TxnContextHolder.current();
        if (ctx == null || !ctx.isActive()) return false;
        return ctx.isPendingBatchFull(maxSize, maxBytes);
    }

    @Override
    public boolean isTransactionActive() {
        TxnContext ctx = TxnContextHolder.current();
        return ctx != null && ctx.isActive();
    }
}
