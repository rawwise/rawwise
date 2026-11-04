package io.github.rawwise.engine.storage.txn;

import io.github.rawwise.common.spi.TxnStoreBridge;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.OperationSet;
import io.github.rawwise.engine.txn.TxnReadCache;
import io.github.rawwise.engine.txn.WriteBuffer;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnContextHolder;
import java.util.List;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultTxnStoreBridge implements TxnStoreBridge {

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
        WriteBuffer.BufferResult bufResult = ctx.writeBuffer().get(tableId, key);
        if (bufResult.exists()) {
            if (bufResult.entry().type() == WriteBuffer.BufferEntryType.DELETED) {
                return new byte[0];
            }
            return bufResult.entry().getValue();
        }
        TxnReadCache readCache = ctx.readCache();
        if (readCache != null) {
            TxnReadCache.CachedRead cached = readCache.get(tableId, key);
            if (cached != null) {
                if (!cached.exists()) {
                    return new byte[0];
                }
                return cached.getValue();
            }
        }
        return null;
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
    public void flushPendingBatch(String tableId) {}

    @Override
    public String[] flushPendingBatch(
            String tableId,
            Function<String, Object> pendingInstanceSupplier,
            Function<String, Object> newPendingInstanceSupplier) {
        return doFlushPendingBatch(tableId, pendingInstanceSupplier, newPendingInstanceSupplier);
    }

    @Override
    public String[] commitTailBatch(
            String tableId,
            Function<String, Object> pendingInstanceSupplier,
            Function<String, Object> newPendingInstanceSupplier) {
        return doFlushPendingBatch(tableId, pendingInstanceSupplier, newPendingInstanceSupplier);
    }

    private String[] doFlushPendingBatch(
            String tableId,
            Function<String, Object> pendingInstanceSupplier,
            Function<String, Object> newPendingInstanceSupplier) {
        TxnContext ctx = TxnContextHolder.current();
        if (ctx == null || !ctx.isActive()) {
            log.debug("doFlushPendingBatch: no active transaction, tableId={}", tableId);
            return new String[0];
        }
        List<byte[]> keys = ctx.pendingBatchKeys();
        List<byte[]> values = ctx.pendingBatchValues();
        if (keys.isEmpty()) {
            log.debug(
                    "doFlushPendingBatch: txn={} tableId={} empty pending batch, no-op",
                    ctx.txnId(),
                    tableId);
            return new String[0];
        }
        if (pendingInstanceSupplier == null || newPendingInstanceSupplier == null) {
            log.warn("doFlushPendingBatch: missing instance suppliers for tableId={}", tableId);
            return new String[0];
        }

        LmdbInstance pendingInstance = (LmdbInstance) pendingInstanceSupplier.apply(tableId);
        if (pendingInstance == null) {
            log.warn(
                    "doFlushPendingBatch: pendingInstanceSupplier returned null for tableId={}",
                    tableId);
            return new String[0];
        }

        LmdbInstance.BatchPutResult result = pendingInstance.batchPut(keys, values);
        if (result.status() == LmdbInstance.PutStatus.MAP_FULL) {
            log.warn(
                    "doFlushPendingBatch: MAP_FULL on instance={}, creating new instance",
                    pendingInstance.instanceId());
            LmdbInstance newInstance = (LmdbInstance) newPendingInstanceSupplier.apply(tableId);
            if (newInstance == null) {
                log.error(
                        "doFlushPendingBatch: newPendingInstanceSupplier returned null for tableId={}",
                        tableId);
                return new String[0];
            }
            LmdbInstance.BatchPutResult retryResult = newInstance.batchPut(keys, values);
            if (!retryResult.isAllSuccess()) {
                log.error(
                        "doFlushPendingBatch: batchPut retry failed after MAP_FULL: {}",
                        retryResult.status());
                return new String[0];
            }
            ctx.addPendingInstanceId(newInstance.instanceId());
            ctx.clearPendingBatch();
            log.info(
                    "doFlushPendingBatch: txn={} tableId={} flushed {} entries to new instance {}",
                    ctx.txnId(),
                    tableId,
                    keys.size(),
                    newInstance.instanceId());
            return new String[] {newInstance.instanceId()};
        } else if (!result.isAllSuccess()) {
            log.error("doFlushPendingBatch: batchPut failed: {}", result.status());
            return new String[0];
        }

        ctx.addPendingInstanceId(pendingInstance.instanceId());
        ctx.clearPendingBatch();
        log.info(
                "doFlushPendingBatch: txn={} tableId={} flushed {} entries to instance {}",
                ctx.txnId(),
                tableId,
                keys.size(),
                pendingInstance.instanceId());
        return new String[] {pendingInstance.instanceId()};
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
