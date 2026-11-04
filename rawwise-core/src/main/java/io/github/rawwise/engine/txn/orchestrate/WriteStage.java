package io.github.rawwise.engine.txn.orchestrate;

import io.github.rawwise.engine.txn.TxnMode;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.wal.TxnWALRecord;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * WriteStage: write stage interface.
 *
 * <p>responsible for:
 *
 * <ol>
 *   <li>WAL TXN_WRITE_INTENT write (complete write set)
 *   <li>Staging store write (WRITE_THROUGH mode writes directly to LMDB PENDING; DEFERRED mode
 *       writes uniformly in DurableStage)
 * </ol>
 *
 * <p>Supports both WRITE_THROUGH and DEFERRED modes. In WRITE_THROUGH mode, actual PENDING write is
 * performed here; In DEFERRED mode, only WAL is written; data persistence is deferred to
 * DurableStage.
 */
public interface WriteStage {

    /** Write result: includes WAL records and staging references. */
    record WriteResult(
            List<TxnWALRecord.TxnWriteIntent> intents, long totalBytesWritten, long crc32cSum) {

        public static WriteResult of(List<TxnWALRecord.TxnWriteIntent> intents) {
            long totalBytes = 0;
            long crc = 0;
            for (TxnWALRecord.TxnWriteIntent i : intents) {
                totalBytes += i.keyBytes().length;
                if (i.valueRef() != null) totalBytes += i.valueRef().length;
                crc ^= i.crc32c();
            }
            return new WriteResult(intents, totalBytes, crc);
        }
    }

    /**
     * Execute write stage.
     *
     * <p>Extract all writes from TxnContext.writeBuffer, generate WAL TXN_WRITE_INTENT records. In
     * WRITE_THROUGH mode, also writes to staging LMDB.
     *
     * @param ctx transaction context
     * @param txnMode writemode
     * @param baseVersions baseVersion for each key(tableId|key→commitTs), for OCC conflict
     *     detection
     * @return write result (WAL record list)
     */
    WriteResult execute(
            TxnContext ctx, TxnMode txnMode, Map<String, Map<ByteBuffer, Long>> baseVersions);
}
