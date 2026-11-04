package io.github.rawwise.engine.txn.orchestrate;

import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.TxnMode;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.wal.TxnWALRecord;
import io.github.rawwise.engine.txn.wal.TxnWALWriter;
import java.util.List;
import java.util.function.Function;

/**
 * DurableStage: datapersistencestageinterface.
 *
 * <p>responsibility:
 *
 * <ol>
 *   <li>WAL TXN_PREPARE_AND_DURABLE write(singletime fsync)
 *   <li>LMDB batchPut(willdatawrite LMDB)
 *   <li>CommitMarker write(and batchPut insameone LMDB write transaction in, INV-006)
 *   <li>sealedDigest CRC32 compute(forcrash recoveryvalidate)
 * </ol>
 *
 * <h3>ThreadLocal protocol (STO-TXN-002)</h3>
 *
 * <pre>
 * TxnContextHolder.clear() // prevent DurableStage writetriggertransactionreentrant
 * try { durableStage.execute(ctx, ...) }
 * finally { TxnContextHolder.set(ctx) } // recoverycontext
 * </pre>
 */
public interface DurableStage {

    /** persistenceresult. */
    record DurableResult(
            byte[] sealedDigest,
            int persistedKeyCount,
            long persistedByteCount,
            boolean commitMarkerWritten) {

        public static DurableResult of(
                byte[] digest, int keyCount, long byteCount, boolean marker) {
            return new DurableResult(digest, keyCount, byteCount, marker);
        }
    }

    /**
     * executedatapersistence.
     *
     * @param ctx transaction context
     * @param writeIntents writeintentlist
     * @param lmdbInstance LMDB instance
     * @param txnMode writemode
     * @param walWriter WAL writer(forwrite PREPARE_AND_DURABLE)
     * @return persistenceresult(including sealedDigest)
     */
    DurableResult execute(
            TxnContext ctx,
            List<TxnWALRecord.TxnWriteIntent> writeIntents,
            LmdbInstance lmdbInstance,
            TxnMode txnMode,
            TxnWALWriter walWriter);

    /**
     * multipletablepersistence.
     *
     * <p>by {@link TxnWALRecord.TxnWriteIntent#primaryTableId()} writeIntents groupto eachselftable
     * LMDB instance, andineach LMDB aboveindependentcall batchPutWithMarker — eachtableall get self
     * commit marker.
     *
     * <h3>atomicityargument</h3>
     *
     * <p>multipletable commit atomicity <b>not</b> dependent"all LMDB marker
     * whileexist".atomicityby {@code PublishStage} writeto Metastore {@code TxnCommitRecord}(CAS
     * NotExists) uniquedecide — thisiscross-nodeglobalvisible"decisionpoint".LMDB marker
     * onlyasasthistabledatapersistencelocal hint.
     *
     * <h3>recoverysemantics</h3>
     *
     * <ul>
     *   <li>if Metastore TxnCommitRecord exist → transactionalready committed →
     *       recoverywheninmissing marker table replay(idempotent)
     *   <li>if Metastore TxnCommitRecord does not exist → transactionnot committed →
     *       recoverywheninall alreadywrite marker tableabove tombstone(rollback)
     * </ul>
     *
     * @param ctx transaction context
     * @param writeIntents alltable WriteIntent(group by primaryTableId)
     * @param tableLmdbResolver tableId → LmdbInstance parsefunctioncount(notmay be null)
     * @param txnMode writemode
     * @param walWriter WAL writer
     * @return persistenceresult(aggregated digest / count / bytes / commitMarker
     *     alltablewritecomplete)
     */
    default DurableResult executeMultiTable(
            TxnContext ctx,
            List<TxnWALRecord.TxnWriteIntent> writeIntents,
            Function<String, LmdbInstance> tableLmdbResolver,
            TxnMode txnMode,
            TxnWALWriter walWriter) {
        throw new UnsupportedOperationException(
                "executeMultiTable not implemented in this DurableStage; "
                        + "use DefaultDurableStage or override.");
    }
}
