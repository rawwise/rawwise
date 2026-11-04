package io.github.rawwise.engine.txn.isolate;

import io.github.rawwise.engine.txn.wal.TxnWALRecord;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * phantom readdetection, for SERIALIZABLE isolationlevel.
 *
 * <p>in ValidateStage in, checkcurrentlytransactionwrite key whether falls in
 * otherconcurrenttransaction alreadyreadrangequerywithin.ifdiscoverconflict,
 * currentlytransactionrequired abort.
 *
 * <h3>detectionlogic(currentlyimplementation)</h3>
 *
 * <ol>
 *   <li>tocurrentlytransactioneachwrite key, checkwhether falls in thistransactionrecord
 *       RangeReadSet in
 *   <li>onlycheckthistransactionselfconsistentity(self-check), notdocrosstransaction phantom
 *       detection
 * </ol>
 *
 * <h3>knownlimitation</h3>
 *
 * <p>currentlyimplementationnotcheck<em>otherconcurrenttransaction</em>writewhether affected
 * thistransactionrangeread. completecrosstransaction phantom detection(othertransactionin [K1, K5]
 * rangeinsert K3 after, currentlytransaction SELECT K1..K5 shoulddetectionto phantom) in {@code
 * ShardVersionTracker.detectPhantomConflicts()} inalreadyimplementation, but not yet integrated
 * into the v10 pipeline ValidateStage.
 *
 * <p>approach: ValidateStage via Function&lt;String, ShardVersionTracker&gt; get tracker, to
 * SERIALIZABLE transactioncall tracker.detectPhantomConflicts() replacecurrentlyself-check logic.
 * state.TxnContext alreadyincluding OperationSet, can directlybridge.
 */
@Slf4j
public final class PhantomDetector {

    private PhantomDetector() {
        /* utility class */
    }

    /** phantom readdetectionresult. */
    public record PhantomResult(boolean phantom, String reason) {
        /** nophantom read */
        public static final PhantomResult OK = new PhantomResult(false, "");

        public static PhantomResult conflict(String reason) {
            return new PhantomResult(true, reason);
        }
    }

    /**
     * checkwhetherexistphantom readconflict.
     *
     * <p>willcurrentlytransactionallwrite key andits RangeReadSet inrecordrangeenterrowcompare.
     * ifhasanywrite key fallinbeforenotreadthroughrange, reportphantom read.
     *
     * <p>note: completeimplementationalsorequirescheckotherconcurrenttransactionwritewhether
     * affected thistransactionrangeread.
     * thisfoundationimplementationonlycheckthistransactionselfconsistentity.
     *
     * @param rangeReadSet thistransactionrangereadset
     * @param writtenKeys thistransactionwrite key and its tableId mapping
     * @return detectionresult
     */
    public static PhantomResult detect(RangeReadSet rangeReadSet, List<WriteKeyEntry> writtenKeys) {
        if (rangeReadSet == null || rangeReadSet.isEmpty()) {
            return PhantomResult.OK;
        }
        if (writtenKeys == null || writtenKeys.isEmpty()) {
            return PhantomResult.OK;
        }

        for (WriteKeyEntry entry : writtenKeys) {
            // Check if the written key falls within a previously read range
            boolean inRange = rangeReadSet.isInAnyRange(entry.tableId(), entry.key());
            if (inRange) {
                // Key IS in a previously read range → phantom detected
                // A new key appearing in a range that was previously read is a phantom
                if (entry.writeType() == TxnWALRecord.WriteType.INSERT) {
                    return PhantomResult.conflict(
                            "Phantom detected: INSERT key in table '"
                                    + entry.tableId()
                                    + "' conflicts with existing range reads");
                }
            }
        }

        return PhantomResult.OK;
    }

    /** write key entry: forphantom readdetectionwriteinfo. */
    public record WriteKeyEntry(
            String tableId, byte[] key, TxnWALRecord.WriteType writeType, long baseVersion) {}
}
