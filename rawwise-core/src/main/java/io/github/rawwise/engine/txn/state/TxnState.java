package io.github.rawwise.engine.txn.state;

/**
 * unifiedtransactionstate machine 11 stateenum.
 *
 * <p>statetransitionpath(commit):
 *
 * <pre>
 * BEGINNING → ACTIVE → VALIDATING → PREPARING → DATA_DURABLE
 * → MANIFEST_COMMITTED → PUBLISHED → COMMITTED → ACKED
 * </pre>
 *
 * <p>statetransitionpath(rollback):
 *
 * <pre>
 * (any rollbackable state) → ABORTING → ABORTED
 * </pre>
 *
 * <p>rollbackboundary:
 *
 * <ul>
 *   <li>DATA_DURABLE before: canbyrollback(abort)
 *   <li>DATA_DURABLE: conditioncanrollback — has CommitMarker then redo, nothen abort
 *   <li>MANIFEST_COMMITTED andafter: notrollback, can only redo complete
 * </ul>
 *
 * <p>dead-end conclusion (INV-002): DATA_DURABLE before, any reader invisible. MANIFEST_COMMITTED
 * isglobalatomicswitch.
 */
public enum TxnState {

    /** transactionalreadystart, WAL BEGIN already fsync, not yethaswrite */
    BEGINNING(false, true, "WAL BEGIN 已 fsync，无 write → 直接 abort"),

    /** transactionactivein, canreadwrite, writebufferin WriteSet/ReadSet */
    ACTIVE(false, true, "清理 staging，写 TXN_ABORT"),

    /** OCC validatein: check baseVersion vs currentCommitTs + phantom detection */
    VALIDATING(false, true, "OCC 已通过但未 durable → abort，commitTs 永久空洞"),

    /** preparestage: WAL PREPARE_AND_DURABLE fsync + staging seal */
    PREPARING(false, true, "WAL PREPARE_AND_DURABLE 失败则 abort"),

    /** dataalreadypersistenceto LMDB/Parquet, butnot yetpublication */
    DATA_DURABLE(
            false,
            true,
            "有 CommitMarker → redo COMMITTED；无 CommitMarker → abort (rollbackPendingInstance)"),

    /** Manifest committed(Metastore CAS TxnCommitRecord), globalvisibleswitch enabled */
    MANIFEST_COMMITTED(true, false, "redo publish"),

    /** versionalreadypublication(VersionTracker + latestSnapshot update) */
    PUBLISHED(true, false, "redo COMMITTED + ACK"),

    /** transactioncommittedconfirm(WAL COMMITTED fsync + ledger ACKED) */
    COMMITTED(true, false, "重试返回 already committed（幂等）"),

    /** final confirmationstate(terminal state) */
    ACKED(true, false, "终态，无需恢复动作"),

    /** currentlyrollback: cleanup staging, write WAL TXN_ABORT */
    ABORTING(false, false, "清理 staging，写 TXN_ABORT"),

    /** alreadyrollback(terminal state) */
    ABORTED(false, false, "终态，无需恢复动作");

    private final boolean readable;
    private final boolean rollbackable;
    private final String recoveryDescription;

    TxnState(boolean readable, boolean rollbackable, String recoveryDescription) {
        this.readable = readable;
        this.rollbackable = rollbackable;
        this.recoveryDescription = recoveryDescription;
    }

    /** thestateunderdatawhetherto reader visible */
    public boolean isReadable() {
        return readable;
    }

    /** thestateunderwhethersupportrollbackoperation */
    public boolean isRollbackable() {
        return rollbackable;
    }

    /** crash recoverywhenhandleactiondescription */
    public String recoveryAction() {
        return recoveryDescription;
    }

    /**
     * determinefromcurrentlystatewhethercanbytransitiontotargetstate.
     *
     * @param target targetstate
     * @return true iftransitionvalid
     */
    public boolean canTransitionTo(TxnState target) {
        return switch (this) {
            case BEGINNING -> target == ACTIVE || target == ABORTING;
            case ACTIVE ->
                    target == VALIDATING
                            || target == PREPARING
                            || target == COMMITTED
                            || target == ABORTING;
            case VALIDATING -> target == PREPARING || target == ABORTING;
            case PREPARING -> target == DATA_DURABLE || target == ABORTING;
            case DATA_DURABLE ->
                    target == MANIFEST_COMMITTED || target == COMMITTED || target == ABORTING;
            case MANIFEST_COMMITTED -> target == PUBLISHED;
            case PUBLISHED -> target == COMMITTED || target == ABORTING;
            case COMMITTED -> target == ACKED;
            case ACKED -> false;
            case ABORTING -> target == ABORTED;
            case ABORTED -> false;
        };
    }

    /** determinefromcurrentlystatewhethercanbyrollback(enter abort path). */
    public boolean canRollback() {
        return this == BEGINNING
                || this == ACTIVE
                || this == VALIDATING
                || this == PREPARING
                || this == DATA_DURABLE;
    }

    /**
     * determinefromcurrentlystatewhether can conditionallyrollback. DATA_DURABLE stateunder, based
     * on whether has CommitMarker decide.
     */
    public boolean canConditionallyRollback() {
        return this == DATA_DURABLE;
    }
}
