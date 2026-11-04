package io.github.rawwise.engine.txn.ingest;

/**
 * Batch commitlifecyclestateenum(persistencestate machine).
 *
 * <p>thisenumfor {@link CommitLedger} state field, driver crashrecovery decision.
 *
 * <p>statetransitionpath(normalcommit):
 *
 * <pre>
 * OPEN → SHARDS_PREPARED → DATA_DURABLE → GLOBAL_MARKER_COMMITTED → PUBLISHING → COMMITTED → ACKED
 * </pre>
 *
 * <p>statetransitionpath(rollback):
 *
 * <pre>
 * OPEN → ABORTED
 * SHARDS_PREPARED → ABORTED
 * DATA_DURABLE → ABORTED
 * </pre>
 *
 * <p>notrollbackboundary: {@link #GLOBAL_MARKER_COMMITTED} ofafternotrollback, becauseas
 * GlobalCommitMarker alreadywrite Metastore, Read pathcanenergyalreadyvisible.
 *
 * <p>recovery decision:
 *
 * <ul>
 *   <li>OPEN / SHARDS_PREPARED / DATA_DURABLE → abortBatch(datanotglobalvisible)
 *   <li>GLOBAL_MARKER_COMMITTED / PUBLISHING → continueCommit(replay PublishStage)
 *   <li>COMMITTED / ACKED → already committed(idempotentreturn)
 *   <li>ABORTED → already aborted
 *   <li>FAILED_NEEDS_RECOVERY → requires manual intervention orautorecovery
 * </ul>
 *
 * <p>terminal state: {@link #ACKED}、{@link #ABORTED}、{@link #FAILED_NEEDS_RECOVERY},
 * notagaintransition.
 *
 * <p>note: thisenumand {@code io.github.rawwise.engine.txn.orchestrate.BatchState}(5 state)
 * isnotsamestate machine.orchestrate.BatchState for Node side TxnOrchestrator memorystate machine;
 * thisenumfor Server side CommitLedger persistencestate machine,
 * includemoremultiplerecoveryrelatedstate.
 */
public enum BatchState {

    /** batch opened, write currently in progress.can abort. */
    OPEN(false, true, false, "写入进行中，可 abort"),

    /** allshardwritealreadycomplete, datain WriteBuffer / PENDING Instance in.can abort. */
    SHARDS_PREPARED(false, true, false, "分片写入完成，可 abort"),

    /** dataalreadypersistenceto LMDB PENDING Instance / Parquet file.can abort(cleanup PENDING). */
    DATA_DURABLE(false, true, false, "数据已持久化，可 abort"),

    /**
     * GlobalCommitMarker alreadyvia CAS write Metastore.
     *
     * <p><b>notrollbackboundary</b>: Read pathcanenergyalreadyvisiblethisbatchdata. crash
     * recoverywhenneed continueCommit(replay PublishStage).
     */
    GLOBAL_MARKER_COMMITTED(false, false, true, "全局标记已提交，不可回滚"),

    /** currentlypublicationversion(VersionTracker + latestSnapshot updatein).notrollback. */
    PUBLISHING(false, false, true, "发布中，不可回滚"),

    /** batchcommitted, datavisible.wait ACK. */
    COMMITTED(true, false, false, "已提交，数据可见"),

    /** terminal state: clientalready ACK. */
    ACKED(true, false, false, "终态，已 ACK"),

    /** terminal state: batch aborted, data cleaned up. */
    ABORTED(false, false, false, "终态，已中止"),

    /** terminal state: requiresrecoveryintervention(autorecoveryfailordatacorrupt). */
    FAILED_NEEDS_RECOVERY(false, false, false, "终态，需要恢复");

    private final boolean readable;
    private final boolean abortable;
    private final boolean pastPointOfNoReturn;
    private final String description;

    BatchState(
            boolean readable, boolean abortable, boolean pastPointOfNoReturn, String description) {
        this.readable = readable;
        this.abortable = abortable;
        this.pastPointOfNoReturn = pastPointOfNoReturn;
        this.description = description;
    }

    /** thestateunderdatawhetherto reader visible */
    public boolean isReadable() {
        return readable;
    }

    /** thestateunderwhethersupport abort operation */
    public boolean isAbortable() {
        return abortable;
    }

    /** whether exceeded non-rollbackboundary(GlobalCommitMarker alreadywrite) */
    public boolean isPastPointOfNoReturn() {
        return pastPointOfNoReturn;
    }

    /** whether is terminal state */
    public boolean isTerminal() {
        return this == ACKED || this == ABORTED || this == FAILED_NEEDS_RECOVERY;
    }

    /** statedescription */
    public String description() {
        return description;
    }

    /**
     * determinefromcurrentlystatewhethercanbytransitiontotargetstate.
     *
     * <p>validtransition(normalpath):
     *
     * <pre>
     * OPEN → SHARDS_PREPARED
     * SHARDS_PREPARED → DATA_DURABLE
     * DATA_DURABLE → GLOBAL_MARKER_COMMITTED
     * GLOBAL_MARKER_COMMITTED → PUBLISHING
     * PUBLISHING → COMMITTED
     * COMMITTED → ACKED
     * </pre>
     *
     * <p>validtransition(rollbackpath):
     *
     * <pre>
     * OPEN → ABORTED
     * SHARDS_PREPARED → ABORTED
     * DATA_DURABLE → ABORTED
     * </pre>
     *
     * <p>validtransition(recoverypath):
     *
     * <pre>
     * GLOBAL_MARKER_COMMITTED → COMMITTED (replay PublishStage afterdirectlyskipto COMMITTED)
     * PUBLISHING → COMMITTED
     * any non-terminal state → FAILED_NEEDS_RECOVERY
     * </pre>
     *
     * @param target targetstate
     * @return true iftransitionvalid
     */
    public boolean canTransitionTo(BatchState target) {
        if (this == target) {
            return false;
        }
        return switch (this) {
            case OPEN ->
                    target == SHARDS_PREPARED
                            || target == ABORTED
                            || target == FAILED_NEEDS_RECOVERY;
            case SHARDS_PREPARED ->
                    target == DATA_DURABLE || target == ABORTED || target == FAILED_NEEDS_RECOVERY;
            case DATA_DURABLE ->
                    target == GLOBAL_MARKER_COMMITTED
                            || target == ABORTED
                            || target == COMMITTED
                            || target == FAILED_NEEDS_RECOVERY;
            case GLOBAL_MARKER_COMMITTED ->
                    target == PUBLISHING || target == COMMITTED || target == FAILED_NEEDS_RECOVERY;
            case PUBLISHING -> target == COMMITTED || target == FAILED_NEEDS_RECOVERY;
            case COMMITTED -> target == ACKED || target == FAILED_NEEDS_RECOVERY;
            case ACKED, ABORTED, FAILED_NEEDS_RECOVERY -> false;
        };
    }
}
