package io.github.rawwise.engine.txn.orchestrate;

/**
 * Batch commitlifecyclestateenum.
 *
 * <p>statetransitionpath(commit--completepath, includingrecoveryscenario):
 *
 * <pre>
 * OPEN → SHARDS_PREPARED → DATA_DURABLE → COMMITTED
 * </pre>
 *
 * <p>statetransitionpath(commit--directlypath, commit()
 * internalalreadycompletepersistence+publication):
 *
 * <pre>
 * SHARDS_PREPARED → COMMITTED (skip DATA_DURABLE, becauseas commit() alreadyensurepersistence)
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
 * <p>terminal state: COMMITTED、ABORTED, notagaintransition.
 *
 * <p>note: {@code SHARDS_PREPARED → COMMITTED} directlyjumponlyin commit() return CommitOk
 * whenuse.thiswhendataalreadyvia commit() pipeline DurableStage + PublishStage
 * completepersistenceandglobalpublication, skip DATA_DURABLE caneliminate race window with
 * abortBatch--prevent alreadyglobalvisibledatabemarknoteas ABORTED.
 */
public enum BatchState {

    /** batch opened, write currently in progress */
    OPEN(false, true, "写入进行中，可 abort"),

    /** all shard writes complete, data in WriteBuffer */
    SHARDS_PREPARED(false, true, "分片写入完成，可 abort"),

    /** data persisted to LMDB PENDING instance */
    DATA_DURABLE(false, true, "数据已持久化，可 abort"),

    /** batchcommitted, datavisible */
    COMMITTED(true, false, "终态，已提交"),

    /** batch aborted, data cleaned up */
    ABORTED(false, false, "终态，已中止");

    private final boolean readable;
    private final boolean abortable;
    private final String description;

    BatchState(boolean readable, boolean abortable, String description) {
        this.readable = readable;
        this.abortable = abortable;
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

    /** statedescription */
    public String description() {
        return description;
    }

    /**
     * determinefromcurrentlystatewhethercanbytransitiontotargetstate.
     *
     * @param target targetstate
     * @return true iftransitionvalid
     */
    public boolean canTransitionTo(BatchState target) {
        return switch (this) {
            case OPEN -> target == SHARDS_PREPARED || target == ABORTED;
            case SHARDS_PREPARED ->
                    target == DATA_DURABLE || target == COMMITTED || target == ABORTED;
            case DATA_DURABLE -> target == COMMITTED || target == ABORTED;
            case COMMITTED, ABORTED -> false;
        };
    }
}
