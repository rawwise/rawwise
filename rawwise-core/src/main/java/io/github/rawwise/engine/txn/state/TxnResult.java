package io.github.rawwise.engine.txn.state;

/**
 * unifiedtransactionoperationresulttype.
 *
 * <p>alltransactionoperation(commit、abort、batch commit)returnthis sealed
 * layertimestructureinstance. callpartyviamodematchhandlenotsameresulttype.
 */
public sealed interface TxnResult
        permits TxnResult.CommitOk,
                TxnResult.CommitConflict,
                TxnResult.AbortResult,
                TxnResult.AlreadyCommitted {

    /**
     * commitsuccessresult.
     *
     * @param commitTs commit timestamp(i.e. txnId)
     */
    record CommitOk(long commitTs) implements TxnResult {}

    /**
     * commitconflictresult.
     *
     * @param reason conflictoriginalbecausedescription
     */
    record CommitConflict(String reason) implements TxnResult {}

    /**
     * rollbackresult.
     *
     * @param txnId transaction ID
     * @param reason rollbackoriginalbecausedescription
     */
    record AbortResult(long txnId, String reason) implements TxnResult {}

    /**
     * idempotentreturn: transactioncommitted.
     *
     * @param commitTs beforeon commit commitTs
     */
    record AlreadyCommitted(long commitTs) implements TxnResult {}

    // --- convenientmethod ---

    /** whethersuccesscommit */
    default boolean isSuccess() {
        return this instanceof CommitOk;
    }

    /** whetherconflict */
    default boolean isConflict() {
        return this instanceof CommitConflict;
    }

    /** whetheralreadyrollback */
    default boolean isAborted() {
        return this instanceof AbortResult;
    }

    /** whethercommitted(idempotentreturn) */
    default boolean isAlreadyCommitted() {
        return this instanceof AlreadyCommitted;
    }

    /**
     * getresultmessage.
     *
     * @return resultdescription, successwhenis emptystring
     */
    default String message() {
        return switch (this) {
            case CommitOk ok -> "";
            case CommitConflict c -> c.reason();
            case AbortResult a -> a.reason();
            case AlreadyCommitted ac -> "already committed at " + ac.commitTs();
        };
    }
}
