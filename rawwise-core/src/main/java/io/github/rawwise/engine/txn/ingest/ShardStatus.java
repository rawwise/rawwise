package io.github.rawwise.engine.txn.ingest;

/**
 * singleitemshard(Shard)inBatch commitinpersistencestate.
 *
 * <p>for ShardRef.status fieldand ShardLedgerState.status field.
 *
 * <p>statetransitionpath:
 *
 * <pre>
 * PREPARED → DATA_DURABLE → PUBLISHED
 * </pre>
 *
 * <p>semantics:
 *
 * <ul>
 *   <li>{@link #PREPARED} — shardwritecomplete, datain WriteBuffer / staging in, notpersistence
 *   <li>{@link #DATA_DURABLE} — dataalreadypersistenceto LMDB PENDING Instance / Parquet file
 *   <li>{@link #PUBLISHED} — CommitMarker alreadywrite, datato reader visible
 * </ul>
 */
public enum ShardStatus {

    /** shardwritecomplete, datain WriteBuffer / staging in, notpersistence */
    PREPARED,

    /** dataalreadypersistenceto LMDB PENDING Instance / Parquet file */
    DATA_DURABLE,

    /** CommitMarker alreadywrite, datato reader visible */
    PUBLISHED;

    /**
     * determinefromcurrentlystatewhethercanbytransitiontotargetstate.
     *
     * @param target targetstate
     * @return true iftransitionvalid
     */
    public boolean canTransitionTo(ShardStatus target) {
        if (this == target) {
            return false;
        }
        return switch (this) {
            case PREPARED -> target == DATA_DURABLE || target == PUBLISHED;
            case DATA_DURABLE -> target == PUBLISHED;
            case PUBLISHED -> false;
        };
    }
}
