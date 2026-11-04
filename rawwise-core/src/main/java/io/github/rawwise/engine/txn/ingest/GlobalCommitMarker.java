package io.github.rawwise.engine.txn.ingest;

import java.util.List;
import java.util.Objects;

/**
 * globalatomiccommitmark — Read pathvisibleityuniqueswitch.
 *
 * <p>persistenceto Metastore (key: /commit_marker/{catalog}/{db}/{table}/{batchId}), via CAS
 * create(precondition: not exists).
 *
 * <p>Core design:
 *
 * <ul>
 *   <li>nohas marker datanever readable(Read pathvisibleityuniqueswitch)
 *   <li>GlobalCommitMarker CAS alwaysby Server sideexecute, becauseasrequirescross Node atomicity
 *   <li>ServerTxnCoordinator.commitBatch() aggregatetotalall shard resultafterexecute CAS
 *   <li>marker createsuccess = notrollbackboundary(CommitLedger enter GLOBAL_MARKER_COMMITTED
 *       state)
 * </ul>
 *
 * <p>concurrentcontentionsemantics(E2E-006b):
 *
 * <ul>
 *   <li>two batch while CAS sameone marker → onlyonesuccess, failpartyreturn conflict
 *   <li>CAS precondition: NotExists(key) → ensure marker onlycreateonetime
 * </ul>
 *
 * @param markerVersion marker formatversion(currently "v1")
 * @param catalog catalog name
 * @param database Database name
 * @param table table name
 * @param batchId batch ID
 * @param txnId transaction ID
 * @param commitTs commit timestamp
 * @param snapshotId snapshot ID(SnapshotManager allocation)
 * @param schemaVersion schema version
 * @param state marker state({@link MarkerState})
 * @param createdAtMs createwhenbetween(epoch millis)
 * @param committedAtMs on commitbetween(epoch millis, state=COMMITTED whenset)
 * @param writer writerinfo
 * @param shards shardreferencelist
 * @param manifestDigest Manifest digest
 * @param idempotencyKey idempotent key
 * @param previousSnapshotId beforeonesnapshot ID(for snapshot chain)
 */
public record GlobalCommitMarker(
        String markerVersion,
        String catalog,
        String database,
        String table,
        String batchId,
        long txnId,
        long commitTs,
        long snapshotId,
        int schemaVersion,
        MarkerState state,
        long createdAtMs,
        long committedAtMs,
        WriterInfo writer,
        List<ShardRef> shards,
        String manifestDigest,
        String idempotencyKey,
        long previousSnapshotId) {

    public GlobalCommitMarker {
        Objects.requireNonNull(markerVersion, "markerVersion must not be null");
        if (catalog == null || catalog.isBlank()) {
            throw new IllegalArgumentException("catalog must not be blank");
        }
        if (database == null || database.isBlank()) {
            throw new IllegalArgumentException("database must not be blank");
        }
        if (table == null || table.isBlank()) {
            throw new IllegalArgumentException("table must not be blank");
        }
        if (batchId == null || batchId.isBlank()) {
            throw new IllegalArgumentException("batchId must not be blank");
        }
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(writer, "writer must not be null");
        Objects.requireNonNull(manifestDigest, "manifestDigest must not be null");
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must not be blank");
        }
        shards = shards == null ? List.of() : List.copyOf(shards);
    }

    /**
     * constructcompletetable identifier(catalog.database.table).
     *
     * @return table identifierstring
     */
    public String tableId() {
        return catalog + "." + database + "." + table;
    }

    /**
     * determine marker whethercommitted(Read pathvisible).
     *
     * @return true if state == COMMITTED
     */
    public boolean isCommitted() {
        return state == MarkerState.COMMITTED;
    }

    /**
     * Immutable statetransition: returnnewstate GlobalCommitMarker replica.
     *
     * @param newState newstate
     * @return newstate GlobalCommitMarker
     */
    public GlobalCommitMarker withState(MarkerState newState) {
        long now = System.currentTimeMillis();
        long committedAt = newState == MarkerState.COMMITTED ? now : committedAtMs;
        return new GlobalCommitMarker(
                markerVersion,
                catalog,
                database,
                table,
                batchId,
                txnId,
                commitTs,
                snapshotId,
                schemaVersion,
                newState,
                createdAtMs,
                committedAt,
                writer,
                shards,
                manifestDigest,
                idempotencyKey,
                previousSnapshotId);
    }
}
