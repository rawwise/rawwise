package io.github.rawwise.engine.txn.ingest;

import java.util.Map;
import java.util.Objects;

/**
 * Batch commitledgerthis — persistencestate machine.
 *
 * <p>persistenceto Metastore(key: /commit_ledger/{catalog}/{db}/{table}/{idempotencyKey}), resolve
 * ACK lose、Driver failover、partial publish threeclasscrash recoveryissue.
 *
 * <p>Core design:
 *
 * <ul>
 *   <li>eachBatch commitcreateone CommitLedger record, via CAS write Metastore
 *   <li>state machineby {@link BatchState} driver, eachtimestatetransitionvia CAS ensureatomicity
 *   <li>shardStates recordeachshardfine-grainedstate, support speculative execution dedup
 *   <li>digests storagecompleteityvalidatedigest, crash recoverywhenverifydatacompleteity
 *   <li>lease implementationleasemechanism, preventmultiple Driver whileoperationsameonebatch
 * </ul>
 *
 * <p>crashrecovery decision:
 *
 * <ul>
 *   <li>OPEN / SHARDS_PREPARED / DATA_DURABLE → abortBatch(datanotglobalvisible)
 *   <li>GLOBAL_MARKER_COMMITTED / PUBLISHING → continueCommit(replay PublishStage)
 *   <li>COMMITTED / ACKED → already committed(idempotentreturn)
 * </ul>
 *
 * @param ledgerVersion ledgerthisformatversion(currently "v1")
 * @param idempotencyKey idempotent key(typicallyetcin batchId, ensuresameonebatchonlycommitonetime)
 * @param batchId batch ID
 * @param txnId transaction ID(Server sideallocation)
 * @param tableId table identifier(format: catalog.database.table or database.table)
 * @param state batchstate({@link BatchState})
 * @param readSnapshotTs readsnapshot timestamp(begin whencapture)
 * @param commitTs commit timestamp(commit whenvia LogicalClock allocation)
 * @param expectedShardCount expectedshardcount
 * @param preparedShardCount already PREPARED shardcount
 * @param publishedShardCount already PUBLISHED shardcount
 * @param shardStates shardlevelstate Map(shardId → ShardLedgerState)
 * @param digests digestinfoset
 * @param lease leaseinfo
 * @param createdAtMs createwhenbetween(epoch millis)
 * @param updatedAtMs mostafterupdatewhenbetween(epoch millis)
 */
public record CommitLedger(
        String ledgerVersion,
        String idempotencyKey,
        String batchId,
        long txnId,
        String tableId,
        BatchState state,
        long readSnapshotTs,
        long commitTs,
        int expectedShardCount,
        int preparedShardCount,
        int publishedShardCount,
        Map<Integer, ShardLedgerState> shardStates,
        Digests digests,
        LeaseInfo lease,
        long createdAtMs,
        long updatedAtMs) {

    public CommitLedger {
        Objects.requireNonNull(ledgerVersion, "ledgerVersion must not be null");
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must not be blank");
        }
        if (batchId == null || batchId.isBlank()) {
            throw new IllegalArgumentException("batchId must not be blank");
        }
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(digests, "digests must not be null");
        shardStates = shardStates == null ? Map.of() : Map.copyOf(shardStates);
        if (expectedShardCount < 1) {
            throw new IllegalArgumentException(
                    "expectedShardCount must be >= 1, got: " + expectedShardCount);
        }
    }

    /**
     * createinitial OPEN state CommitLedger.
     *
     * @param idempotencyKey idempotent key
     * @param batchId batch ID
     * @param tableId table identifier
     * @param expectedShardCount expectedshardcount
     * @param readSnapshotTs readsnapshot timestamp
     * @param lease leaseinfo
     * @return OPEN state CommitLedger
     */
    public static CommitLedger open(
            String idempotencyKey,
            String batchId,
            String tableId,
            int expectedShardCount,
            long readSnapshotTs,
            LeaseInfo lease) {
        long now = System.currentTimeMillis();
        return new CommitLedger(
                "v1",
                idempotencyKey,
                batchId,
                0L,
                tableId,
                BatchState.OPEN,
                readSnapshotTs,
                0L,
                expectedShardCount,
                0,
                0,
                Map.of(),
                Digests.empty(),
                lease,
                now,
                now);
    }

    /**
     * Immutable statetransition: returnnewstate CommitLedger replica.
     *
     * @param newState newstate
     * @return newstate CommitLedger
     * @throws IllegalStateException ifstatetransitionnotvalid
     */
    public CommitLedger withState(BatchState newState) {
        if (!state.canTransitionTo(newState)) {
            throw new IllegalStateException(
                    "Illegal batch state transition: "
                            + state
                            + " → "
                            + newState
                            + " (batchId="
                            + batchId
                            + ")");
        }
        return new CommitLedger(
                ledgerVersion,
                idempotencyKey,
                batchId,
                txnId,
                tableId,
                newState,
                readSnapshotTs,
                commitTs,
                expectedShardCount,
                preparedShardCount,
                publishedShardCount,
                shardStates,
                digests,
                lease,
                createdAtMs,
                System.currentTimeMillis());
    }

    /**
     * Immutable update: set commitTs and txnId.
     *
     * @param newTxnId transaction ID
     * @param newCommitTs commit timestamp
     * @return updateafter CommitLedger
     */
    public CommitLedger withCommitInfo(long newTxnId, long newCommitTs) {
        return new CommitLedger(
                ledgerVersion,
                idempotencyKey,
                batchId,
                newTxnId,
                tableId,
                state,
                readSnapshotTs,
                newCommitTs,
                expectedShardCount,
                preparedShardCount,
                publishedShardCount,
                shardStates,
                digests,
                lease,
                createdAtMs,
                System.currentTimeMillis());
    }

    /**
     * Immutable update: replace shardStates andupdatecountcount.
     *
     * @param newShardStates newshardstate Map
     * @return updateafter CommitLedger
     */
    public CommitLedger withShardStates(Map<Integer, ShardLedgerState> newShardStates) {
        Map<Integer, ShardLedgerState> copied =
                newShardStates == null ? Map.of() : Map.copyOf(newShardStates);
        int prepared =
                (int)
                        copied.values().stream()
                                .filter(s -> s.status() == ShardStatus.PREPARED)
                                .count();
        int published =
                (int)
                        copied.values().stream()
                                .filter(s -> s.status() == ShardStatus.PUBLISHED)
                                .count();
        return new CommitLedger(
                ledgerVersion,
                idempotencyKey,
                batchId,
                txnId,
                tableId,
                state,
                readSnapshotTs,
                commitTs,
                expectedShardCount,
                prepared,
                published,
                copied,
                digests,
                lease,
                createdAtMs,
                System.currentTimeMillis());
    }

    /**
     * Immutable update: replace digests.
     *
     * @param newDigests newdigestinfo
     * @return updateafter CommitLedger
     */
    public CommitLedger withDigests(Digests newDigests) {
        return new CommitLedger(
                ledgerVersion,
                idempotencyKey,
                batchId,
                txnId,
                tableId,
                state,
                readSnapshotTs,
                commitTs,
                expectedShardCount,
                preparedShardCount,
                publishedShardCount,
                shardStates,
                newDigests,
                lease,
                createdAtMs,
                System.currentTimeMillis());
    }

    /**
     * determineallshardwhetherallalready PUBLISHED.
     *
     * @return true if publishedShardCount == expectedShardCount
     */
    public boolean allShardsPublished() {
        return publishedShardCount >= expectedShardCount;
    }

    /**
     * determineallshardwhetherallalreadyat least PREPARED(including DATA_DURABLE and PUBLISHED).
     *
     * @return true if preparedShardCount + publishedShardCount >= expectedShardCount
     */
    public boolean allShardsPrepared() {
        return preparedShardCount + publishedShardCount >= expectedShardCount;
    }
}
