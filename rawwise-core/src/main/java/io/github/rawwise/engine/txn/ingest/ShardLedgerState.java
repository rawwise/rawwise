package io.github.rawwise.engine.txn.ingest;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * singleitemshard(Shard)in CommitLedger inpersistencestate.
 *
 * <p>for {@link CommitLedger} shardStates field, recordeachshardwrite、persistence、publicationstate,
 * supportcrash recoverywhenbyshardgranularitydecision(speculative execution dedup).
 *
 * @param shardId shard ID
 * @param partitionId Spark partition ID(typicallyand shardId consistent, butallownotsame)
 * @param taskAttemptId Spark taskAttemptId(for speculative execution dedup)
 * @param preparedTxnId shardleveltransaction ID(Node side TxnOrchestrator allocation)
 * @param dataRefs datafilereferencelist(such as parquet://warehouse/.../part-00012.parquet)
 * @param rowCount theshardwriterowcount
 * @param minSeq minimumsequencenumber
 * @param maxSeq maximumsequencenumber
 * @param dataDigest datadigest(Hex format CRC32C)
 * @param status shardstate({@link ShardStatus})
 */
public record ShardLedgerState(
        int shardId,
        int partitionId,
        String taskAttemptId,
        String preparedTxnId,
        List<String> dataRefs,
        long rowCount,
        long minSeq,
        long maxSeq,
        String dataDigest,
        ShardStatus status) {

    public ShardLedgerState {
        Objects.requireNonNull(taskAttemptId, "taskAttemptId must not be null");
        Objects.requireNonNull(preparedTxnId, "preparedTxnId must not be null");
        Objects.requireNonNull(dataDigest, "dataDigest must not be null");
        Objects.requireNonNull(status, "status must not be null");
        dataRefs = dataRefs == null ? List.of() : List.copyOf(dataRefs);
    }

    /**
     * createonestatetransitionafternew ShardLedgerState(immutable update).
     *
     * @param newStatus newstate
     * @return new ShardLedgerState instance
     */
    public ShardLedgerState withStatus(ShardStatus newStatus) {
        return new ShardLedgerState(
                shardId,
                partitionId,
                taskAttemptId,
                preparedTxnId,
                dataRefs,
                rowCount,
                minSeq,
                maxSeq,
                dataDigest,
                newStatus);
    }

    /**
     * factorymethod: createinitial PREPARED state ShardLedgerState.
     *
     * @param shardId shard ID
     * @param partitionId partition ID
     * @param taskAttemptId task attempt ID
     * @param preparedTxnId prepared transaction ID
     * @return PREPARED state ShardLedgerState
     */
    public static ShardLedgerState prepared(
            int shardId, int partitionId, String taskAttemptId, String preparedTxnId) {
        return new ShardLedgerState(
                shardId,
                partitionId,
                taskAttemptId,
                preparedTxnId,
                List.of(),
                0L,
                0L,
                0L,
                "",
                ShardStatus.PREPARED);
    }

    /**
     * from ShardLedgerState transitionas ShardRef(for GlobalCommitMarker).
     *
     * @return corresponding ShardRef
     */
    public ShardRef toShardRef() {
        return new ShardRef(
                shardId,
                partitionId,
                taskAttemptId,
                preparedTxnId,
                dataRefs,
                rowCount,
                minSeq,
                maxSeq,
                dataDigest,
                status);
    }

    /**
     * from Map&lt;Integer, ShardLedgerState&gt; transitionas List&lt;ShardRef&gt;.
     *
     * @param shardStates shard state Map
     * @return ShardRef list
     */
    public static List<ShardRef> toShardRefs(Map<Integer, ShardLedgerState> shardStates) {
        if (shardStates == null || shardStates.isEmpty()) {
            return List.of();
        }
        return shardStates.values().stream().map(ShardLedgerState::toShardRef).toList();
    }
}
