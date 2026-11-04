package io.github.rawwise.engine.txn.ingest;

import java.util.List;
import java.util.Objects;

/**
 * GlobalCommitMarker inshardreference.
 *
 * <p>for {@link GlobalCommitMarker} shards field, recordthebatchinvolveallshardand
 * itsdatareference, isRead pathlocatedatabased on.
 *
 * <p>and {@link ShardLedgerState} fieldphasesame, butsemanticsnotsame:
 *
 * <ul>
 *   <li>ShardRef is GlobalCommitMarker immutable insnapshot(on commitmomentshardstate)
 *   <li>ShardLedgerState is CommitLedger incanchangestate(followcommitprogressupdate)
 * </ul>
 *
 * @param shardId shard ID
 * @param partitionId Spark partition ID
 * @param taskAttemptId Spark taskAttemptId
 * @param preparedTxnId shardleveltransaction ID
 * @param dataRefs datafilereferencelist
 * @param rowCount rowcount
 * @param minSeq minimumsequencenumber
 * @param maxSeq maximumsequencenumber
 * @param dataDigest datadigest
 * @param status shardstate
 */
public record ShardRef(
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

    public ShardRef {
        Objects.requireNonNull(taskAttemptId, "taskAttemptId must not be null");
        Objects.requireNonNull(preparedTxnId, "preparedTxnId must not be null");
        Objects.requireNonNull(dataDigest, "dataDigest must not be null");
        Objects.requireNonNull(status, "status must not be null");
        dataRefs = dataRefs == null ? List.of() : List.copyOf(dataRefs);
    }
}
