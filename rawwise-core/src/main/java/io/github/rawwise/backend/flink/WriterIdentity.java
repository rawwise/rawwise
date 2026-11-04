package io.github.rawwise.backend.flink;

import java.io.Serializable;
import java.util.Objects;

/**
 * Flink writer identity — used forgenerate deterministic idempotency key.
 *
 * <p>core design:
 *
 * <ul>
 *   <li>engineType fixed to "FLINK", identifieswrite engine source
 *   <li>appId corresponds to Flink JobID, ensuressamejob committables traceable
 *   <li>queryId identifieslogical query(usually equals tableId), forcross-jobdistinguish
 *   <li>epochId identifies Flink job epoch(restart count), used fordistinguishdifferent job
 *       instances
 *   <li>attemptId identifiestask attempt count, used for task retry scenariounder idempotent
 *       deduplication
 * </ul>
 *
 * <p>idempotentkeyformat: {@code flink-{appId}-{checkpointId}-{attemptId}} <br>
 * where checkpointId from {@link RawWiseCommittable#checkpointId()}, ensuressame checkpoint
 * duplicate commit(crash recovery / retry)producesameidempotentkey, thusvia {@link
 * io.github.rawwise.engine.txn.ingest.CommitLedgerStore#load} detect committed stateand return
 * idempotently.
 *
 * <p>Serializable: Flink Sink needs serialization WriterIdentity to TaskManager.
 *
 * @param engineType engine type(fixed "FLINK")
 * @param appId Flink JobID string
 * @param queryId logical query identifier(usually tableId)
 * @param epochId job epoch(restart count)
 * @param attemptId task attempt ID
 */
public record WriterIdentity(
        String engineType, String appId, String queryId, long epochId, long attemptId)
        implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String ENGINE_TYPE_FLINK = "FLINK";

    public WriterIdentity {
        Objects.requireNonNull(engineType, "engineType must not be null");
        if (appId == null || appId.isBlank()) {
            throw new IllegalArgumentException("appId must not be blank");
        }
        if (queryId == null) {
            queryId = "";
        }
    }

    /**
     * Convenience factory: creates a WriterIdentity for the Flink engine.
     *
     * @param appId Flink JobID
     * @param queryId logical query identifier (tableId)
     * @param epochId job epoch
     * @param attemptId task attempt ID
     * @return FLINK-type WriterIdentity
     */
    public static WriterIdentity flink(String appId, String queryId, long epochId, long attemptId) {
        return new WriterIdentity(ENGINE_TYPE_FLINK, appId, queryId, epochId, attemptId);
    }

    /**
     * generate deterministic idempotency key.
     *
     * <p>format: {@code flink-{appId}-{checkpointId}-{attemptId}}
     *
     * <p>idempotency semantics:
     *
     * <ul>
     *   <li>same checkpoint + same attempt → same key → CommitLedger already exists → idempotent
     *       return
     *   <li>same checkpoint + different attempt(task retry)→ different key → need via pendingId
     *       deduplication
     *   <li>different checkpoint → different key → normal commit(no conflict)
     * </ul>
     *
     * @param checkpointId Flink checkpoint ID(from RawWiseCommittable)
     * @return deterministic idempotency key string
     */
    public String idempotencyKey(long checkpointId) {
        return engineType.toLowerCase() + "-" + appId + "-" + checkpointId + "-" + attemptId;
    }

    /**
     * Generate batchId (used for CommitLedger.batchId field).
     *
     * <p>Format: {@code flink-{appId}-ckpt{checkpointId}-attempt{attemptId}}
     *
     * @param checkpointId Flink checkpoint ID
     * @return batchId string
     */
    public String batchId(long checkpointId) {
        return engineType.toLowerCase()
                + "-"
                + appId
                + "-ckpt"
                + checkpointId
                + "-attempt"
                + attemptId;
    }
}
