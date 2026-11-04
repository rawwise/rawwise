package io.github.rawwise.engine.txn.ingest;

import java.io.IOException;
import java.util.Optional;

/**
 * CommitLedger persistencestorageinterface.
 *
 * <p>will {@link CommitLedger} persistenceto Metastore, via CAS ensurestatetransitionatomicity.
 *
 * <p>Metastore key pattern: /commit_ledger/{catalog}/{db}/{table}/{idempotencyKey}
 *
 * <p>CAS semantics:
 *
 * <ul>
 *   <li>{@link #save} — precondition: NotExists(key), firstcreate
 *   <li>{@link #updateState} — precondition: ValueEquals(key, oldSerializedValue),
 *       statetransitionatomicity
 * </ul>
 *
 * <p>Implementation class: {@link MetastoreCommitLedgerStore}
 */
public interface CommitLedgerStore {

    /**
     * firstsave CommitLedger(CAS: NotExists precondition).
     *
     * @param ledger needsave CommitLedger
     * @return true if CAS success(firstcreate), false ifalreadyexistsame idempotencyKey record
     * @throws IOException if Metastore readwritefail
     */
    boolean save(CommitLedger ledger) throws IOException;

    /**
     * by idempotencyKey load CommitLedger.
     *
     * <p>byin key include catalog/db/table path, thismethodvia prefix scan + suffix
     * matchimplementation.
     *
     * @param idempotencyKey idempotent key
     * @return CommitLedger(ifexist), otherwise Optional.empty()
     * @throws IOException if Metastore readwritefail
     */
    Optional<CommitLedger> load(String idempotencyKey) throws IOException;

    /**
     * by batchId load CommitLedger.
     *
     * <p>via prefix scan + deserializationthroughfilterimplementation(batchId in value in).
     *
     * @param batchId batch ID
     * @return CommitLedger(ifexist), otherwise Optional.empty()
     * @throws IOException if Metastore readwritefail
     */
    Optional<CommitLedger> loadByBatchId(String batchId) throws IOException;

    /**
     * by idempotencyKey delete CommitLedger.
     *
     * <p>onlyforterminal statecleanup(ACKED after GC), should notinactivetransactionabovecall.
     *
     * @param idempotencyKey idempotent key
     * @return true ifdeletesuccess, false ifrecorddoes not exist
     * @throws IOException if Metastore readwritefail
     */
    boolean delete(String idempotencyKey) throws IOException;

    /**
     * atomicstatetransition(CAS: ValueEquals precondition).
     *
     * <p>loadcurrently CommitLedger, verifystatetransitionvalidity, thenaftervia CAS updatestate.
     * ifconcurrentmodifiedcause CAS fail, throw {@link IllegalStateException}.
     *
     * @param idempotencyKey idempotent key
     * @param newState newstate
     * @return updateafter CommitLedger
     * @throws IOException if Metastore readwritefail
     * @throws IllegalStateException ifrecorddoes not exist、statetransitionnotvalid、or CAS fail
     */
    CommitLedger updateState(String idempotencyKey, BatchState newState) throws IOException;

    /**
     * List all incomplete (non-terminal) CommitLedgers.
     *
     * @return list of incomplete ledgers
     * @throws IOException if Metastore read/write fails
     */
    java.util.List<CommitLedger> listIncomplete() throws IOException;
}
