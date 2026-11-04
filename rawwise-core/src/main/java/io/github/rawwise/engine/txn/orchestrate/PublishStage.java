package io.github.rawwise.engine.txn.orchestrate;

import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.wal.TxnWALRecord;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.util.List;

/**
 * PublishStage: versionpublicationstageinterface.
 *
 * <p>Responsibilities (in order):
 *
 * <ol>
 *   <li>4a. CommitMarker write(PENDING → ACTIVE, Batch path)
 *   <li>4b. Metastore CAS create TxnCommitRecord
 *   <li>4c. UpdateLog commit (handled by LazyUpdateManager, not in this phase)
 *   <li>4d. GlobalCommitMarker CAS(batch mode only)
 *   <li>4e. VersionTracker update(commitTs)
 *   <li>4f. latestSnapshot update via SnapshotManager (CCI-011)
 * </ol>
 *
 * <h3>batch optimization</h3>
 *
 * <p>single transaction writing TxnCommitRecord's Metastore write frequency can via Group Commit
 * aggregation reduction: each batch aggregates 1 batch write, reduce frequency from ~1000/s to
 * ~17/s.
 */
public interface PublishStage {

    /** publicationresult. */
    record PublishResult(boolean success, long publishedCommitTs, String errorMessage) {

        public static PublishResult ok(long commitTs) {
            return new PublishResult(true, commitTs, "");
        }

        public static PublishResult fail(String error) {
            return new PublishResult(false, 0, error);
        }
    }

    /**
     * executeversionpublication.
     *
     * @param ctx transaction context
     * @param writeIntents writeintentlist
     * @param metastore Metastore instance
     * @param catalogName Catalog name
     * @param databaseName Database name
     * @param tableName table name
     * @param validateStage ValidateStage(forupdate commitTs)
     * @return publicationresult
     */
    PublishResult execute(
            TxnContext ctx,
            List<TxnWALRecord.TxnWriteIntent> writeIntents,
            Metastore metastore,
            String catalogName,
            String databaseName,
            String tableName,
            ValidateStage validateStage);

    /** batch create TxnCommitRecord (Group Commit optimization). */
    boolean createBatchTxnCommitRecord(
            Metastore metastore,
            String catalog,
            String db,
            String table,
            String batchId,
            List<Long> txnIds)
            throws IOException;
}
