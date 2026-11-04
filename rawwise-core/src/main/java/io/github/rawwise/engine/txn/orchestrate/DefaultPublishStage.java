package io.github.rawwise.engine.txn.orchestrate;

import io.github.rawwise.engine.txn.ShardVersionTracker;
import io.github.rawwise.engine.txn.record.CommitType;
import io.github.rawwise.engine.txn.record.TxnCommitRecord;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.wal.TxnWALRecord;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * PublishStage default implementation.
 *
 * <p>can inject to support fault injection test.
 */
@Slf4j
public class DefaultPublishStage implements PublishStage {

    private static final String TXN_COMMIT_RECORD_PREFIX = "/txn_commit_record/";

    private ShardVersionTracker tracker;

    public DefaultPublishStage() {}

    public void setTracker(ShardVersionTracker tracker) {
        this.tracker = tracker;
    }

    @Override
    public PublishResult execute(
            TxnContext ctx,
            List<TxnWALRecord.TxnWriteIntent> writeIntents,
            Metastore metastore,
            String catalogName,
            String databaseName,
            String tableName,
            ValidateStage validateStage) {
        try {
            // Metastore CAS create TxnCommitRecord
            // Write full TxnCommitRecord (not just commitTs) with
            // commitType=OLTP. This is the cross-domain atomic commit contract (CCI-010).
            // Uses CAS (NotExists) for idempotency: if SnapshotManager already wrote the
            // record (or crash-recovery replay), CAS fails and we treat it as idempotent
            // success.
            boolean recordCreated =
                    createTxnCommitRecord(
                            metastore, catalogName, databaseName, tableName, ctx, writeIntents);

            if (!recordCreated) {
                log.warn(
                        "PublishStage: txn={} TxnCommitRecord already exists (idempotent)",
                        ctx.txnId());
                return PublishResult.ok(ctx.txnId());
            }

            // VersionTracker update — per-key via ShardVersionTracker
            if (tracker != null) {
                for (TxnWALRecord.TxnWriteIntent intent : writeIntents) {
                    tracker.setCommitTs(intent.keyBytes(), ctx.txnId(), ctx.txnId());
                }
                log.debug(
                        "PublishStage: txn={} updated {} key versions via tracker",
                        ctx.txnId(),
                        writeIntents.size());
            }
            for (TxnWALRecord.TxnWriteIntent intent : writeIntents) {
                validateStage.updateCommitTs(intent.primaryTableId(), ctx.txnId());
            }

            // latestSnapshot update — CAS with ValueEquals precondition (CCI-011)
            updateLatestSnapshot(metastore, catalogName, databaseName, tableName, ctx.txnId());

            log.info("PublishStage: txn={} published successfully", ctx.txnId());
            return PublishResult.ok(ctx.txnId());

        } catch (IOException e) {
            log.error("PublishStage: txn={} Metastore CAS failed", ctx.txnId(), e);
            return PublishResult.fail("Metastore CAS failed: " + e.getMessage());
        }
    }

    @Override
    public boolean createBatchTxnCommitRecord(
            Metastore metastore,
            String catalog,
            String db,
            String table,
            String batchId,
            List<Long> txnIds)
            throws IOException {
        String keyStr =
                TXN_COMMIT_RECORD_PREFIX + catalog + "/" + db + "/" + table + "/batch/" + batchId;
        byte[] key = keyStr.getBytes(StandardCharsets.UTF_8);

        String valueStr = txnIds.stream().map(String::valueOf).collect(Collectors.joining(","));
        byte[] value = valueStr.getBytes(StandardCharsets.UTF_8);

        List<Precondition> preconditions = List.of(new Precondition.NotExists(key));
        List<Mutation> mutations = List.of(new Mutation.Put(key, value));

        return metastore.compareAndSwap(preconditions, mutations);
    }

    /**
     * Creates a TxnCommitRecord in the Metastore using CAS (NotExists) for idempotency.
     *
     * <p>Writes the full {@link TxnCommitRecord} JSON (not just commitTs) with {@code
     * commitType=OLTP}. This is the cross-domain atomic commit contract — once this record exists,
     * the transaction is irrevocably committed (CCI-010).
     *
     * <p>If a record already exists for the given txnId (e.g., SnapshotManager wrote it first
     * during {@code commitSnapshotViaCatalog()}, or crash-recovery replay), the CAS fails and the
     * write is treated as an idempotent success.
     *
     * <p>Key format: {@code /txn_commit_record/{catalog}/{db}/{table}/{txnId}}
     *
     * @param metastore Metastore instance
     * @param catalog catalog name
     * @param db database name
     * @param table table name
     * @param ctx transaction context (provides txnId, tableIds, readSnapshot)
     * @param writeIntents write intents (provides schemaVersion)
     * @return true if this call created the record; false if it already existed (idempotent)
     */
    private boolean createTxnCommitRecord(
            Metastore metastore,
            String catalog,
            String db,
            String table,
            TxnContext ctx,
            List<TxnWALRecord.TxnWriteIntent> writeIntents)
            throws IOException {
        long txnId = ctx.txnId();

        // Determine schemaVersion from writeIntents (use first intent's version)
        int schemaVersion =
                (writeIntents == null || writeIntents.isEmpty())
                        ? 0
                        : writeIntents.get(0).schemaVersion();

        // Determine previousSnapshotId from readSnapshot (null for READ_COMMITTED)
        long previousSnapshotId =
                ctx.readSnapshot() != null ? ctx.readSnapshot().baseSnapshotId() : 0L;

        // Build the full TxnCommitRecord.
        // snapshotId is UNASSIGNED here because PublishStage runs before
        // SnapshotManager.commitSnapshotViaCatalog() assigns the actual
        // snapshot ID. SnapshotManager will overwrite this record (or write
        // a new one if CAS fails) with the real snapshotId.
        TxnCommitRecord record =
                new TxnCommitRecord(
                        txnId,
                        txnId, // commitTs ≡ txnId per unified strategy
                        ctx.primaryTableId(),
                        ctx.tableIds(),
                        TxnCommitRecord.UNASSIGNED_SNAPSHOT_ID,
                        previousSnapshotId,
                        schemaVersion,
                        System.currentTimeMillis(),
                        CommitType.OLTP);

        byte[] key = TxnCommitRecord.buildMetastoreKey(catalog, db, table, txnId);
        byte[] value = record.toJson();

        List<Precondition> preconditions = List.of(new Precondition.NotExists(key));
        List<Mutation> mutations = List.of(new Mutation.Put(key, value));

        boolean success = metastore.compareAndSwap(preconditions, mutations);
        if (success) {
            log.debug(
                    "PublishStage: TxnCommitRecord created for txn={} table={}.{}{}",
                    txnId,
                    db,
                    table,
                    "");
        }
        return success;
    }

    /**
     * update latestSnapshot reference, use Metastore CAS (ValueEquals) to ensure global atomicity
     * (CCI-011).
     *
     * <p>semantics:
     *
     * <ul>
     *   <li>read currently {@code /snapshot_latest/{catalog}/{db}/{table}} value
     *   <li>if current value >= new snapshotId, treat as idempotent success(concurrent commit
     *       already wrote updated version)
     *   <li>otherwise use {@link Precondition.ValueEquals} as precondition to execute CAS
     *   <li>CAS fail(concurrent commit)then retry, treat as idempotent success after exceeding
     *       limit
     * </ul>
     */
    private void updateLatestSnapshot(
            Metastore metastore, String catalog, String db, String table, long snapshotId)
            throws IOException {
        String keyStr = "/snapshot_latest/" + catalog + "/" + db + "/" + table;
        byte[] key = keyStr.getBytes(StandardCharsets.UTF_8);
        byte[] newValue = longToBytes(snapshotId);

        int maxRetries = 3;
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            Optional<byte[]> currentValueOpt = metastore.get(key);

            if (currentValueOpt.isPresent()) {
                long currentSnapshotId = bytesToLong(currentValueOpt.get());
                if (currentSnapshotId >= snapshotId) {
                    log.debug(
                            "PublishStage: latestSnapshot already at {} >= {} for {}.{}.{} (idempotent)",
                            currentSnapshotId,
                            snapshotId,
                            catalog,
                            db,
                            table);
                    return;
                }
            }

            Precondition precondition =
                    currentValueOpt.isPresent()
                            ? new Precondition.ValueEquals(key, currentValueOpt.get())
                            : new Precondition.NotExists(key);

            List<Precondition> preconditions = List.of(precondition);
            List<Mutation> mutations = List.of(new Mutation.Put(key, newValue));

            boolean success = metastore.compareAndSwap(preconditions, mutations);
            if (success) {
                log.debug(
                        "PublishStage: latestSnapshot updated to {} for {}.{}.{}",
                        snapshotId,
                        catalog,
                        db,
                        table);
                return;
            }

            log.debug(
                    "PublishStage: latestSnapshot CAS failed (attempt {}/{}) for {}.{}.{}",
                    attempt + 1,
                    maxRetries,
                    catalog,
                    db,
                    table);
        }

        log.warn(
                "PublishStage: latestSnapshot CAS failed after {} retries for {}.{}.{}, "
                        + "treating as idempotent (concurrent commit likely won)",
                maxRetries,
                catalog,
                db,
                table);
    }

    private static byte[] longToBytes(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    private static long bytesToLong(byte[] bytes) {
        if (bytes == null || bytes.length < Long.BYTES) {
            return 0L;
        }
        return ByteBuffer.wrap(bytes).getLong();
    }
}
