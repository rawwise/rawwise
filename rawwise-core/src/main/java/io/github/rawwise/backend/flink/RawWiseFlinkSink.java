package io.github.rawwise.backend.flink;

import io.github.rawwise.backend.flink.config.FlinkRawWiseConfig;
import io.github.rawwise.engine.storage.ingestion.IngestionDataSink;
import io.github.rawwise.engine.storage.ingestion.IngestionDataSinkFactory;
import io.github.rawwise.engine.storage.manifest.SnapshotOperations;
import io.github.rawwise.engine.storage.manifest.SnapshotOperationsSupplier;
import io.github.rawwise.engine.txn.ingest.CommitLedgerStore;
import io.github.rawwise.governance.config.RawWiseOptions;
import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * RawWise Flink Sink — two-phase commit entry point.
 *
 * <h2>2PC idempotency integration</h2>
 *
 * <p>Sink inat constructiongeneratestable {@link WriterIdentity}(appId is UUID, remains unchanged
 * after serialization), passto {@link RawWiseFlinkCommitter} used forgenerate deterministic
 * idempotency key:
 *
 * <pre>
 * idempotencyKey = flink-{appId}-{checkpointId}-{attemptId}
 * </pre>
 *
 * <p>optional {@link CommitLedgerStoreSupplier} provide {@link CommitLedgerStore} instance,
 * enabling Committer can integrate CommitLedger persistedstatemachine, implementcrash
 * recoveryandidempotent commit. when not providedwhen, Committer fallbacktodirect commit
 * mode(backward compatibility).
 *
 * <h2>double commit prevention (Double Deduction Fix)</h2>
 *
 * <ul>
 *   <li>appId in Sink at constructiongenerate(UUID), stable after serialization,
 *       jobrestartafterchanges
 *   <li>samejob same checkpoint retry → same appId + checkpointId + attemptId → sameidempotentkey →
 *       idempotent return
 *   <li>different checkpoint → different checkpointId → differentidempotentkey → no conflict
 *   <li>jobrestart → different appId → differentidempotentkey → new commit(correct behavior)
 * </ul>
 */
@Slf4j
public class RawWiseFlinkSink
        implements TwoPhaseCommittingSink<RowData, RawWiseCommittable>, Serializable {

    private static final long serialVersionUID = 1L;

    private final FlinkRawWiseConfig config;
    private final RowType rowType;
    private final int[] primaryKeyIndices;
    private final SnapshotOperationsSupplier snapshotOpsSupplier;
    private final CommitLedgerStoreSupplier commitLedgerStoreSupplier;

    private final String appId;
    private final WriterIdentity writerIdentity;

    public RawWiseFlinkSink(
            FlinkRawWiseConfig config,
            RowType rowType,
            int[] primaryKeyIndices,
            SnapshotOperationsSupplier snapshotOpsSupplier) {
        this(config, rowType, primaryKeyIndices, snapshotOpsSupplier, null);
    }

    public RawWiseFlinkSink(
            FlinkRawWiseConfig config,
            RowType rowType,
            int[] primaryKeyIndices,
            SnapshotOperationsSupplier snapshotOpsSupplier,
            CommitLedgerStoreSupplier commitLedgerStoreSupplier) {
        this.config = config;
        this.rowType = rowType;
        this.primaryKeyIndices = primaryKeyIndices;
        this.snapshotOpsSupplier = snapshotOpsSupplier;
        this.commitLedgerStoreSupplier = commitLedgerStoreSupplier;

        this.appId = UUID.randomUUID().toString().replace("-", "");
        this.writerIdentity = WriterIdentity.flink(this.appId, config.tableId(), 0L, 0L);
    }

    @Override
    public PrecommittingSinkWriter<RowData, RawWiseCommittable> createWriter(InitContext context)
            throws IOException {
        IngestionDataSink sink = createIngestionDataSink();
        int subTaskIndex = context.getSubtaskId();
        long baseOffset = System.nanoTime() & 0x7FFFFFFFFFFFFFFFL;
        AtomicLong seqGenerator = new AtomicLong(baseOffset + subTaskIndex * 1_000_000L + 1);
        return new RawWiseFlinkWriter(
                config.tableId(),
                rowType,
                primaryKeyIndices,
                sink,
                config.maxBatchRows(),
                config.maxBatchBytes(),
                seqGenerator,
                config.autoFlush(),
                subTaskIndex);
    }

    @Override
    public Committer<RawWiseCommittable> createCommitter() throws IOException {
        IngestionDataSink sink = createIngestionDataSink();

        CommitLedgerStore ledgerStore = null;
        if (commitLedgerStoreSupplier != null) {
            try {
                ledgerStore = commitLedgerStoreSupplier.get();
                log.info(
                        "Flink CommitLedgerStore enabled: appId={} tableId={}",
                        appId,
                        config.tableId());
            } catch (Exception e) {
                log.warn(
                        "Failed to create CommitLedgerStore, falling back to direct commit mode: {}",
                        e.getMessage());
            }
        }

        if (ledgerStore != null) {
            return new RawWiseFlinkCommitter(sink, writerIdentity, ledgerStore);
        }
        return new RawWiseFlinkCommitter(sink);
    }

    @Override
    public SimpleVersionedSerializer<RawWiseCommittable> getCommittableSerializer() {
        return new RawWiseCommittableSerializer();
    }

    private IngestionDataSink createIngestionDataSink() throws IOException {
        String sinkType = config.rawwiseConfig().get(RawWiseOptions.INGESTION_SINK_TYPE);
        if ("LMDB".equalsIgnoreCase(sinkType)) {
            throw new UnsupportedOperationException(
                    "LMDB sink path requires Phase 2 (ExecutorIdProvider + LMDB lease). "
                            + "Use PARQUET for Phase 1.");
        }
        SnapshotOperations snapshotOps = snapshotOpsSupplier.get();
        return IngestionDataSinkFactory.createParquetSink(config.rawwiseConfig(), snapshotOps);
    }

    /**
     * CommitLedgerStore supplier — serializable functional interface.
     *
     * <p>used forin TaskManager onlazily create {@link CommitLedgerStore} instance, avoid Metastore
     * connection receivesetc.non-serializable resourcesin Sink serializationwhencaptured.
     *
     * <p>typical implementation: via {@link io.github.rawwise.metastore.MetastoreRegistry} create
     * Metastore, then wrap as {@link
     * io.github.rawwise.engine.txn.ingest.MetastoreCommitLedgerStore}.
     */
    @FunctionalInterface
    public interface CommitLedgerStoreSupplier extends Serializable {
        CommitLedgerStore get() throws IOException;
    }
}
