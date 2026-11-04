package io.github.rawwise.engine.txn.state;

import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.OperationSet;
import io.github.rawwise.engine.txn.TxnMode;
import io.github.rawwise.engine.txn.TxnReadCache;
import io.github.rawwise.engine.txn.TxnTimeoutProfile;
import io.github.rawwise.engine.txn.WriteBuffer;
import io.github.rawwise.engine.txn.isolate.ReadSnapshot;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Setter;

/**
 * unifiedtransaction context — mergelegacy {@code engine.txn.TxnContext} andnewversion {@code
 * state.TxnContext}.
 *
 * <p><b>commitTs ≡ txnId unified strategy</b>: {@link #txnId}
 * whileasastransactionidentifierandcommit timestamp, in {@code begin()} whenvia {@code
 * clock.next()} pre-allocate (INV-003, INV-009).
 *
 * <h3>Serialization Note</h3>
 *
 * <p>ReadSnapshot, WriteBuffer, ReadCache, OperationSet etccanchangefieldas transient,
 * notseeandserialization.Spark Executor deserializationafterautorebuildemptybuffer. REPEATABLE_READ
 * consistency within a single Executor task is guaranteed by TxnReadCache + OCC validation at
 * commit time.
 */
public class TxnContext implements Serializable {

    @Serial private static final long serialVersionUID = 3L;

    // ==================== immutable part ====================

    private final long txnId;
    private final long startTs;
    private final long wallClockBeginMs;
    private final IsolationLevel isolationLevel;
    private final TxnTimeoutProfile timeoutProfile;
    private final Set<String> tableIds;
    private final int shardId;
    private final long baseSnapshotId;
    private final long routeTableVersion;
    private final String taskAttemptId;
    private final OperationSet operationSet;

    // ==================== canvariable part ====================

    @Setter private volatile long schemaVersion;

    @Setter private transient ReadSnapshot readSnapshot;

    private transient AtomicReference<TxnState> stateRef;

    private transient WriteBuffer writeBuffer;

    private transient TxnReadCache readCache;

    private volatile long commitTs;

    private TxnMode txnMode = TxnMode.DEFERRED;

    private transient Set<String> pendingInstanceIds;
    private transient List<byte[]> pendingBatchKeys;
    private transient List<byte[]> pendingBatchValues;
    private transient long pendingBatchBytes;

    // ==================== constructfunctioncount ====================

    public TxnContext(
            long txnId, long startTs, IsolationLevel isolationLevel, String tableId, int shardId) {
        this(
                txnId,
                startTs,
                isolationLevel,
                Collections.singleton(tableId),
                shardId,
                0L,
                0L,
                null,
                TxnTimeoutProfile.DEFAULT);
    }

    public TxnContext(
            long txnId,
            long startTs,
            IsolationLevel isolationLevel,
            String tableId,
            int shardId,
            long baseSnapshotId,
            long routeTableVersion,
            String taskAttemptId) {
        this(
                txnId,
                startTs,
                isolationLevel,
                Collections.singleton(tableId),
                shardId,
                baseSnapshotId,
                routeTableVersion,
                taskAttemptId,
                TxnTimeoutProfile.DEFAULT);
    }

    public TxnContext(
            long txnId,
            long startTs,
            IsolationLevel isolationLevel,
            Set<String> tableIds,
            int shardId,
            long baseSnapshotId,
            long routeTableVersion,
            String taskAttemptId,
            TxnTimeoutProfile timeoutProfile) {
        if (txnId <= 0) {
            throw new IllegalArgumentException("txnId must be positive, got: " + txnId);
        }
        if (isolationLevel == null) {
            throw new IllegalArgumentException("Isolation level must not be null");
        }
        if (tableIds == null || tableIds.isEmpty()) {
            throw new IllegalArgumentException("Table IDs must not be null or empty");
        }
        Set<String> filtered = new LinkedHashSet<>();
        for (String tid : tableIds) {
            if (tid != null && !tid.isBlank()) {
                filtered.add(tid);
            }
        }
        if (filtered.isEmpty()) {
            throw new IllegalArgumentException("Table IDs must not be null or empty (all blank)");
        }
        if (timeoutProfile == null) {
            throw new IllegalArgumentException("Timeout profile must not be null");
        }
        this.txnId = txnId;
        this.startTs = startTs;
        this.wallClockBeginMs = System.currentTimeMillis();
        this.isolationLevel = isolationLevel;
        this.timeoutProfile = timeoutProfile;
        this.tableIds = Collections.unmodifiableSet(filtered);
        this.shardId = shardId;
        this.baseSnapshotId = baseSnapshotId;
        this.routeTableVersion = routeTableVersion;
        this.taskAttemptId = taskAttemptId;
        this.operationSet = new OperationSet();
        this.stateRef = new AtomicReference<>(TxnState.BEGINNING);
        this.writeBuffer = new WriteBuffer();
        this.readSnapshot =
                (isolationLevel == IsolationLevel.READ_COMMITTED)
                        ? null
                        : new ReadSnapshot(startTs, baseSnapshotId);
        this.readCache =
                (isolationLevel == IsolationLevel.READ_COMMITTED) ? null : new TxnReadCache();
        this.commitTs = 0L;
        this.txnMode = TxnMode.DEFERRED;
        this.pendingInstanceIds = ConcurrentHashMap.newKeySet();
        this.pendingBatchKeys = new ArrayList<>();
        this.pendingBatchValues = new ArrayList<>();
        this.pendingBatchBytes = 0L;
    }

    /**
     * Orchestrator useconstructfunctioncount(6 parameter, without state). autoinitialize state as
     * BEGINNING.
     */
    public TxnContext(
            long txnId,
            ReadSnapshot readSnapshot,
            IsolationLevel isolationLevel,
            Set<String> tableIds,
            WriteBuffer writeBuffer,
            TxnReadCache readCache) {
        this(txnId, txnId, isolationLevel, tableIds, 0, 0L, 0L, null, TxnTimeoutProfile.DEFAULT);
        this.readSnapshot = readSnapshot;
        this.writeBuffer = writeBuffer != null ? writeBuffer : new WriteBuffer();
        this.readCache = readCache;
    }

    /** completeconstructfunctioncount(including state). */
    public TxnContext(
            long txnId,
            ReadSnapshot readSnapshot,
            IsolationLevel isolationLevel,
            Set<String> tableIds,
            WriteBuffer writeBuffer,
            TxnReadCache readCache,
            AtomicReference<TxnState> state) {
        this(txnId, readSnapshot, isolationLevel, tableIds, writeBuffer, readCache);
        this.stateRef = state != null ? state : new AtomicReference<>(TxnState.BEGINNING);
    }

    // ==================== serialization ====================

    @Serial
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeObject(stateRef.get());
    }

    @Serial
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        TxnState savedState = (TxnState) in.readObject();
        stateRef = new AtomicReference<>(savedState);
        writeBuffer = new WriteBuffer();
        readCache = (isolationLevel == IsolationLevel.READ_COMMITTED) ? null : new TxnReadCache();
        readSnapshot = null;
        pendingInstanceIds = ConcurrentHashMap.newKeySet();
        pendingBatchKeys = new ArrayList<>();
        pendingBatchValues = new ArrayList<>();
        pendingBatchBytes = 0L;
        if (txnMode == null) {
            txnMode = TxnMode.DEFERRED;
        }
    }

    // ==================== Getter ====================

    public long txnId() {
        return txnId;
    }

    public long startTs() {
        return startTs;
    }

    public long wallClockBeginMs() {
        return wallClockBeginMs;
    }

    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }

    public TxnTimeoutProfile timeoutProfile() {
        return timeoutProfile;
    }

    public String tableId() {
        Set<String> ids = tableIds();
        if (ids.isEmpty()) {
            return null;
        }
        return ids.iterator().next();
    }

    public Set<String> tableIds() {
        return tableIds != null ? tableIds : Collections.emptySet();
    }

    public String primaryTableId() {
        return tableIds.iterator().next();
    }

    public int shardId() {
        return shardId;
    }

    public OperationSet operationSet() {
        return operationSet;
    }

    public WriteBuffer writeBuffer() {
        return writeBuffer;
    }

    public ReadSnapshot readSnapshot() {
        return readSnapshot;
    }

    public TxnReadCache readCache() {
        return readCache;
    }

    public long baseSnapshotId() {
        return baseSnapshotId;
    }

    public long routeTableVersion() {
        return routeTableVersion;
    }

    public long schemaVersion() {
        return schemaVersion;
    }

    public String taskAttemptId() {
        return taskAttemptId;
    }

    /**
     * commitTs ≡ txnId (unified strategy). ifvia {@link #setCommitTs} explicitsetthrough,
     * returnsetvalue.
     */
    public long commitTs() {
        return commitTs > 0 ? commitTs : txnId;
    }

    public void setCommitTs(long ts) {
        this.commitTs = ts;
    }

    public TxnMode txnMode() {
        return txnMode;
    }

    public void setTxnMode(TxnMode mode) {
        this.txnMode = mode;
    }

    public Set<String> pendingInstanceIds() {
        return pendingInstanceIds;
    }

    public void addPendingInstanceId(String instanceId) {
        pendingInstanceIds.add(instanceId);
    }

    public List<byte[]> pendingBatchKeys() {
        return pendingBatchKeys;
    }

    public List<byte[]> pendingBatchValues() {
        return pendingBatchValues;
    }

    public void addPendingWrite(byte[] key, byte[] value) {
        pendingBatchKeys.add(key);
        pendingBatchValues.add(value);
        pendingBatchBytes += key.length + value.length;
    }

    public void clearPendingBatch() {
        pendingBatchKeys = new ArrayList<>();
        pendingBatchValues = new ArrayList<>();
        pendingBatchBytes = 0L;
    }

    public boolean isPendingBatchFull(int maxBatchSize, long maxBatchBytes) {
        return pendingBatchKeys.size() >= maxBatchSize || pendingBatchBytes >= maxBatchBytes;
    }

    /** use tracker commitTs valueupdate readSnapshot timestamp boundary. */
    public void setReadSnapshotTs(long snapshotTs) {
        if (readSnapshot != null) {
            this.readSnapshot = new ReadSnapshot(snapshotTs, readSnapshot.baseSnapshotId());
        }
    }

    // ==================== state machine ====================

    /** returncurrentlytransactionstate. */
    public TxnState currentState() {
        return stateRef.get();
    }

    /** compatibilitylegacy {@code status()} call, returncurrently TxnState. */
    public TxnState status() {
        return stateRef.get();
    }

    /** state machinetransition.atomicgroundfromcurrentlystateswitchtotargetstate. */
    public TxnState transitionTo(TxnState newState) {
        TxnState current;
        do {
            current = stateRef.get();
            if (!current.canTransitionTo(newState)) {
                throw new IllegalStateException(
                        "Invalid state transition: "
                                + current
                                + " → "
                                + newState
                                + " for txn="
                                + txnId);
            }
        } while (!stateRef.compareAndSet(current, newState));
        return current;
    }

    public void markPreparing() {
        TxnState current = stateRef.get();
        if (current == TxnState.BEGINNING) {
            transitionTo(TxnState.ACTIVE);
            current = TxnState.ACTIVE;
        }
        if (current == TxnState.ACTIVE) {
            transitionTo(TxnState.PREPARING);
        } else {
            throw new IllegalStateException(
                    "Cannot transition to PREPARING from " + current + " for txn=" + txnId);
        }
    }

    public void markPrepared() {
        TxnState current = stateRef.get();
        if (current == TxnState.PREPARING) {
            transitionTo(TxnState.DATA_DURABLE);
        } else {
            throw new IllegalStateException(
                    "Cannot transition to PREPARED from " + current + " for txn=" + txnId);
        }
    }

    public void markCommitting() {
        TxnState current = stateRef.get();
        if (current == TxnState.DATA_DURABLE) {
            transitionTo(TxnState.MANIFEST_COMMITTED);
            transitionTo(TxnState.PUBLISHED);
        } else if (current == TxnState.MANIFEST_COMMITTED) {
            transitionTo(TxnState.PUBLISHED);
        } else if (current == TxnState.PUBLISHED) {
            // already PUBLISHED
        } else {
            throw new IllegalStateException(
                    "Cannot transition to COMMITTING from " + current + " for txn=" + txnId);
        }
    }

    public void markCommitted() {
        TxnState current = stateRef.get();
        if (current == TxnState.PUBLISHED) {
            transitionTo(TxnState.COMMITTED);
        } else if (current == TxnState.MANIFEST_COMMITTED) {
            transitionTo(TxnState.PUBLISHED);
            transitionTo(TxnState.COMMITTED);
        } else if (current == TxnState.COMMITTED || current == TxnState.ACKED) {
            // idempotent
        } else {
            throw new IllegalStateException(
                    "Cannot commit transaction " + txnId + " in state " + current);
        }
    }

    /**
     * Direct transition from ACTIVE or DATA_DURABLE to COMMITTED, skipping intermediate states.
     * Used by mini-batch ingestion and simplified commit paths.
     */
    public void markCommittedDirect() {
        TxnState current = stateRef.get();
        if (current == TxnState.ACTIVE || current == TxnState.DATA_DURABLE) {
            transitionTo(TxnState.COMMITTED);
        } else {
            throw new IllegalStateException(
                    "Cannot directly commit transaction "
                            + txnId
                            + " in state "
                            + current
                            + " (expected ACTIVE or DATA_DURABLE)");
        }
    }

    public void markAborting() {
        transitionTo(TxnState.ABORTING);
    }

    public void markAborted() {
        if (!stateRef.compareAndSet(TxnState.ABORTING, TxnState.ABORTED)) {
            throw new IllegalStateException(
                    "Cannot abort transaction " + txnId + " in state " + stateRef.get());
        }
    }

    // ==================== statequery ====================

    public boolean isActive() {
        TxnState s = stateRef.get();
        return s == TxnState.ACTIVE || s == TxnState.BEGINNING;
    }

    public boolean isCommitted() {
        TxnState s = stateRef.get();
        return s == TxnState.COMMITTED || s == TxnState.ACKED;
    }

    public boolean isAborted() {
        return stateRef.get() == TxnState.ABORTED;
    }

    public boolean isPrepared() {
        return stateRef.get() == TxnState.DATA_DURABLE;
    }

    public boolean canRollback() {
        return stateRef.get().canRollback();
    }

    public boolean isReadable() {
        return stateRef.get().isReadable();
    }

    // ==================== Utility methods ====================

    public String identity() {
        return "txn:" + txnId + ":" + isolationLevel.name();
    }

    @Override
    public String toString() {
        return "TxnContext{txnId="
                + txnId
                + ", startTs="
                + startTs
                + ", isolationLevel="
                + isolationLevel
                + ", timeoutProfile="
                + timeoutProfile.name()
                + ", txnMode="
                + txnMode
                + ", wallClockBeginMs="
                + java.time.Instant.ofEpochMilli(wallClockBeginMs)
                + ", tableIds="
                + tableIds()
                + ", shardId="
                + shardId
                + ", state="
                + stateRef.get()
                + ", operationSetSize="
                + operationSet.size()
                + ", baseSnapshotId="
                + baseSnapshotId
                + ", routeTableVersion="
                + routeTableVersion
                + ", taskAttemptId='"
                + taskAttemptId
                + "'"
                + ", readSnapshot="
                + readSnapshot
                + "}";
    }
}
