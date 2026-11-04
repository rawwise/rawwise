package io.github.rawwise.backend.server.txn;

import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.routing.ShardLookupResult;
import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.ingest.BatchState;
import io.github.rawwise.engine.txn.ingest.CommitLedger;
import io.github.rawwise.engine.txn.ingest.CommitLedgerStore;
import io.github.rawwise.engine.txn.ingest.GlobalCommitMarker;
import io.github.rawwise.engine.txn.ingest.GlobalCommitMarkerStore;
import io.github.rawwise.engine.txn.ingest.LeaseInfo;
import io.github.rawwise.engine.txn.ingest.MarkerState;
import io.github.rawwise.engine.txn.ingest.ShardRef;
import io.github.rawwise.engine.txn.ingest.ShardStatus;
import io.github.rawwise.engine.txn.ingest.WriterInfo;
import io.github.rawwise.engine.txn.state.TxnResult;
import io.github.rawwise.txn.AbortBatchRequest;
import io.github.rawwise.txn.AbortTxnRequest;
import io.github.rawwise.txn.BeginBatchRequest;
import io.github.rawwise.txn.BeginBatchResponse;
import io.github.rawwise.txn.BeginTxnRequest;
import io.github.rawwise.txn.BeginTxnResponse;
import io.github.rawwise.txn.CommitBatchRequest;
import io.github.rawwise.txn.CommitBatchResponse;
import io.github.rawwise.txn.CommitTxnRequest;
import io.github.rawwise.txn.CommitTxnResponse;
import io.github.rawwise.txn.QueryBatchStatusRequest;
import io.github.rawwise.txn.QueryBatchStatusResponse;
import io.github.rawwise.txn.TxnCoordinatorServiceGrpc;
import io.grpc.ManagedChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class DefaultServerTxnCoordinator implements ServerTxnCoordinator {

    private static final String DEFAULT_CATALOG = "rawwise";
    private static final String DEFAULT_DATABASE = "default";
    private static final long LEASE_DURATION_MS = 300_000L;

    private final Map<String, TxnCoordinatorServiceGrpc.TxnCoordinatorServiceBlockingStub>
            executorStubs;
    private final RoutingCoordinator routingCoordinator;
    private final int grpcTimeoutSeconds;
    private final CommitLedgerStore commitLedgerStore;
    private final GlobalCommitMarkerStore globalCommitMarkerStore;

    private final ConcurrentHashMap<Long, TxnHandle> activeTxns = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BatchHandle> activeBatches = new ConcurrentHashMap<>();

    public DefaultServerTxnCoordinator(
            Map<String, ManagedChannel> executorChannels,
            RoutingCoordinator routingCoordinator,
            int grpcTimeoutSeconds) {
        this(executorChannels, routingCoordinator, grpcTimeoutSeconds, null, null);
    }

    public DefaultServerTxnCoordinator(
            Map<String, ManagedChannel> executorChannels,
            RoutingCoordinator routingCoordinator,
            int grpcTimeoutSeconds,
            CommitLedgerStore commitLedgerStore,
            GlobalCommitMarkerStore globalCommitMarkerStore) {
        this.routingCoordinator = Objects.requireNonNull(routingCoordinator, "routingCoordinator");
        this.grpcTimeoutSeconds = grpcTimeoutSeconds;
        this.commitLedgerStore = commitLedgerStore;
        this.globalCommitMarkerStore = globalCommitMarkerStore;
        this.executorStubs = new HashMap<>();
        for (var entry : executorChannels.entrySet()) {
            this.executorStubs.put(
                    entry.getKey(), TxnCoordinatorServiceGrpc.newBlockingStub(entry.getValue()));
        }
    }

    @Override
    public TxnHandle beginTxn(Set<String> tableIds, IsolationLevel level) {
        log.info("beginTxn: tables={} level={}", tableIds, level);

        Map<String, String> nodeAssignments = resolveNodeAssignments(tableIds);

        long txnId;
        Map<String, String> responseAssignments;
        String targetNode = pickTargetNode(nodeAssignments);

        if (targetNode != null && executorStubs.containsKey(targetNode)) {
            BeginTxnRequest request =
                    BeginTxnRequest.newBuilder()
                            .addAllTableIds(tableIds)
                            .setIsolationLevel(level.name())
                            .build();
            TxnCoordinatorServiceGrpc.TxnCoordinatorServiceBlockingStub stub =
                    executorStubs.get(targetNode);
            BeginTxnResponse response =
                    stub.withDeadlineAfter(grpcTimeoutSeconds, TimeUnit.SECONDS).beginTxn(request);
            txnId = response.getTxnId();
            responseAssignments = response.getNodeAssignmentsMap();
        } else {
            txnId = System.nanoTime();
            responseAssignments = Map.of();
            log.info("beginTxn: no executor stubs available, using local txnId={}", txnId);
        }

        if (txnId <= 0) {
            throw new RuntimeException("Failed to begin transaction: no valid txnId received");
        }

        Map<String, String> mergedAssignments = new HashMap<>(nodeAssignments);
        mergedAssignments.putAll(responseAssignments);

        TxnHandle handle =
                new TxnHandle(
                        txnId, level, tableIds, mergedAssignments, System.currentTimeMillis());
        activeTxns.put(txnId, handle);

        log.info("beginTxn: txnId={} nodeAssignments={}", txnId, mergedAssignments);
        return handle;
    }

    @Override
    public TxnResult commitTxn(long txnId) {
        TxnHandle handle = activeTxns.get(txnId);
        if (handle == null) {
            log.warn("commitTxn: unknown txnId={}", txnId);
            return new TxnResult.CommitConflict("Unknown txnId: " + txnId);
        }

        log.info("commitTxn: txnId={}", txnId);

        String targetNode = pickTargetNode(handle.nodeAssignments());
        if (targetNode == null && executorStubs.isEmpty()) {
            activeTxns.remove(txnId);
            long commitTs = System.nanoTime();
            log.info("commitTxn: txnId={} local commit ts={}", txnId, commitTs);
            return new TxnResult.CommitOk(commitTs);
        }
        if (targetNode == null) {
            activeTxns.remove(txnId);
            return new TxnResult.CommitConflict("No target node for txnId: " + txnId);
        }

        TxnCoordinatorServiceGrpc.TxnCoordinatorServiceBlockingStub stub =
                executorStubs.get(targetNode);
        if (stub == null) {
            activeTxns.remove(txnId);
            return new TxnResult.CommitConflict("No gRPC stub for node: " + targetNode);
        }

        CommitTxnRequest request = CommitTxnRequest.newBuilder().setTxnId(txnId).build();

        try {
            CommitTxnResponse response =
                    stub.withDeadlineAfter(grpcTimeoutSeconds, TimeUnit.SECONDS).commitTxn(request);

            if (response.getSuccess()) {
                log.info("commitTxn: txnId={} committed at ts={}", txnId, response.getCommitTs());
                return new TxnResult.CommitOk(response.getCommitTs());
            } else {
                log.warn("commitTxn: txnId={} conflict: {}", txnId, response.getConflictReason());
                return new TxnResult.CommitConflict(response.getConflictReason());
            }
        } catch (Exception e) {
            log.error("commitTxn: txnId={} gRPC error: {}", txnId, e.getMessage());
            return new TxnResult.CommitConflict("gRPC error: " + e.getMessage());
        } finally {
            activeTxns.remove(txnId);
        }
    }

    @Override
    public TxnResult abortTxn(long txnId) {
        TxnHandle handle = activeTxns.get(txnId);
        if (handle == null) {
            log.warn("abortTxn: unknown txnId={}", txnId);
            return new TxnResult.AbortResult(txnId, "Unknown txnId");
        }

        log.info("abortTxn: txnId={}", txnId);

        String targetNode = pickTargetNode(handle.nodeAssignments());
        if (targetNode != null) {
            TxnCoordinatorServiceGrpc.TxnCoordinatorServiceBlockingStub stub =
                    executorStubs.get(targetNode);
            if (stub != null) {
                AbortTxnRequest request = AbortTxnRequest.newBuilder().setTxnId(txnId).build();
                try {
                    stub.withDeadlineAfter(grpcTimeoutSeconds, TimeUnit.SECONDS).abortTxn(request);
                } catch (Exception e) {
                    log.warn("abortTxn: gRPC call failed for txnId={}: {}", txnId, e.getMessage());
                }
            }
        }

        activeTxns.remove(txnId);
        return new TxnResult.AbortResult(txnId, "user-requested abort");
    }

    @Override
    public BatchHandle beginBatch(String tableId, String batchId, int expectedShardCount) {
        log.info(
                "beginBatch: tableId={} batchId={} expectedShardCount={}",
                tableId,
                batchId,
                expectedShardCount);

        Map<Integer, String> shardAssignments =
                resolveShardAssignments(tableId, expectedShardCount);

        BeginBatchRequest request =
                BeginBatchRequest.newBuilder()
                        .setBatchId(batchId)
                        .setTableId(tableId)
                        .setExpectedShardCount(expectedShardCount)
                        .build();

        Map<Integer, String> responseAssignments = new HashMap<>();
        for (var entry : shardAssignments.entrySet()) {
            String nodeId = entry.getValue();
            TxnCoordinatorServiceGrpc.TxnCoordinatorServiceBlockingStub stub =
                    executorStubs.get(nodeId);
            if (stub != null) {
                try {
                    BeginBatchResponse response =
                            stub.withDeadlineAfter(grpcTimeoutSeconds, TimeUnit.SECONDS)
                                    .beginBatch(request);
                    responseAssignments.putAll(response.getShardAssignmentsMap());
                } catch (Exception e) {
                    log.warn("beginBatch: gRPC call to node={} failed: {}", nodeId, e.getMessage());
                }
            }
        }

        Map<Integer, String> mergedAssignments = new HashMap<>(shardAssignments);
        mergedAssignments.putAll(responseAssignments);

        BatchHandle handle =
                new BatchHandle(
                        batchId,
                        tableId,
                        expectedShardCount,
                        mergedAssignments,
                        System.currentTimeMillis());
        activeBatches.put(batchId, handle);

        log.info("beginBatch: batchId={} shardAssignments={}", batchId, mergedAssignments);
        return handle;
    }

    @Override
    public TxnResult commitBatch(String batchId) {
        BatchHandle handle = activeBatches.get(batchId);
        if (handle == null) {
            log.warn("commitBatch: unknown batchId={}", batchId);
            return new TxnResult.CommitConflict("Unknown batchId: " + batchId);
        }

        log.info(
                "commitBatch: batchId={} tableId={} expectedShardCount={}",
                batchId,
                handle.tableId(),
                handle.expectedShardCount());

        String idempotencyKey = batchId;
        boolean storesConfigured = commitLedgerStore != null && globalCommitMarkerStore != null;

        if (storesConfigured) {
            TxnResult earlyResult = createInitialLedgerOrFail(handle, idempotencyKey);
            if (earlyResult != null) {
                activeBatches.remove(batchId);
                return earlyResult;
            }
            try {
                commitLedgerStore.updateState(idempotencyKey, BatchState.SHARDS_PREPARED);
            } catch (Exception e) {
                log.error(
                        "commitBatch: batchId={} failed to transition to SHARDS_PREPARED: {}",
                        batchId,
                        e.getMessage(),
                        e);
                activeBatches.remove(batchId);
                return new TxnResult.CommitConflict(
                        "Ledger state transition failed: " + e.getMessage());
            }
        }

        boolean allSuccess = true;
        String conflictReason = "";
        long commitTs = 0;
        List<ShardRef> shardRefs = new ArrayList<>();

        for (var entry : handle.shardAssignments().entrySet()) {
            int shardId = entry.getKey();
            String nodeId = entry.getValue();

            TxnCoordinatorServiceGrpc.TxnCoordinatorServiceBlockingStub stub =
                    executorStubs.get(nodeId);
            if (stub == null) {
                log.warn("commitBatch: no stub for nodeId={}", nodeId);
                allSuccess = false;
                conflictReason = "No gRPC stub for node: " + nodeId;
                continue;
            }

            CommitBatchRequest request =
                    CommitBatchRequest.newBuilder()
                            .setBatchId(batchId)
                            .setTableId(handle.tableId())
                            .setIdempotencyKey(idempotencyKey)
                            .setExpectedShardCount(handle.expectedShardCount())
                            .build();

            try {
                CommitBatchResponse response =
                        stub.withDeadlineAfter(grpcTimeoutSeconds, TimeUnit.SECONDS)
                                .commitBatch(request);
                if (!response.getSuccess()) {
                    allSuccess = false;
                    conflictReason = response.getConflictReason();
                    log.warn(
                            "commitBatch: shardId={} nodeId={} failed: {}",
                            shardId,
                            nodeId,
                            conflictReason);
                } else {
                    commitTs = Math.max(commitTs, response.getCommitTs());
                    shardRefs.add(buildShardRef(shardId, response.getCommitTs()));
                }
            } catch (Exception e) {
                log.error(
                        "commitBatch: shardId={} nodeId={} gRPC error: {}",
                        shardId,
                        nodeId,
                        e.getMessage());
                allSuccess = false;
                conflictReason = "gRPC error: " + e.getMessage();
            }
        }

        TxnResult result;
        if (allSuccess) {
            if (storesConfigured) {
                try {
                    commitLedgerStore.updateState(idempotencyKey, BatchState.DATA_DURABLE);
                } catch (Exception e) {
                    log.error(
                            "commitBatch: batchId={} failed to transition to DATA_DURABLE: {}",
                            batchId,
                            e.getMessage(),
                            e);
                    activeBatches.remove(batchId);
                    return new TxnResult.CommitConflict(
                            "Ledger data durable transition failed: " + e.getMessage());
                }
                result = createGlobalMarkerAndCommit(handle, idempotencyKey, commitTs, shardRefs);
            } else {
                log.info(
                        "commitBatch: batchId={} COMMITTED commitTs={} (ledger/marker stores not configured)",
                        handle.batchId(),
                        commitTs);
                result = new TxnResult.CommitOk(commitTs);
            }
        } else {
            if (storesConfigured) {
                markBatchAbortedIfLedgerExists(idempotencyKey, handle, conflictReason);
            }
            log.warn("commitBatch: batchId={} failed: {}", batchId, conflictReason);
            result = new TxnResult.CommitConflict(conflictReason);
        }

        activeBatches.remove(batchId);
        return result;
    }

    @Override
    public TxnResult abortBatch(String batchId) {
        BatchHandle handle = activeBatches.get(batchId);
        if (handle == null) {
            log.warn("abortBatch: unknown batchId={}", batchId);
            return new TxnResult.AbortResult(0, "Unknown batchId: " + batchId);
        }

        log.info("abortBatch: batchId={}", batchId);

        for (var entry : handle.shardAssignments().entrySet()) {
            String nodeId = entry.getValue();
            TxnCoordinatorServiceGrpc.TxnCoordinatorServiceBlockingStub stub =
                    executorStubs.get(nodeId);
            if (stub != null) {
                try {
                    AbortBatchRequest request =
                            AbortBatchRequest.newBuilder().setIdempotencyKey(batchId).build();
                    stub.withDeadlineAfter(grpcTimeoutSeconds, TimeUnit.SECONDS)
                            .abortBatch(request);
                } catch (Exception e) {
                    log.warn("abortBatch: nodeId={} gRPC error: {}", nodeId, e.getMessage());
                }
            }
        }

        activeBatches.remove(batchId);
        return new TxnResult.AbortResult(0, "batch aborted: " + batchId);
    }

    @Override
    public RecoveryDecision decideBatchRecovery(String idempotencyKey) {
        log.info("decideBatchRecovery: idempotencyKey={}", idempotencyKey);

        BatchHandle handle = activeBatches.get(idempotencyKey);
        if (handle != null) {
            return new RecoveryDecision(
                    RecoveryDecision.DecisionType.ACTIVE_BATCH, "active batch in memory", null);
        }

        RecoveryDecision ledgerDecision = decideRecoveryFromLedger(idempotencyKey);
        if (ledgerDecision != null) {
            return ledgerDecision;
        }

        for (var entry : executorStubs.entrySet()) {
            String nodeId = entry.getKey();
            TxnCoordinatorServiceGrpc.TxnCoordinatorServiceBlockingStub stub = entry.getValue();
            try {
                QueryBatchStatusRequest request =
                        QueryBatchStatusRequest.newBuilder()
                                .setIdempotencyKey(idempotencyKey)
                                .build();
                QueryBatchStatusResponse response =
                        stub.withDeadlineAfter(grpcTimeoutSeconds, TimeUnit.SECONDS)
                                .queryBatchStatus(request);
                String state = response.getState();
                if ("COMMITTED".equals(state)) {
                    return new RecoveryDecision(
                            RecoveryDecision.DecisionType.ALREADY_COMMITTED,
                            "node reports COMMITTED",
                            null);
                }
                if ("ABORTED".equals(state)) {
                    return new RecoveryDecision(
                            RecoveryDecision.DecisionType.ALREADY_ABORTED,
                            "node reports ABORTED",
                            null);
                }
                if ("needs-abort".equals(state)) {
                    return new RecoveryDecision(
                            RecoveryDecision.DecisionType.NEEDS_ABORT,
                            "node reports needs-abort",
                            null);
                }
                if (!"UNKNOWN".equals(state)) {
                    return new RecoveryDecision(
                            RecoveryDecision.DecisionType.NEEDS_RECOVERY,
                            "node state: " + state,
                            null);
                }
            } catch (Exception e) {
                log.warn("decideBatchRecovery: nodeId={} query failed: {}", nodeId, e.getMessage());
            }
        }

        return new RecoveryDecision(
                RecoveryDecision.DecisionType.NEEDS_ABORT, "no ledger, no node reports", null);
    }

    private Map<String, String> resolveNodeAssignments(Set<String> tableIds) {
        Map<String, String> assignments = new HashMap<>();
        for (String tableId : tableIds) {
            ShardId shardId = new ShardId(0);
            ShardLookupResult result = routingCoordinator.lookupOwner(shardId);
            if (result != null && result.assignment() != null) {
                assignments.put(tableId, result.assignment().executorId());
            }
        }
        return assignments;
    }

    private Map<Integer, String> resolveShardAssignments(String tableId, int shardCount) {
        Map<Integer, String> assignments = new HashMap<>();
        for (int i = 0; i < shardCount; i++) {
            ShardId shardId = new ShardId(i);
            ShardLookupResult result = routingCoordinator.lookupOwner(shardId);
            if (result != null && result.assignment() != null) {
                assignments.put(i, result.assignment().executorId());
            }
        }
        return assignments;
    }

    private String pickTargetNode(Map<String, String> nodeAssignments) {
        if (nodeAssignments == null || nodeAssignments.isEmpty()) {
            if (!executorStubs.isEmpty()) {
                return executorStubs.keySet().iterator().next();
            }
            return null;
        }
        return nodeAssignments.values().iterator().next();
    }

    // ===== CommitLedger + GlobalCommitMarker integration =====

    /**
     * Create CommitLedger (OPEN) and save to Metastore BEFORE shards commit.
     *
     * <p>This ensures that if the Server crashes after shards are committed but before
     * GlobalCommitMarker is created, the CommitLedger allows crash recovery to identify the
     * already-committed shards.
     *
     * @return null if ledger created successfully (continue with commit flow), or a TxnResult if an
     *     early return is needed (already committed, conflict, error)
     */
    private TxnResult createInitialLedgerOrFail(BatchHandle handle, String idempotencyKey) {
        try {
            LeaseInfo lease = createLeaseInfo();
            CommitLedger ledger =
                    CommitLedger.open(
                            idempotencyKey,
                            handle.batchId(),
                            handle.tableId(),
                            handle.expectedShardCount(),
                            0L,
                            lease);

            if (!commitLedgerStore.save(ledger)) {
                Optional<CommitLedger> existing = commitLedgerStore.load(idempotencyKey);
                if (existing.isPresent() && existing.get().state() == BatchState.COMMITTED) {
                    log.info(
                            "commitBatch: batchId={} already committed (ledger exists)",
                            handle.batchId());
                    return new TxnResult.AlreadyCommitted(existing.get().commitTs());
                }
                log.warn(
                        "commitBatch: batchId={} ledger already exists, state={}",
                        handle.batchId(),
                        existing.map(l -> l.state().toString()).orElse("unknown"));
                return new TxnResult.CommitConflict(
                        "Batch ledger already exists for idempotencyKey=" + idempotencyKey);
            }
            return null;
        } catch (Exception e) {
            log.error(
                    "commitBatch: batchId={} ledger creation failed: {}",
                    handle.batchId(),
                    e.getMessage(),
                    e);
            return new TxnResult.CommitConflict("Ledger creation failed: " + e.getMessage());
        }
    }

    /**
     * Create GlobalCommitMarker (CAS NotExists) and transition ledger to COMMITTED.
     *
     * <p>This method is called AFTER all shards have successfully committed. It:
     *
     * <ol>
     *   <li>Creates GlobalCommitMarker via CAS (precondition: NotExists)
     *   <li>Transitions CommitLedger to GLOBAL_MARKER_COMMITTED (point of no return)
     *   <li>Transitions CommitLedger to COMMITTED
     * </ol>
     */
    private TxnResult createGlobalMarkerAndCommit(
            BatchHandle handle, String idempotencyKey, long commitTs, List<ShardRef> shardRefs) {
        try {
            GlobalCommitMarker marker =
                    createGlobalCommitMarker(handle, idempotencyKey, commitTs, shardRefs);
            boolean markerCreated = globalCommitMarkerStore.create(marker);
            if (!markerCreated) {
                Optional<GlobalCommitMarker> existingMarker =
                        globalCommitMarkerStore.load(handle.batchId());
                if (existingMarker.isPresent() && existingMarker.get().isCommitted()) {
                    log.info(
                            "commitBatch: batchId={} marker already committed (CAS race)",
                            handle.batchId());
                } else {
                    log.warn(
                            "commitBatch: batchId={} marker CAS failed, continuing with ledger transition",
                            handle.batchId());
                }
            }

            CommitLedger ledger =
                    commitLedgerStore.updateState(
                            idempotencyKey, BatchState.GLOBAL_MARKER_COMMITTED);
            ledger = commitLedgerStore.updateState(idempotencyKey, BatchState.COMMITTED);

            log.info(
                    "commitBatch: batchId={} COMMITTED commitTs={} ledgerState={}",
                    handle.batchId(),
                    commitTs,
                    ledger.state());
            return new TxnResult.CommitOk(commitTs);
        } catch (Exception e) {
            log.error(
                    "commitBatch: batchId={} marker/commit finalization failed: {}",
                    handle.batchId(),
                    e.getMessage(),
                    e);
            return new TxnResult.CommitConflict("Batch finalization failed: " + e.getMessage());
        }
    }

    /** Mark batch as ABORTED in CommitLedger if it exists (failure path). */
    private void markBatchAbortedIfLedgerExists(
            String idempotencyKey, BatchHandle handle, String reason) {
        if (commitLedgerStore == null) {
            return;
        }
        try {
            Optional<CommitLedger> existing = commitLedgerStore.load(idempotencyKey);
            if (existing.isEmpty()) {
                return;
            }
            CommitLedger ledger = existing.get();
            if (ledger.state().isAbortable()) {
                commitLedgerStore.updateState(idempotencyKey, BatchState.ABORTED);
                log.info(
                        "commitBatch: batchId={} marked ABORTED in ledger (reason: {})",
                        handle.batchId(),
                        reason);
            } else if (ledger.state().isPastPointOfNoReturn()) {
                log.error(
                        "commitBatch: batchId={} cannot abort - past point of no return (state={})",
                        handle.batchId(),
                        ledger.state());
            }
        } catch (Exception e) {
            log.warn(
                    "commitBatch: batchId={} failed to mark ABORTED in ledger: {}",
                    handle.batchId(),
                    e.getMessage());
        }
    }

    /**
     * Decide recovery action from CommitLedger.
     *
     * @return recovery decision, or null if no ledger found
     */
    private RecoveryDecision decideRecoveryFromLedger(String idempotencyKey) {
        if (commitLedgerStore == null) {
            return null;
        }
        try {
            Optional<CommitLedger> optLedger = commitLedgerStore.load(idempotencyKey);
            if (optLedger.isEmpty()) {
                return null;
            }
            CommitLedger ledger = optLedger.get();
            BatchState state = ledger.state();
            log.info(
                    "decideBatchRecovery: idempotencyKey={} ledger state={}",
                    idempotencyKey,
                    state);

            return switch (state) {
                case OPEN, SHARDS_PREPARED ->
                        new RecoveryDecision(
                                RecoveryDecision.DecisionType.NEEDS_RECOVERY,
                                "abort: " + state,
                                state);
                case DATA_DURABLE, GLOBAL_MARKER_COMMITTED, PUBLISHING ->
                        new RecoveryDecision(
                                RecoveryDecision.DecisionType.NEEDS_RECOVERY,
                                "continue-commit: " + state,
                                state);
                case COMMITTED, ACKED ->
                        new RecoveryDecision(
                                RecoveryDecision.DecisionType.ALREADY_COMMITTED,
                                "ledger state: " + state,
                                state);
                case ABORTED ->
                        new RecoveryDecision(
                                RecoveryDecision.DecisionType.ALREADY_ABORTED,
                                "ledger state: " + state,
                                state);
                case FAILED_NEEDS_RECOVERY ->
                        new RecoveryDecision(
                                RecoveryDecision.DecisionType.NEEDS_RECOVERY,
                                "failed: " + state,
                                state);
            };
        } catch (Exception e) {
            log.warn(
                    "decideBatchRecovery: idempotencyKey={} ledger load failed: {}",
                    idempotencyKey,
                    e.getMessage());
            return null;
        }
    }

    private ShardRef buildShardRef(int shardId, long commitTs) {
        // dataRefs are currently empty; pending Node-side CommitBatchResponse to populate
        // dataRefs/rowCount/digest fields
        return new ShardRef(
                shardId,
                shardId,
                "",
                String.valueOf(commitTs),
                List.of(),
                0L,
                commitTs,
                commitTs,
                "",
                ShardStatus.PUBLISHED);
    }

    private GlobalCommitMarker createGlobalCommitMarker(
            BatchHandle handle, String idempotencyKey, long commitTs, List<ShardRef> shardRefs) {
        String[] parts = parseTableId(handle.tableId());
        long now = System.currentTimeMillis();
        return new GlobalCommitMarker(
                "v1",
                parts[0],
                parts[1],
                parts[2],
                handle.batchId(),
                commitTs,
                commitTs,
                0L,
                0,
                MarkerState.COMMITTED,
                now,
                now,
                new WriterInfo("server", "driver", handle.batchId()),
                shardRefs,
                "",
                idempotencyKey,
                0L);
    }

    private LeaseInfo createLeaseInfo() {
        long now = System.currentTimeMillis();
        return new LeaseInfo("server-driver", now + LEASE_DURATION_MS, 1L);
    }

    /**
     * Parse tableId into [catalog, database, table] components.
     *
     * <p>Supported formats:
     *
     * <ul>
     *   <li>"catalog.database.table" (3-segment) → ["catalog", "database", "table"]
     *   <li>"database.table" (2-segment) → [DEFAULT_CATALOG, "database", "table"]
     *   <li>"table" (1-segment) → [DEFAULT_CATALOG, DEFAULT_DATABASE, "table"]
     * </ul>
     */
    private String[] parseTableId(String tableId) {
        String[] parts = tableId.split("\\.");
        return switch (parts.length) {
            case 1 -> new String[] {DEFAULT_CATALOG, DEFAULT_DATABASE, parts[0]};
            case 2 -> new String[] {DEFAULT_CATALOG, parts[0], parts[1]};
            default -> new String[] {parts[0], parts[1], parts[2]};
        };
    }

    /**
     * Recover a batch transaction by idempotency key. Returns the recovery decision for the batch.
     */
    public io.github.rawwise.backend.server.txn.RecoveryDecision recoverBatch(
            String idempotencyKey) {
        log.info("recoverBatch: idempotencyKey={}", idempotencyKey);
        try {
            var status = queryBatchStatus(idempotencyKey);
            if (status != null) {
                String state = status.state();
                if ("COMMITTED".equals(state)) {
                    return new io.github.rawwise.backend.server.txn.RecoveryDecision(
                            io.github.rawwise.backend.server.txn.RecoveryDecision.DecisionType
                                    .ALREADY_COMMITTED,
                            "idempotencyKey=" + idempotencyKey + " commitTs=" + status.commitTs(),
                            io.github.rawwise.engine.txn.ingest.BatchState.COMMITTED);
                }
                if ("ABORTED".equals(state)) {
                    return new io.github.rawwise.backend.server.txn.RecoveryDecision(
                            io.github.rawwise.backend.server.txn.RecoveryDecision.DecisionType
                                    .ALREADY_ABORTED,
                            "idempotencyKey=" + idempotencyKey,
                            io.github.rawwise.engine.txn.ingest.BatchState.ABORTED);
                }
                if ("OPEN".equals(state) || "SHARDS_PREPARED".equals(state)) {
                    commitLedgerStore.updateState(
                            idempotencyKey, io.github.rawwise.engine.txn.ingest.BatchState.ABORTED);
                    log.info(
                            "recoverBatch: aborted incomplete ledger idempotencyKey={} state={}",
                            idempotencyKey,
                            state);
                    return new io.github.rawwise.backend.server.txn.RecoveryDecision(
                            io.github.rawwise.backend.server.txn.RecoveryDecision.DecisionType
                                    .ALREADY_ABORTED,
                            "idempotencyKey="
                                    + idempotencyKey
                                    + " aborted during recovery (was "
                                    + state
                                    + ")",
                            io.github.rawwise.engine.txn.ingest.BatchState.ABORTED);
                }
                if ("DATA_DURABLE".equals(state)
                        || "GLOBAL_MARKER_COMMITTED".equals(state)
                        || "PUBLISHING".equals(state)) {
                    commitLedgerStore.updateState(
                            idempotencyKey,
                            io.github.rawwise.engine.txn.ingest.BatchState.COMMITTED);
                    log.info(
                            "recoverBatch: committed durable ledger idempotencyKey={} state={}",
                            idempotencyKey,
                            state);
                    return new io.github.rawwise.backend.server.txn.RecoveryDecision(
                            io.github.rawwise.backend.server.txn.RecoveryDecision.DecisionType
                                    .ALREADY_COMMITTED,
                            "idempotencyKey="
                                    + idempotencyKey
                                    + " committed during recovery (was "
                                    + state
                                    + ")",
                            io.github.rawwise.engine.txn.ingest.BatchState.COMMITTED);
                }
            }
            // No state found - batch was never started or completed
            return new io.github.rawwise.backend.server.txn.RecoveryDecision(
                    io.github.rawwise.backend.server.txn.RecoveryDecision.DecisionType
                            .NEEDS_RECOVERY,
                    "idempotencyKey=" + idempotencyKey + " no state found",
                    null);
        } catch (Exception e) {
            log.error(
                    "recoverBatch failed for idempotencyKey={}: {}",
                    idempotencyKey,
                    e.getMessage(),
                    e);
            return new io.github.rawwise.backend.server.txn.RecoveryDecision(
                    io.github.rawwise.backend.server.txn.RecoveryDecision.DecisionType
                            .NEEDS_RECOVERY,
                    "idempotencyKey=" + idempotencyKey + " error: " + e.getMessage(),
                    null);
        }
    }

    public record BatchStatus(String state, long commitTs) {}

    public BatchStatus queryBatchStatus(String idempotencyKey) {
        if (commitLedgerStore == null) {
            return null;
        }
        try {
            Optional<CommitLedger> ledger = commitLedgerStore.load(idempotencyKey);
            if (ledger.isPresent()) {
                CommitLedger l = ledger.get();
                return new BatchStatus(l.state().name(), l.commitTs());
            }
        } catch (Exception e) {
            log.warn("queryBatchStatus failed for {}: {}", idempotencyKey, e.getMessage());
        }
        return null;
    }
}
