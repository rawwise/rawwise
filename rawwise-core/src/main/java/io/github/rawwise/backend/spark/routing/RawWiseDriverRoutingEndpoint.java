package io.github.rawwise.backend.spark.routing;

import io.github.rawwise.backend.routing.InMemoryRoutingCoordinator;
import io.github.rawwise.backend.routing.RecoveryCompleteMessage;
import io.github.rawwise.backend.routing.RecoveryTaskMessage;
import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.RoutingStatus;
import io.github.rawwise.backend.routing.ShardAssignment;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.routing.ShardLookupResult;
import io.github.rawwise.governance.config.ConfigAbortMessage;
import io.github.rawwise.governance.config.ConfigCommitMessage;
import io.github.rawwise.governance.config.ConfigPropagationMessages;
import io.github.rawwise.governance.config.ConfigUpdateMessage;
import io.github.rawwise.governance.config.DynamicConfigRegistry;
import io.github.rawwise.governance.config.PropagationMode;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.rpc.*;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;
import scala.runtime.BoxedUnit;

@Slf4j
public final class RawWiseDriverRoutingEndpoint implements RpcEndpoint {
    public static final String ENDPOINT_NAME = "rawwise-driver-routing-endpoint";

    private final RpcEnv rpcEnv;
    private final RoutingCoordinator routingCoordinator;
    private final SparkConf sparkConf;
    private final RawWiseConfig rawwiseConfig;
    private final DynamicConfigRegistry configRegistry;
    private final Map<String, RpcEndpointRef> executorEndpoints = new ConcurrentHashMap<>();
    private final Map<String, RoutingMessages.ExecutorEndpointInfo> executorEndpointInfos =
            new ConcurrentHashMap<>();
    private final Map<ShardId, Integer> recoveryRetryCount = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ShardId, DispatchEntry> dispatchedRecoveryShards =
            new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread t = new Thread(r, "rawwise-recovery-scheduler");
                        t.setDaemon(true);
                        return t;
                    });

    private final ExecutorService propagationExecutor =
            new ThreadPoolExecutor(
                    8,
                    64,
                    60L,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(256),
                    r -> {
                        Thread t = new Thread(r, "rawwise-config-propagation");
                        t.setDaemon(true);
                        return t;
                    },
                    new ThreadPoolExecutor.CallerRunsPolicy());

    public RawWiseDriverRoutingEndpoint(RpcEnv rpcEnv, RoutingCoordinator routingCoordinator) {
        this(rpcEnv, routingCoordinator, SparkEnv.get().conf(), null, null);
    }

    public RawWiseDriverRoutingEndpoint(
            RpcEnv rpcEnv, RoutingCoordinator routingCoordinator, SparkConf sparkConf) {
        this(rpcEnv, routingCoordinator, sparkConf, null, null);
    }

    public RawWiseDriverRoutingEndpoint(
            RpcEnv rpcEnv,
            RoutingCoordinator routingCoordinator,
            SparkConf sparkConf,
            RawWiseConfig rawwiseConfig) {
        this(rpcEnv, routingCoordinator, sparkConf, rawwiseConfig, null);
    }

    RawWiseDriverRoutingEndpoint(
            RpcEnv rpcEnv,
            RoutingCoordinator routingCoordinator,
            SparkConf sparkConf,
            RawWiseConfig rawwiseConfig,
            DynamicConfigRegistry configRegistry) {
        this.rpcEnv = Objects.requireNonNull(rpcEnv, "rpcEnv must not be null");
        this.routingCoordinator =
                Objects.requireNonNull(routingCoordinator, "routingCoordinator must not be null");
        this.sparkConf = Objects.requireNonNull(sparkConf, "sparkConf must not be null");
        this.rawwiseConfig = rawwiseConfig;
        this.configRegistry = configRegistry;
        if (routingCoordinator instanceof InMemoryRoutingCoordinator coordinator) {
            coordinator.setRecoveryTaskDispatcher(this::submitRecoveryDispatch);
        }
        if (configRegistry != null) {
            configRegistry.setPropagationCallback(
                    new DynamicConfigRegistry.PropagationCallback() {
                        @Override
                        public DynamicConfigRegistry.PropagationResult propagate(
                                ConfigUpdateMessage message) {
                            return propagateConfig(message);
                        }

                        @Override
                        public void sendCommit(
                                ConfigUpdateMessage message, Set<String> targetExecutorIds) {
                            sendCommitToExecutors(message, targetExecutorIds);
                        }

                        @Override
                        public void sendAbort(
                                ConfigUpdateMessage message, Set<String> targetExecutorIds) {
                            sendAbortToExecutors(message, targetExecutorIds);
                        }
                    });
        }
    }

    @Override
    public RpcEnv rpcEnv() {
        return rpcEnv;
    }

    @Override
    public void onStart() {
        long timeoutMs = getRecoveryTimeoutMs();
        long checkIntervalMs = Math.max(timeoutMs / 2, 1000);
        scheduler.scheduleAtFixedRate(
                () -> {
                    try {
                        dispatchPendingRecoveryTasks();
                    } catch (Exception e) {
                        log.error("Periodic recovery dispatch failed", e);
                    }
                },
                checkIntervalMs,
                checkIntervalMs,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void onStop() {
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Recovery scheduler did not terminate in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        propagationExecutor.shutdownNow();
        try {
            if (!propagationExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                log.warn("Propagation executor did not terminate in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executorEndpoints.clear();
        executorEndpointInfos.clear();
        dispatchedRecoveryShards.clear();
    }

    @Override
    public PartialFunction<Object, BoxedUnit> receive() {
        return new AbstractPartialFunction<>() {
            @Override
            public boolean isDefinedAt(Object message) {
                return message instanceof RecoveryCompleteMessage
                        || message instanceof RoutingMessages.ShardAbortTxn
                        || message instanceof ConfigPropagationMessages.ConfigEpochHeartbeat;
            }

            @Override
            public BoxedUnit apply(Object message) {
                if (message instanceof RecoveryCompleteMessage complete) {
                    handleRecoveryComplete(complete);
                } else if (message instanceof RoutingMessages.ShardAbortTxn abort) {
                    handleAbortTxn(abort);
                } else if (message
                        instanceof ConfigPropagationMessages.ConfigEpochHeartbeat heartbeat) {
                    handleConfigEpochHeartbeat(heartbeat);
                }
                return BoxedUnit.UNIT;
            }
        };
    }

    @Override
    public PartialFunction<Object, BoxedUnit> receiveAndReply(RpcCallContext context) {
        return new AbstractPartialFunction<>() {
            @Override
            public boolean isDefinedAt(Object message) {
                return message instanceof RoutingMessages.ShardWriteRequest
                        || message instanceof RoutingMessages.MultiShardWriteRequest
                        || message instanceof RoutingMessages.RegisterExecutorEndpointMessage
                        || message instanceof RoutingMessages.LookupExecutorEndpointMessage
                        || message instanceof RoutingMessages.LookupShardOwnerMessage
                        || message instanceof RoutingMessages.LookupShardOwnersMessage
                        || message instanceof RoutingMessages.ShardPrepareTxn
                        || message instanceof RoutingMessages.ShardCommitTxn;
            }

            @Override
            public BoxedUnit apply(Object message) {
                if (message
                        instanceof
                        RoutingMessages.RegisterExecutorEndpointMessage(
                                String executorId,
                                String host,
                                int port,
                                String endpointName)) {
                    RpcAddress address = RpcAddress.apply(host, port);
                    RpcEndpointRef ref = rpcEnv.setupEndpointRef(address, endpointName);
                    executorEndpoints.put(executorId, ref);
                    executorEndpointInfos.put(
                            executorId,
                            new RoutingMessages.ExecutorEndpointInfo(
                                    executorId, host, port, endpointName));
                    context.reply(Boolean.TRUE);
                    submitRecoveryDispatch();
                    return BoxedUnit.UNIT;
                }
                if (message instanceof RoutingMessages.ShardWriteRequest request) {
                    context.reply(handleWrite(request));
                    return BoxedUnit.UNIT;
                }
                if (message instanceof RoutingMessages.MultiShardWriteRequest request) {
                    context.reply(handleMultiShardWrite(request));
                    return BoxedUnit.UNIT;
                }
                if (message instanceof RoutingMessages.LookupShardOwnerMessage(int shardId)) {
                    context.reply(routingCoordinator.lookupOwner(new ShardId(shardId)));
                    return BoxedUnit.UNIT;
                }
                if (message
                        instanceof
                        RoutingMessages.LookupExecutorEndpointMessage(String executorId)) {
                    context.reply(executorEndpointInfos.get(executorId));
                    return BoxedUnit.UNIT;
                }
                if (message
                        instanceof
                        RoutingMessages.LookupShardOwnersMessage(java.util.Set<Integer> shardIds)) {
                    context.reply(
                            routingCoordinator.lookupOwners(
                                    shardIds.stream()
                                            .map(ShardId::new)
                                            .collect(java.util.stream.Collectors.toSet())));
                } else if (message instanceof RoutingMessages.ShardPrepareTxn prepare) {
                    context.reply(handlePrepareTxn(prepare));
                } else if (message instanceof RoutingMessages.ShardCommitTxn commit) {
                    context.reply(handleCommitTxn(commit));
                }
                return BoxedUnit.UNIT;
            }
        };
    }

    private RoutingMessages.ShardPrepareTxnResult handlePrepareTxn(
            RoutingMessages.ShardPrepareTxn prepare) {
        ShardLookupResult lookup = routingCoordinator.lookupOwner(new ShardId(prepare.shardId()));
        if (lookup.status() != RoutingStatus.ASSIGNED) {
            return RoutingMessages.ShardPrepareTxnResult.error(
                    0L, "routing_status=" + lookup.status());
        }
        ShardAssignment assignment = lookup.assignment();
        if (assignment.epoch().value() != prepare.epoch()) {
            return RoutingMessages.ShardPrepareTxnResult.error(0L, "stale_epoch");
        }
        RpcEndpointRef target = executorEndpoints.get(assignment.executorId());
        if (target == null) {
            return RoutingMessages.ShardPrepareTxnResult.error(0L, "executor_endpoint_missing");
        }
        try {
            RpcTimeout timeout = askTimeout();
            Object reply =
                    target.askSync(
                            prepare, timeout, scala.reflect.ClassTag$.MODULE$.apply(Object.class));
            if (reply instanceof RoutingMessages.ShardPrepareTxnResult result) {
                return result;
            }
            return RoutingMessages.ShardPrepareTxnResult.error(0L, "invalid_reply");
        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            if (isHardRpcFailure(e)) {
                routingCoordinator.onExecutorDown(assignment.executorId());
                cleanupExecutorEndpoint(assignment.executorId());
            }
            return RoutingMessages.ShardPrepareTxnResult.error(0L, "forward_failed: " + msg);
        }
    }

    private RoutingMessages.ShardCommitTxnResult handleCommitTxn(
            RoutingMessages.ShardCommitTxn commit) {
        ShardLookupResult lookup = routingCoordinator.lookupOwner(new ShardId(commit.shardId()));
        if (lookup.status() != RoutingStatus.ASSIGNED) {
            return RoutingMessages.ShardCommitTxnResult.error(
                    commit.txnId(), "routing_status=" + lookup.status());
        }
        ShardAssignment assignment = lookup.assignment();
        if (assignment.epoch().value() != commit.epoch()) {
            return RoutingMessages.ShardCommitTxnResult.error(commit.txnId(), "stale_epoch");
        }
        RpcEndpointRef target = executorEndpoints.get(assignment.executorId());
        if (target == null) {
            return RoutingMessages.ShardCommitTxnResult.error(
                    commit.txnId(), "executor_endpoint_missing");
        }
        try {
            RpcTimeout timeout = askTimeout();
            Object reply =
                    target.askSync(
                            commit, timeout, scala.reflect.ClassTag$.MODULE$.apply(Object.class));
            if (reply instanceof RoutingMessages.ShardCommitTxnResult result) {
                return result;
            }
            return RoutingMessages.ShardCommitTxnResult.error(commit.txnId(), "invalid_reply");
        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            if (isHardRpcFailure(e)) {
                routingCoordinator.onExecutorDown(assignment.executorId());
                cleanupExecutorEndpoint(assignment.executorId());
            }
            return RoutingMessages.ShardCommitTxnResult.error(
                    commit.txnId(), "forward_failed: " + msg);
        }
    }

    private void handleAbortTxn(RoutingMessages.ShardAbortTxn abort) {
        ShardLookupResult lookup = routingCoordinator.lookupOwner(new ShardId(abort.shardId()));
        if (lookup.status() != RoutingStatus.ASSIGNED) {
            log.warn(
                    "Shard owner not found for abort: table={}, shard={}, status={}",
                    abort.tableIdentifier(),
                    abort.shardId(),
                    lookup.status());
            return;
        }
        ShardAssignment assignment = lookup.assignment();
        if (assignment.epoch().value() != abort.epoch()) {
            log.info(
                    "Abort epoch mismatch: assignment_epoch={}, abort_epoch={}, table={}, shard={}. Forwarding anyway for cleanup.",
                    assignment.epoch().value(),
                    abort.epoch(),
                    abort.tableIdentifier(),
                    abort.shardId());
        }
        RpcEndpointRef target = executorEndpoints.get(assignment.executorId());
        if (target == null) {
            log.warn(
                    "Executor endpoint missing for abort: table={}, shard={}, executor={}",
                    abort.tableIdentifier(),
                    abort.shardId(),
                    assignment.executorId());
            return;
        }
        try {
            target.send(abort);
        } catch (Exception e) {
            log.warn(
                    "Failed to forward abort: table={}, shard={}, error={}",
                    abort.tableIdentifier(),
                    abort.shardId(),
                    e.getMessage());
            if (isHardRpcFailure(e)) {
                routingCoordinator.onExecutorDown(assignment.executorId());
                cleanupExecutorEndpoint(assignment.executorId());
            }
        }
    }

    private RoutingMessages.WriteAck handleWrite(RoutingMessages.ShardWriteRequest request) {
        ShardLookupResult lookup = routingCoordinator.lookupOwner(new ShardId(request.shardId()));
        if (lookup.status() != RoutingStatus.ASSIGNED) {
            return RoutingMessages.WriteAck.retry("routing_status=" + lookup.status());
        }
        ShardAssignment assignment = lookup.assignment();
        if (assignment.epoch().value() != request.epoch()) {
            return RoutingMessages.WriteAck.staleEpoch("epoch_mismatch");
        }
        RpcEndpointRef target = executorEndpoints.get(assignment.executorId());
        if (target == null) {
            return RoutingMessages.WriteAck.retry("executor_endpoint_missing");
        }
        RoutingMessages.ShardWriteForward forward =
                new RoutingMessages.ShardWriteForward(
                        request.basePath(),
                        request.storeId(),
                        request.tableIdentifier(),
                        request.shardId(),
                        request.epoch(),
                        request.batchId(),
                        request.keys(),
                        request.values());
        try {
            RpcTimeout timeout = askTimeout();
            Object reply =
                    target.askSync(
                            forward, timeout, scala.reflect.ClassTag$.MODULE$.apply(Object.class));
            if (reply instanceof RoutingMessages.WriteAck ack) {
                return ack;
            }
            return RoutingMessages.WriteAck.error("invalid_reply");
        } catch (Exception e) {
            if (isRpcTimeout(e)) {
                // Timeout is not a proof of executor death. Keep routing stable and let the caller
                // retry.
                return RoutingMessages.WriteAck.retry("forward_timeout");
            }
            if (isHardRpcFailure(e)) {
                routingCoordinator.onExecutorDown(assignment.executorId());
                cleanupExecutorEndpoint(assignment.executorId());
            }
            return RoutingMessages.WriteAck.retry("forward_failed");
        }
    }

    private RoutingMessages.WriteAck handleMultiShardWrite(
            RoutingMessages.MultiShardWriteRequest request) {
        if (request.keys() == null
                || request.values() == null
                || request.shardIds() == null
                || request.epochs() == null) {
            return RoutingMessages.WriteAck.error("invalid_batch");
        }
        if (request.keys().length != request.values().length
                || request.keys().length != request.shardIds().length
                || request.keys().length != request.epochs().length) {
            return RoutingMessages.WriteAck.error("invalid_batch_len");
        }
        if (request.keys().length == 0) {
            return RoutingMessages.WriteAck.ok();
        }

        HashMap<Integer, ShardBatch> batches = new HashMap<>();
        for (int i = 0; i < request.keys().length; i++) {
            int shardId = request.shardIds()[i];
            long epoch = request.epochs()[i];
            ShardBatch batch = batches.computeIfAbsent(shardId, ignored -> new ShardBatch(epoch));
            if (batch.epoch != epoch) {
                return RoutingMessages.WriteAck.retry("mixed_epoch_in_shard");
            }
            batch.add(request.keys()[i], request.values()[i]);
        }

        // Pre-check routing/epochs/endpoints before doing any forwarding to reduce partial success.
        HashMap<Integer, ShardAssignment> assignments = new HashMap<>(batches.size());
        for (Map.Entry<Integer, ShardBatch> e : batches.entrySet()) {
            int shardId = e.getKey();
            ShardBatch batch = e.getValue();
            ShardLookupResult lookup = routingCoordinator.lookupOwner(new ShardId(shardId));
            if (lookup == null || lookup.status() != RoutingStatus.ASSIGNED) {
                return RoutingMessages.WriteAck.retry(
                        "routing_status=" + (lookup == null ? "null" : lookup.status()));
            }
            ShardAssignment assignment = lookup.assignment();
            if (assignment == null) {
                return RoutingMessages.WriteAck.retry("missing_assignment");
            }
            if (assignment.epoch().value() != batch.epoch) {
                return RoutingMessages.WriteAck.staleEpoch("epoch_mismatch");
            }
            RpcEndpointRef target = executorEndpoints.get(assignment.executorId());
            if (target == null) {
                return RoutingMessages.WriteAck.retry("executor_endpoint_missing");
            }
            assignments.put(shardId, assignment);
        }

        RpcTimeout timeout = askTimeout();
        for (Map.Entry<Integer, ShardBatch> e : batches.entrySet()) {
            int shardId = e.getKey();
            ShardBatch batch = e.getValue();
            ShardAssignment assignment = assignments.get(shardId);
            RpcEndpointRef target = executorEndpoints.get(assignment.executorId());

            RoutingMessages.ShardWriteForward forward =
                    new RoutingMessages.ShardWriteForward(
                            request.basePath(),
                            request.storeId(),
                            request.tableIdentifier(),
                            shardId,
                            batch.epoch,
                            request.batchId(),
                            batch.keys(),
                            batch.values(),
                            request.taskAttemptId());
            try {
                Object reply =
                        target.askSync(
                                forward,
                                timeout,
                                scala.reflect.ClassTag$.MODULE$.apply(Object.class));
                if (!(reply instanceof RoutingMessages.WriteAck ack)) {
                    return RoutingMessages.WriteAck.error("invalid_reply");
                }
                if (ack.status() != RoutingMessages.WriteAckStatus.OK) {
                    return ack;
                }
            } catch (Exception ex) {
                if (isRpcTimeout(ex)) {
                    // Same contract as single-shard: timeout is retryable and does NOT mark
                    // executor down.
                    return RoutingMessages.WriteAck.retry("forward_timeout");
                }
                if (isHardRpcFailure(ex)) {
                    routingCoordinator.onExecutorDown(assignment.executorId());
                    cleanupExecutorEndpoint(assignment.executorId());
                }
                return RoutingMessages.WriteAck.retry("forward_failed");
            }
        }
        return RoutingMessages.WriteAck.ok();
    }

    private void cleanupExecutorEndpoint(String executorId) {
        executorEndpoints.remove(executorId);
        executorEndpointInfos.remove(executorId);
        dispatchedRecoveryShards
                .entrySet()
                .removeIf(e -> executorId.equals(e.getValue().executorId));
    }

    public boolean sendRecoveryTask(RecoveryTaskMessage message) {
        RpcEndpointRef target = executorEndpoints.get(message.targetExecutorId());
        if (target == null) {
            return false;
        }
        try {
            RpcTimeout timeout = askTimeout();
            Object reply =
                    target.askSync(
                            message, timeout, scala.reflect.ClassTag$.MODULE$.apply(Object.class));
            return Boolean.TRUE.equals(reply);
        } catch (Exception e) {
            return false;
        }
    }

    private void dispatchPendingRecoveryTasks() {
        if (!(routingCoordinator instanceof InMemoryRoutingCoordinator coordinator)) {
            return;
        }
        String basePath = getStorageMmapPath();
        String storeId = getStorageStoreId();
        long timeoutMs = getRecoveryTimeoutMs();
        long now = System.currentTimeMillis();
        for (ShardLookupResult routing : coordinator.getRecoveringShards()) {
            ShardAssignment assignment = routing.assignment();
            if (assignment == null) {
                continue;
            }
            ShardId shardId = assignment.shardId();
            String targetExecutorId = assignment.executorId();
            RpcEndpointRef targetRef = executorEndpoints.get(targetExecutorId);
            if (targetRef == null) {
                continue;
            }
            java.util.List<io.github.rawwise.backend.routing.ShardRecoveryRecord> records =
                    coordinator.getShardHistory(shardId);
            if (records.isEmpty()) {
                continue;
            }
            RecoveryTaskMessage message =
                    new RecoveryTaskMessage(targetExecutorId, records, basePath, storeId);
            DispatchEntry entry = new DispatchEntry(targetExecutorId, now);
            dispatchedRecoveryShards.compute(
                    shardId,
                    (key, existing) -> {
                        if (existing != null
                                && existing.executorId.equals(targetExecutorId)
                                && (now - existing.dispatchTimestamp) < timeoutMs) {
                            return existing;
                        }
                        return entry;
                    });
            if (dispatchedRecoveryShards.get(shardId) != entry) {
                continue;
            }
            boolean sent = sendRecoveryTask(message);
            if (sent) {
                log.info(
                        "Dispatched recovery task for shard {} to executor {}",
                        shardId,
                        targetExecutorId);
            } else {
                dispatchedRecoveryShards.remove(shardId);
            }
        }
    }

    private void handleRecoveryComplete(RecoveryCompleteMessage message) {
        if (routingCoordinator instanceof InMemoryRoutingCoordinator coordinator) {
            if (message.success()) {
                boolean cleared =
                        coordinator.onRecoveryComplete(message.shardId(), message.epoch(), true);
                if (cleared) {
                    recoveryRetryCount.remove(message.shardId());
                    dispatchedRecoveryShards.remove(message.shardId());
                }
            } else {
                coordinator.onRecoveryComplete(message.shardId(), message.epoch(), false);
                int maxRetries = getMaxRetries();
                int currentRetries = recoveryRetryCount.getOrDefault(message.shardId(), 0) + 1;
                if (currentRetries < maxRetries) {
                    recoveryRetryCount.put(message.shardId(), currentRetries);
                    log.warn(
                            "Recovery failed for shard {} (attempt {}/{}), will retry. Error: {}",
                            message.shardId(),
                            currentRetries,
                            maxRetries,
                            message.errorMessage());
                    scheduleRecoveryRetry(message.shardId(), message.epoch());
                } else {
                    log.error(
                            "Recovery failed for shard {} after {} retries, transitioning shard to RECOVERING on new executor after recovery failure. Error: {}",
                            message.shardId(),
                            maxRetries,
                            message.errorMessage());
                    recoveryRetryCount.remove(message.shardId());
                    dispatchedRecoveryShards.remove(message.shardId());
                    coordinator.reassignShardForRecovery(message.shardId());
                    submitRecoveryDispatch();
                }
            }
        }
    }

    private void handleConfigEpochHeartbeat(
            ConfigPropagationMessages.ConfigEpochHeartbeat heartbeat) {
        if (configRegistry == null) {
            return;
        }
        long driverEpoch = configRegistry.currentEpoch();
        if (heartbeat.configEpoch() < driverEpoch) {
            propagationExecutor.submit(() -> doReconcileLaggingExecutor(heartbeat, driverEpoch));
        }
    }

    private void doReconcileLaggingExecutor(
            ConfigPropagationMessages.ConfigEpochHeartbeat heartbeat, long driverEpoch) {
        log.info(
                "Executor {} lagging on config: executorEpoch={}, driverEpoch={}. Sending config update + commit.",
                heartbeat.executorId(),
                heartbeat.configEpoch(),
                driverEpoch);
        RpcEndpointRef target = executorEndpoints.get(heartbeat.executorId());
        if (target != null) {
            try {
                RawWiseConfig current = configRegistry.currentConfig();
                Set<String> allKeys = RawWiseOptions.allKeys();
                Map<String, String> allValues =
                        configRegistry.serializeConfigForPropagation(allKeys);
                String checksum =
                        DynamicConfigRegistry.computeValidationChecksum(
                                driverEpoch, allValues, allKeys);
                PropagationMode propagationMode =
                        PropagationMode.fromString(
                                current.get(RawWiseOptions.CONFIG_PROPAGATION_MODE));
                ConfigUpdateMessage updateMessage =
                        new ConfigUpdateMessage(
                                driverEpoch, allValues, allKeys, checksum, propagationMode);

                Object reply =
                        target.askSync(
                                updateMessage,
                                askTimeout(),
                                scala.reflect.ClassTag$.MODULE$.apply(Object.class));

                if (reply instanceof ConfigPropagationMessages.ConfigPrepareResponse prepareResponse
                        && prepareResponse.accepted()) {
                    ConfigCommitMessage commitMessage = new ConfigCommitMessage(driverEpoch);
                    target.send(commitMessage);
                    log.info(
                            "Sent config update + commit to lagging executor {}, epoch {}",
                            heartbeat.executorId(),
                            driverEpoch);
                } else {
                    String rejectReason =
                            (reply instanceof ConfigPropagationMessages.ConfigPrepareResponse pr)
                                    ? pr.rejectReason()
                                    : "unexpected_reply";
                    log.warn(
                            "Lagging executor {} rejected config prepare for epoch {}: {}",
                            heartbeat.executorId(),
                            driverEpoch,
                            rejectReason);
                }
            } catch (Exception e) {
                log.warn(
                        "Failed to send config update to lagging executor {}: {}",
                        heartbeat.executorId(),
                        e.getMessage());
            }
        }
    }

    private int getMaxRetries() {
        if (rawwiseConfig != null) {
            return rawwiseConfig.get(RawWiseOptions.RECOVERY_MAX_RETRIES);
        }
        return RawWiseOptions.RECOVERY_MAX_RETRIES.defaultValue();
    }

    private void submitRecoveryDispatch() {
        scheduler.submit(
                () -> {
                    try {
                        dispatchPendingRecoveryTasks();
                    } catch (Exception e) {
                        log.error("Failed to dispatch pending recovery tasks", e);
                    }
                });
    }

    private long getBackoffMs() {
        if (rawwiseConfig != null) {
            return rawwiseConfig.get(RawWiseOptions.RECOVERY_BACKOFF_MS);
        }
        return RawWiseOptions.RECOVERY_BACKOFF_MS.defaultValue();
    }

    private String getStorageMmapPath() {
        if (rawwiseConfig != null) {
            return rawwiseConfig.get(RawWiseOptions.STORAGE_MMAP_PATH);
        }
        return RawWiseOptions.STORAGE_MMAP_PATH.defaultValue();
    }

    private String getStorageStoreId() {
        if (rawwiseConfig != null) {
            return rawwiseConfig.get(RawWiseOptions.STORAGE_STORE_ID);
        }
        return RawWiseOptions.STORAGE_STORE_ID.defaultValue();
    }

    private long getRecoveryTimeoutMs() {
        if (rawwiseConfig != null) {
            return rawwiseConfig.get(RawWiseOptions.RECOVERY_TIMEOUT_MS);
        }
        return RawWiseOptions.RECOVERY_TIMEOUT_MS.defaultValue();
    }

    private void scheduleRecoveryRetry(
            ShardId shardId, io.github.rawwise.backend.routing.RoutingEpoch epoch) {
        long backoffMs = getBackoffMs();
        scheduler.schedule(
                () -> {
                    try {
                        ShardLookupResult lookup = routingCoordinator.lookupOwner(shardId);
                        if (lookup.assignment() != null) {
                            String targetExecutorId = lookup.assignment().executorId();
                            java.util.List<io.github.rawwise.backend.routing.ShardRecoveryRecord>
                                    records =
                                            routingCoordinator
                                                            instanceof
                                                            InMemoryRoutingCoordinator coordinator
                                                    ? coordinator.getShardHistory(shardId)
                                                    : java.util.List.of();
                            if (!records.isEmpty()) {
                                long now = System.currentTimeMillis();
                                dispatchedRecoveryShards.compute(
                                        shardId,
                                        (key, existing) ->
                                                new DispatchEntry(targetExecutorId, now));
                                RecoveryTaskMessage retryMessage =
                                        new RecoveryTaskMessage(
                                                targetExecutorId,
                                                records,
                                                getStorageMmapPath(),
                                                getStorageStoreId());
                                boolean sent = sendRecoveryTask(retryMessage);
                                if (!sent) {
                                    dispatchedRecoveryShards.remove(shardId);
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.error("Failed to schedule recovery retry for shard {}", shardId, e);
                    }
                },
                backoffMs,
                TimeUnit.MILLISECONDS);
    }

    private RpcTimeout askTimeout() {
        return RpcTimeout.apply(sparkConf, "spark.rpc.askTimeout", "30s");
    }

    private static boolean isRpcTimeout(Throwable t) {
        Throwable cur = t;
        for (int i = 0; i < 32 && cur != null; i++) {
            if (cur instanceof RpcTimeoutException) {
                return true;
            }
            cur = cur.getCause();
        }
        return false;
    }

    private static boolean isHardRpcFailure(Throwable t) {
        Throwable cur = t;
        for (int i = 0; i < 32 && cur != null; i++) {
            if (cur instanceof RpcEndpointNotFoundException) {
                return true;
            }
            if (cur instanceof RpcEnvStoppedException) {
                return true;
            }
            if (cur instanceof ConnectException) {
                return true;
            }
            if (cur instanceof java.nio.channels.ClosedChannelException) {
                return true;
            }
            cur = cur.getCause();
        }
        return false;
    }

    private static final class ShardBatch {
        private final long epoch;
        private byte[][] keys = new byte[128][];
        private byte[][] values = new byte[128][];
        private int size = 0;

        private ShardBatch(long epoch) {
            this.epoch = epoch;
        }

        private void add(byte[] key, byte[] value) {
            if (size == keys.length) {
                int newCap = Math.min(Integer.MAX_VALUE - 8, Math.max(16, size * 2));
                keys = java.util.Arrays.copyOf(keys, newCap);
                values = java.util.Arrays.copyOf(values, newCap);
            }
            keys[size] = key;
            values[size] = value;
            size++;
        }

        private byte[][] keys() {
            return size == keys.length ? keys : java.util.Arrays.copyOf(keys, size);
        }

        private byte[][] values() {
            return size == values.length ? values : java.util.Arrays.copyOf(values, size);
        }
    }

    private record DispatchEntry(String executorId, long dispatchTimestamp) {}

    DynamicConfigRegistry.PropagationResult propagateConfig(ConfigUpdateMessage message) {
        Map<String, RpcEndpointRef> snapshot = new HashMap<>(executorEndpoints);
        if (snapshot.isEmpty()) {
            return new DynamicConfigRegistry.PropagationResult(0, 0, 0, 0, true, Set.of());
        }

        int total = snapshot.size();
        long timeoutMs = computePropagationTimeout(total);

        List<String> executorIds = new ArrayList<>(snapshot.keySet());
        List<CompletableFuture<ConfigPropagationMessages.ConfigPrepareResponse>> futures =
                new ArrayList<>();

        for (String executorId : executorIds) {
            RpcEndpointRef ref = snapshot.get(executorId);
            futures.add(
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    RpcTimeout timeout = askTimeout();
                                    Object reply =
                                            ref.askSync(
                                                    message,
                                                    timeout,
                                                    scala.reflect.ClassTag$.MODULE$.apply(
                                                            Object.class));
                                    if (reply
                                            instanceof
                                            ConfigPropagationMessages.ConfigPrepareResponse
                                                            response) {
                                        return response;
                                    }
                                    log.warn(
                                            "Unexpected reply type from executor {} for config prepare: {}",
                                            executorId,
                                            reply.getClass().getSimpleName());
                                    return null;
                                } catch (Exception e) {
                                    log.warn(
                                            "Config prepare timeout/error for executor {}: {}",
                                            executorId,
                                            e.getMessage());
                                    return null;
                                }
                            },
                            propagationExecutor));
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.warn(
                    "Config propagation timed out after {}ms for epoch {}",
                    timeoutMs,
                    message.configEpoch());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Config propagation interrupted for epoch {}", message.configEpoch());
        } catch (ExecutionException e) {
            log.warn(
                    "Config propagation execution error for epoch {}: {}",
                    message.configEpoch(),
                    e.getMessage());
        }

        int acceptedCount = 0;
        int rejectedCount = 0;
        int timedOutCount = 0;
        Set<String> acceptedExecutorIds = new HashSet<>();
        Set<String> rejectedExecutorIds = new HashSet<>();

        for (int i = 0; i < futures.size(); i++) {
            String executorId = executorIds.get(i);
            CompletableFuture<ConfigPropagationMessages.ConfigPrepareResponse> future =
                    futures.get(i);
            if (future.isDone() && !future.isCompletedExceptionally()) {
                try {
                    ConfigPropagationMessages.ConfigPrepareResponse response = future.get();
                    if (response != null && response.accepted()) {
                        acceptedCount++;
                        acceptedExecutorIds.add(executorId);
                    } else {
                        rejectedCount++;
                        rejectedExecutorIds.add(executorId);
                        if (response != null) {
                            log.warn(
                                    "Executor {} rejected config prepare for epoch {}: {}",
                                    executorId,
                                    message.configEpoch(),
                                    response.rejectReason());
                        }
                    }
                } catch (Exception e) {
                    timedOutCount++;
                }
            } else {
                timedOutCount++;
            }
        }

        boolean quorumReached =
                message.propagationMode() == PropagationMode.ALL
                        ? rejectedCount == 0 && timedOutCount == 0
                        : acceptedCount * 2 > total;

        return new DynamicConfigRegistry.PropagationResult(
                total,
                acceptedCount,
                rejectedCount,
                timedOutCount,
                quorumReached,
                acceptedExecutorIds);
    }

    void sendCommitToExecutors(ConfigUpdateMessage message, Set<String> targetExecutorIds) {
        ConfigCommitMessage commitMessage = new ConfigCommitMessage(message.configEpoch());
        for (String executorId : targetExecutorIds) {
            RpcEndpointRef ref = executorEndpoints.get(executorId);
            if (ref != null) {
                try {
                    ref.send(commitMessage);
                } catch (Exception e) {
                    log.warn(
                            "Failed to send ConfigCommitMessage to executor {}: {}",
                            executorId,
                            e.getMessage());
                }
            }
        }
    }

    void sendAbortToExecutors(ConfigUpdateMessage message, Set<String> acceptedExecutorIds) {
        ConfigAbortMessage abortMessage = new ConfigAbortMessage(message.configEpoch());
        Map<String, RpcEndpointRef> snapshot = new HashMap<>(executorEndpoints);
        Set<String> needsAbort = new HashSet<>(acceptedExecutorIds);
        for (String executorId : snapshot.keySet()) {
            if (!acceptedExecutorIds.contains(executorId)) {
                needsAbort.add(executorId);
            }
        }
        for (String executorId : needsAbort) {
            RpcEndpointRef ref = snapshot.get(executorId);
            if (ref != null) {
                try {
                    ref.send(abortMessage);
                } catch (Exception e) {
                    log.warn(
                            "Failed to send ConfigAbortMessage to executor {}: {}",
                            executorId,
                            e.getMessage());
                }
            }
        }
    }

    private long computePropagationTimeout(int totalExecutors) {
        if (configRegistry != null) {
            return configRegistry.get(RawWiseOptions.CONFIG_PROPAGATION_TIMEOUT_BASE_MS)
                    + (long) (Math.log(totalExecutors) / Math.log(2))
                            * configRegistry.get(
                                    RawWiseOptions.CONFIG_PROPAGATION_TIMEOUT_PER_EXECUTOR_MS);
        }
        return RawWiseOptions.CONFIG_PROPAGATION_TIMEOUT_BASE_MS.defaultValue()
                + (long) (Math.log(totalExecutors) / Math.log(2))
                        * RawWiseOptions.CONFIG_PROPAGATION_TIMEOUT_PER_EXECUTOR_MS.defaultValue();
    }
}
