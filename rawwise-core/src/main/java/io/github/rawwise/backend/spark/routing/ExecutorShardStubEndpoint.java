package io.github.rawwise.backend.spark.routing;

import io.github.rawwise.backend.wal.DefaultWALRecord;
import io.github.rawwise.backend.wal.WAL;
import io.github.rawwise.backend.wal.WALWriteException;
import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.SimpleTableRef;
import io.github.rawwise.engine.txn.WriteBuffer;
import io.github.rawwise.engine.txn.clock.AtomicLongClock;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnContextHolder;
import io.github.rawwise.engine.txn.state.TxnState;
import io.github.rawwise.governance.config.ConfigAbortMessage;
import io.github.rawwise.governance.config.ConfigCommitMessage;
import io.github.rawwise.governance.config.ConfigPropagationMessages;
import io.github.rawwise.governance.config.ConfigUpdateMessage;
import io.github.rawwise.governance.config.DynamicConfigRegistry;
import io.github.rawwise.governance.config.RawWiseOptions;
import io.github.rawwise.model.op.KvWriteBuilder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.rpc.RpcAddress;
import org.apache.spark.rpc.RpcCallContext;
import org.apache.spark.rpc.RpcEndpoint;
import org.apache.spark.rpc.RpcEndpointRef;
import org.apache.spark.rpc.RpcEnv;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;
import scala.runtime.BoxedUnit;

@Slf4j
final class ExecutorShardStubEndpoint implements RpcEndpoint {

    static final String ENDPOINT_NAME = "rawwise-executor-shard-endpoint";

    private final RpcEnv rpcEnv;
    private final SparkConf sparkConf;
    private final DynamicConfigRegistry configRegistry;
    private final String executorId;
    private final ScheduledExecutorService heartbeatScheduler =
            Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread t = new Thread(r, "rawwise-config-heartbeat");
                        t.setDaemon(true);
                        return t;
                    });

    private final ScheduledExecutorService cleanupScheduler =
            Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread t = new Thread(r, "rawwise-shard-write-cleanup");
                        t.setDaemon(true);
                        return t;
                    });

    private final ConcurrentHashMap<String, ShardWriteState> activeWrites =
            new ConcurrentHashMap<>();

    private volatile AtomicLongClock clock;

    private static final long WRITE_STATE_EXPIRY_MS = TimeUnit.MINUTES.toMillis(5);

    /**
     * Records the state of an in-flight shard write that has been buffered but not yet committed.
     * Keyed by Spark taskAttemptId for cross-thread lifecycle tracking (WriteForward -> PrepareTxn
     * -> CommitTxn).
     */
    private static class ShardWriteState {
        final String taskAttemptId;
        final String tableId;
        final int shardId;
        final long epoch;
        final TxnContext txnContext;
        final DynamicMmapStore store;
        final String basePath;
        final long writeTime;

        ShardWriteState(
                String taskAttemptId,
                String tableId,
                int shardId,
                long epoch,
                TxnContext txnContext,
                DynamicMmapStore store,
                String basePath) {
            this.taskAttemptId = taskAttemptId;
            this.tableId = tableId;
            this.shardId = shardId;
            this.epoch = epoch;
            this.txnContext = txnContext;
            this.store = store;
            this.basePath = basePath;
            this.writeTime = System.currentTimeMillis();
        }
    }

    ExecutorShardStubEndpoint(
            RpcEnv rpcEnv,
            SparkConf sparkConf,
            DynamicConfigRegistry configRegistry,
            String executorId) {
        this.rpcEnv = Objects.requireNonNull(rpcEnv, "rpcEnv must not be null");
        this.sparkConf = Objects.requireNonNull(sparkConf, "sparkConf must not be null");
        this.configRegistry = configRegistry;
        this.executorId = Objects.requireNonNull(executorId, "executorId must not be null");
    }

    @Override
    public RpcEnv rpcEnv() {
        return rpcEnv;
    }

    @Override
    public void onStart() {
        if (configRegistry != null) {
            scheduleNextHeartbeat();
            log.info("Config epoch heartbeat scheduled: executorId={}", executorId);
        }
        scheduleCleanup();
    }

    private void scheduleCleanup() {
        cleanupScheduler.scheduleWithFixedDelay(
                this::cleanExpiredStates,
                WRITE_STATE_EXPIRY_MS,
                WRITE_STATE_EXPIRY_MS,
                TimeUnit.MILLISECONDS);
    }

    private void scheduleNextHeartbeat() {
        long intervalMs = configRegistry.get(RawWiseOptions.CONFIG_HEARTBEAT_INTERVAL_MS);
        heartbeatScheduler.schedule(
                () -> {
                    try {
                        sendConfigEpochHeartbeat();
                    } catch (Exception e) {
                        log.debug("Config epoch heartbeat failed: {}", e.getMessage());
                    } finally {
                        scheduleNextHeartbeat();
                    }
                },
                intervalMs,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void onStop() {
        heartbeatScheduler.shutdownNow();
        cleanupScheduler.shutdownNow();
        boolean interrupted = false;
        try {
            if (!heartbeatScheduler.awaitTermination(3, TimeUnit.SECONDS)) {
                log.warn("Heartbeat scheduler did not terminate in time");
            }
        } catch (InterruptedException e) {
            interrupted = true;
        }
        try {
            if (!cleanupScheduler.awaitTermination(3, TimeUnit.SECONDS)) {
                log.warn("Cleanup scheduler did not terminate in time");
            }
        } catch (InterruptedException e) {
            interrupted = true;
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        activeWrites.clear();
    }

    @Override
    public PartialFunction<Object, BoxedUnit> receive() {
        return new AbstractPartialFunction<>() {
            @Override
            public boolean isDefinedAt(Object message) {
                return message instanceof ConfigCommitMessage
                        || message instanceof ConfigAbortMessage;
            }

            @Override
            public BoxedUnit apply(Object message) {
                if (message instanceof ConfigCommitMessage commit) {
                    handleConfigCommit(commit);
                } else if (message instanceof ConfigAbortMessage abort) {
                    handleConfigAbort(abort);
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
                return message instanceof ConfigUpdateMessage
                        || message instanceof RoutingMessages.ShardWriteForward
                        || message instanceof RoutingMessages.ShardPrepareTxn
                        || message instanceof RoutingMessages.ShardCommitTxn;
            }

            @Override
            public BoxedUnit apply(Object message) {
                if (message instanceof ConfigUpdateMessage updateMsg) {
                    context.reply(handleConfigUpdate(updateMsg));
                } else if (message instanceof RoutingMessages.ShardWriteForward forward) {
                    context.reply(handleWriteForward(forward));
                } else if (message instanceof RoutingMessages.ShardPrepareTxn prepare) {
                    context.reply(handlePrepareTxn(prepare));
                } else if (message instanceof RoutingMessages.ShardCommitTxn commit) {
                    context.reply(handleCommitTxn(commit));
                }
                return BoxedUnit.UNIT;
            }
        };
    }

    private ConfigPropagationMessages.ConfigPrepareResponse handleConfigUpdate(
            ConfigUpdateMessage message) {
        if (configRegistry == null) {
            return ConfigPropagationMessages.ConfigPrepareResponse.reject(
                    executorId, message.configEpoch(), "no_config_registry");
        }
        try {
            boolean accepted = configRegistry.reloadFromMessage(message);
            if (accepted) {
                return ConfigPropagationMessages.ConfigPrepareResponse.ok(
                        executorId, message.configEpoch());
            } else {
                return ConfigPropagationMessages.ConfigPrepareResponse.reject(
                        executorId, message.configEpoch(), "listener_veto");
            }
        } catch (Exception e) {
            log.error(
                    "Failed to handle ConfigUpdateMessage for epoch {}: {}",
                    message.configEpoch(),
                    e.getMessage());
            return ConfigPropagationMessages.ConfigPrepareResponse.reject(
                    executorId, message.configEpoch(), e.getMessage());
        }
    }

    private void handleConfigCommit(ConfigCommitMessage message) {
        if (configRegistry == null) {
            log.warn("Received ConfigCommitMessage but no configRegistry available");
            return;
        }
        try {
            configRegistry.commitFromMessage(message);
        } catch (Exception e) {
            log.error(
                    "Failed to handle ConfigCommitMessage for epoch {}: {}",
                    message.configEpoch(),
                    e.getMessage());
        }
    }

    private void handleConfigAbort(ConfigAbortMessage message) {
        if (configRegistry == null) {
            log.warn("Received ConfigAbortMessage but no configRegistry available");
            return;
        }
        try {
            configRegistry.handleAbort(message);
        } catch (Exception e) {
            log.error(
                    "Failed to handle ConfigAbortMessage for epoch {}: {}",
                    message.configEpoch(),
                    e.getMessage());
        }
    }

    private void sendConfigEpochHeartbeat() {
        if (configRegistry == null) {
            return;
        }
        long currentEpoch = configRegistry.currentEpoch();
        ConfigPropagationMessages.ConfigEpochHeartbeat heartbeat =
                new ConfigPropagationMessages.ConfigEpochHeartbeat(executorId, currentEpoch);

        String driverHost = sparkConf.get("spark.driver.host", null);
        int driverPort = sparkConf.getInt("spark.driver.port", 0);
        if (driverHost == null || driverPort == 0) {
            log.debug("Cannot send config heartbeat: driver host/port not configured");
            return;
        }

        try {
            RpcAddress driverAddress = RpcAddress.apply(driverHost, driverPort);
            RpcEndpointRef driverRef =
                    rpcEnv.setupEndpointRef(
                            driverAddress, RawWiseDriverRoutingEndpoint.ENDPOINT_NAME);
            driverRef.send(heartbeat);
        } catch (Exception e) {
            log.debug("Failed to send config epoch heartbeat to driver: {}", e.getMessage());
        }
    }

    private AtomicLongClock getOrInitClock() {
        if (clock == null) {
            synchronized (this) {
                if (clock == null) {
                    clock = new AtomicLongClock(0);
                    TxnContextHolder.setClock(clock);
                    log.info(
                            "LogicalClock initialized for ExecutorShardStubEndpoint: executorId={}",
                            executorId);
                }
            }
        }
        return clock;
    }

    private RoutingMessages.WriteAck handleWriteForward(RoutingMessages.ShardWriteForward forward) {
        try {
            DynamicMmapStore store =
                    DynamicMmapStore.getInstance(forward.basePath(), forward.storeId());

            getOrInitClock();

            Set<String> tableIds = Set.of(forward.tableIdentifier());
            TxnContext txnContext =
                    TxnContextHolder.begin(
                            tableIds, IsolationLevel.READ_COMMITTED, null, forward.taskAttemptId());

            byte[][] keys = forward.keys();
            byte[][] values = forward.values();
            for (int i = 0; i < keys.length; i++) {
                KvWriteBuilder builder =
                        KvWriteBuilder.builder()
                                .table(new SimpleTableRef(forward.tableIdentifier()))
                                .key(keys[i])
                                .value(values[i]);
                store.write(builder);
            }

            String taskAttemptId = forward.taskAttemptId();
            if (taskAttemptId != null && !taskAttemptId.isBlank()) {
                ShardWriteState state =
                        new ShardWriteState(
                                taskAttemptId,
                                forward.tableIdentifier(),
                                forward.shardId(),
                                forward.epoch(),
                                txnContext,
                                store,
                                forward.basePath());
                ShardWriteState previous = activeWrites.put(taskAttemptId, state);
                if (previous != null) {
                    log.warn(
                            "Replaced existing ShardWriteState for taskAttemptId={}, "
                                    + "previous txnId={}",
                            taskAttemptId,
                            previous.txnContext.txnId());
                    cleanupPreviousState(previous);
                }
            }

            return RoutingMessages.WriteAck.ok();
        } catch (Exception e) {
            log.error(
                    "handleWriteForward failed for table={} shard={}: {}",
                    forward.tableIdentifier(),
                    forward.shardId(),
                    e.getMessage(),
                    e);
            return RoutingMessages.WriteAck.error("write_failed: " + e.getMessage());
        } finally {
            TxnContextHolder.clear();
        }
    }

    private RoutingMessages.ShardPrepareTxnResult handlePrepareTxn(
            RoutingMessages.ShardPrepareTxn prepare) {
        try {
            ShardWriteState state = activeWrites.get(prepare.taskAttemptId());
            if (state == null) {
                return RoutingMessages.ShardPrepareTxnResult.error(
                        0L, "no_write_state_for: " + prepare.taskAttemptId());
            }
            if (state.shardId != prepare.shardId()) {
                return RoutingMessages.ShardPrepareTxnResult.error(
                        0L,
                        "shard_id_mismatch: expected="
                                + state.shardId
                                + ", actual="
                                + prepare.shardId());
            }
            if (state.epoch != prepare.epoch()) {
                return RoutingMessages.ShardPrepareTxnResult.error(
                        0L,
                        "epoch_mismatch: expected=" + state.epoch + ", actual=" + prepare.epoch());
            }
            if (!state.txnContext.isActive()) {
                return RoutingMessages.ShardPrepareTxnResult.error(
                        0L, "txn_not_active: status=" + state.txnContext.status());
            }
            return RoutingMessages.ShardPrepareTxnResult.ok(state.txnContext.txnId());
        } catch (Exception e) {
            log.error(
                    "handlePrepareTxn failed for taskAttemptId={}: {}",
                    prepare.taskAttemptId(),
                    e.getMessage(),
                    e);
            return RoutingMessages.ShardPrepareTxnResult.error(
                    0L, "prepare_failed: " + e.getMessage());
        }
    }

    private RoutingMessages.ShardCommitTxnResult handleCommitTxn(
            RoutingMessages.ShardCommitTxn commit) {
        String taskAttemptId = commit.taskAttemptId();
        ShardWriteState state = activeWrites.get(taskAttemptId);
        if (state == null) {
            return RoutingMessages.ShardCommitTxnResult.error(
                    commit.txnId(), "no_write_state_for: " + taskAttemptId);
        }
        if (state.shardId != commit.shardId()) {
            return RoutingMessages.ShardCommitTxnResult.error(
                    commit.txnId(),
                    "shard_id_mismatch: expected="
                            + state.shardId
                            + ", actual="
                            + commit.shardId());
        }
        if (state.epoch != commit.epoch()) {
            return RoutingMessages.ShardCommitTxnResult.error(
                    commit.txnId(),
                    "epoch_mismatch: expected=" + state.epoch + ", actual=" + commit.epoch());
        }

        TxnContext ctx = state.txnContext;
        if (!ctx.isActive()) {
            return RoutingMessages.ShardCommitTxnResult.error(
                    commit.txnId(), "txn_not_active_at_commit_time: status=" + ctx.status());
        }

        try {
            TableStore tableStore = state.store.tableStore(state.tableId);
            LmdbInstance instance = ensureWritableInstance(state, executorId, state.epoch);
            if (instance == null) {
                throw new DynamicMmapStore.RecoverableWriteException(
                        "No writable LMDB instance for table " + state.tableId);
            }

            if (clock == null) {
                throw new IllegalStateException("LogicalClock not initialized at commit time");
            }
            long commitTs = clock.next();

            Map<ByteBuffer, WriteBuffer.BufferEntry> drained =
                    ctx.writeBuffer().drain(state.tableId);

            List<byte[]> insertKeys = new ArrayList<>();
            List<byte[]> insertValues = new ArrayList<>();

            int deleteCount = 0;
            for (Map.Entry<ByteBuffer, WriteBuffer.BufferEntry> entry : drained.entrySet()) {
                byte[] key = entry.getKey().array();
                WriteBuffer.BufferEntry bufEntry = entry.getValue();

                if (bufEntry.type() == WriteBuffer.BufferEntryType.DELETED) {
                    insertKeys.add(key);
                    insertValues.add(NullableValueCodec.encodeTombstone(commitTs));
                    deleteCount++;
                } else {
                    byte[] value = bufEntry.getValue();
                    if (value == null) {
                        insertKeys.add(key);
                        insertValues.add(NullableValueCodec.encode(null));
                    } else {
                        insertKeys.add(key);
                        insertValues.add(value);
                    }
                }
            }

            int drainedCount = insertKeys.size();
            int insertCount = drainedCount - deleteCount;
            log.info(
                    "handleCommitTxn: drained {} entries ({} inserts, {} deletes) from WriteBuffer for taskAttemptId={}",
                    drainedCount,
                    insertCount,
                    deleteCount,
                    taskAttemptId);

            if (drainedCount > 0) {
                WAL wal = state.store.getWal();
                writeBatchWal(wal, state.tableId, insertKeys, insertValues);

                LmdbInstance.BatchPutResult putResult = instance.batchPut(insertKeys, insertValues);
                if (!putResult.isAllSuccess()) {
                    throw new RuntimeException(
                            "LMDB batch put partially failed for table "
                                    + state.tableId
                                    + ": "
                                    + putResult.status());
                }
            }

            ctx.markPreparing();
            ctx.markPrepared();
            ctx.markCommitting();
            ctx.markCommitted();

            activeWrites.remove(taskAttemptId);

            log.info(
                    "handleCommitTxn: committed txnId={} commitTs={} taskAttemptId={} entries={}",
                    commit.txnId(),
                    commitTs,
                    taskAttemptId,
                    drainedCount);
            return RoutingMessages.ShardCommitTxnResult.ok(commit.txnId(), commitTs);
        } catch (Exception e) {
            log.error(
                    "handleCommitTxn failed for txnId={} taskAttemptId={}: {}",
                    commit.txnId(),
                    taskAttemptId,
                    e.getMessage(),
                    e);
            abortTxnQuietly(ctx);
            return RoutingMessages.ShardCommitTxnResult.error(
                    commit.txnId(), "commit_failed: " + e.getMessage());
        }
    }

    private LmdbInstance ensureWritableInstance(
            ShardWriteState state, String executorId, long epoch) {
        TableStore tableStore = state.store.tableStore(state.tableId);
        LmdbInstance existing = tableStore.getCurrentWritableInstance();
        if (existing != null) {
            existing.ensureRoutingOwnerLease(executorId, epoch);
            return existing;
        }
        synchronized (tableStore) {
            existing = tableStore.getCurrentWritableInstance();
            if (existing != null) {
                existing.ensureRoutingOwnerLease(executorId, epoch);
                return existing;
            }
            String instanceId = LmdbInstance.generateInstanceId(state.tableId);
            java.nio.file.Path tablePath = java.nio.file.Paths.get(state.basePath, state.tableId);
            long mapSize = 256L * 1024 * 1024;
            LmdbInstance inst = new LmdbInstance(instanceId, tablePath, mapSize);
            inst.ensureRoutingOwnerLease(executorId, epoch);
            tableStore.addWritableInstance(inst);
            log.info(
                    "Created writable LMDB instance {} for table {} (path={}, mapSize={}MB)",
                    instanceId,
                    state.tableId,
                    tablePath,
                    mapSize / (1024 * 1024));
            return inst;
        }
    }

    private void writeBatchWal(
            WAL wal, String tableId, List<byte[]> insertKeys, List<byte[]> insertValues) {
        if (wal == null) {
            return;
        }
        byte[] tableIdBytes = tableId.getBytes(StandardCharsets.UTF_8);
        long now = System.nanoTime();

        for (int i = 0; i < insertKeys.size(); i++) {
            byte[] key = insertKeys.get(i);
            byte[] value = insertValues.get(i);
            byte[] walData = buildWalData(tableIdBytes, key, value);
            try {
                wal.append(new DefaultWALRecord(-1, walData, now, -1L, tableId, "DATA"), false);
            } catch (WALWriteException e) {
                throw new DynamicMmapStore.RecoverableWriteException(
                        "WAL write failed for table "
                                + tableId
                                + ", rejecting commit to prevent data loss",
                        e);
            }
        }
    }

    private static byte[] buildWalData(byte[] tableIdBytes, byte[] key, byte[] value) {
        ByteBuffer buf =
                ByteBuffer.allocate(4 + tableIdBytes.length + 4 + key.length + 4 + value.length);
        buf.putInt(tableIdBytes.length);
        buf.put(tableIdBytes);
        buf.putInt(key.length);
        buf.put(key);
        buf.putInt(value.length);
        buf.put(value);
        return buf.array();
    }

    private void cleanupPreviousState(ShardWriteState state) {
        try {
            abortTxnQuietly(state.txnContext);
        } catch (Exception e) {
            log.debug("Error cleaning up previous ShardWriteState: {}", e.getMessage());
        }
    }

    private void abortTxnQuietly(TxnContext ctx) {
        try {
            TxnState status = ctx.currentState();
            if (status == TxnState.ACTIVE
                    || status == TxnState.PREPARING
                    || status == TxnState.DATA_DURABLE
                    || status == TxnState.PUBLISHED) {
                ctx.markAborting();
                ctx.markAborted();
            }
        } catch (Exception e) {
            log.debug("Error aborting TxnContext {}: {}", ctx.txnId(), e.getMessage());
        }
    }

    private void cleanExpiredStates() {
        long now = System.currentTimeMillis();
        List<String> expiredKeys = new ArrayList<>();
        for (Map.Entry<String, ShardWriteState> entry : activeWrites.entrySet()) {
            ShardWriteState state = entry.getValue();
            if (now - state.writeTime > WRITE_STATE_EXPIRY_MS) {
                expiredKeys.add(entry.getKey());
            }
        }
        for (String key : expiredKeys) {
            ShardWriteState state = activeWrites.remove(key);
            if (state != null) {
                log.warn(
                        "Cleaning up expired ShardWriteState: taskAttemptId={}, age={}ms, txnId={}",
                        state.taskAttemptId,
                        now - state.writeTime,
                        state.txnContext.txnId());
                abortTxnQuietly(state.txnContext);
            }
        }
    }
}
