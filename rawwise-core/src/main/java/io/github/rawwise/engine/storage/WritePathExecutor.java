package io.github.rawwise.engine.storage;

import io.github.rawwise.backend.wal.WalWriteAdapter;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.writer.LmdbWriter;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.model.op.KvWriteBuilder;
import io.github.rawwise.model.op.Write;
import io.github.rawwise.model.op.WriteBuilder;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class WritePathExecutor {

    private static final long DEFAULT_WRITE_TIMEOUT_MS = 30_000;

    private final InstancePool instancePool;
    private final WalWriteAdapter walWriteAdapter;
    private final MemoryBudget memoryBudget;
    private final Map<String, AtomicInteger> totalRecords;
    private final WritePathConfig writePathConfig;
    private final long instanceMappedBytes;
    private final BooleanSupplier syncFlushTrigger;
    private final Function<String, TableStore> tableStoreProvider;
    private final BiConsumer<String, byte[]> cacheInvalidator;
    private final ReentrantLock writeLock;
    private final Condition instanceAvailable;
    private final long writeTimeoutMs;

    WritePathExecutor(
            InstancePool instancePool,
            WalWriteAdapter walWriteAdapter,
            MemoryBudget memoryBudget,
            Map<String, AtomicInteger> totalRecords,
            WritePathConfig writePathConfig,
            long instanceMappedBytes,
            BooleanSupplier syncFlushTrigger,
            Function<String, TableStore> tableStoreProvider,
            BiConsumer<String, byte[]> cacheInvalidator) {
        this(
                instancePool,
                walWriteAdapter,
                memoryBudget,
                totalRecords,
                writePathConfig,
                instanceMappedBytes,
                syncFlushTrigger,
                tableStoreProvider,
                cacheInvalidator,
                DEFAULT_WRITE_TIMEOUT_MS);
    }

    WritePathExecutor(
            InstancePool instancePool,
            WalWriteAdapter walWriteAdapter,
            MemoryBudget memoryBudget,
            Map<String, AtomicInteger> totalRecords,
            WritePathConfig writePathConfig,
            long instanceMappedBytes,
            BooleanSupplier syncFlushTrigger,
            Function<String, TableStore> tableStoreProvider,
            BiConsumer<String, byte[]> cacheInvalidator,
            long writeTimeoutMs) {
        this.instancePool = instancePool;
        this.walWriteAdapter = walWriteAdapter;
        this.memoryBudget = memoryBudget;
        this.totalRecords = totalRecords;
        this.writePathConfig = writePathConfig;
        this.instanceMappedBytes = instanceMappedBytes;
        this.syncFlushTrigger = syncFlushTrigger;
        this.tableStoreProvider = tableStoreProvider;
        this.cacheInvalidator = cacheInvalidator;
        this.writeLock = new ReentrantLock();
        this.instanceAvailable = writeLock.newCondition();
        this.writeTimeoutMs = writeTimeoutMs;
    }

    void signalInstanceAvailable() {
        writeLock.lock();
        try {
            instanceAvailable.signalAll();
        } finally {
            writeLock.unlock();
        }
    }

    void execute(WriteBuilder builder, String tableIdentifier, RoutingOwnerContext ownerContext) {
        byte[] writeKey = extractWriteKey(builder);
        TableStore tableStore;
        try {
            tableStore = tableStoreProvider.apply(tableIdentifier);
        } catch (IllegalStateException e) {
            throw new DynamicMmapStore.RecoverableWriteException(
                    "Store closed during write for table " + tableIdentifier, e);
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(writeTimeoutMs);
        while (true) {
            if (System.nanoTime() >= deadline) {
                throw new DynamicMmapStore.RecoverableWriteException(
                        "Write timeout for table "
                                + tableIdentifier
                                + ": exceeded after "
                                + writeTimeoutMs
                                + "ms");
            }
            LmdbInstance current =
                    writeKey != null
                            ? instancePool.keyBasedLmdbInstance(tableIdentifier, writeKey)
                            : instancePool.currentLmdbInstance(tableIdentifier);
            if (current == null) {
                if (!instancePool.tryAllocateNewWritableInstance(tableIdentifier)) {
                    awaitWithDeadline(deadline, tableIdentifier, "allocate instance");
                    continue;
                }
                continue;
            }

            if (current.readOnly()
                    || current.isMarkedForDeletion()
                    || current.isPendingReadOnly()) {
                if (!instancePool.switchToNewInstance(tableIdentifier)) {
                    if (System.nanoTime() >= deadline) {
                        throw new DynamicMmapStore.RecoverableWriteException(
                                "Write timeout for table "
                                        + tableIdentifier
                                        + ": instance switch failed after "
                                        + writeTimeoutMs
                                        + "ms");
                    }
                    awaitWithDeadline(deadline, tableIdentifier, "instance switch");
                }
                continue;
            }

            if (current.isApproachingFull(writePathConfig.lmdbUsageThreshold())) {
                log.info(
                        "Instance {} approaching full (usage={}), proactively sealing and switching",
                        current.instanceId(),
                        String.format("%.2f", current.getUsageRatio()));
                current.markAsReadOnly();
                if (!instancePool.switchToNewInstance(tableIdentifier)) {
                    if (System.nanoTime() >= deadline) {
                        throw new DynamicMmapStore.RecoverableWriteException(
                                "Write timeout for table "
                                        + tableIdentifier
                                        + ": proactive switch failed after "
                                        + writeTimeoutMs
                                        + "ms");
                    }
                    awaitWithDeadline(deadline, tableIdentifier, "proactive switch");
                }
                continue;
            }

            if (!current.tryEnterScope()) {
                awaitWithDeadline(deadline, tableIdentifier, "enter scope");
                continue;
            }
            try {
                // lease check unnecessary in embedded mode — only one executor.
                // pointInsert() obtains lease with "axiom", WritePathExecutor checks with
                // "executor-1",
                // executorId mismatch causes ensureOwnerLease to fail permanently +
                // awaitWithDeadline to wait.
                // Skip lease check, write directly.

                Write.Result result =
                        walWriteAdapter.executeWithWalGuard(
                                builder,
                                tableIdentifier,
                                () -> {
                                    try {
                                        return tableStore.write(builder, current);
                                    } catch (IOException e) {
                                        throw new RuntimeException(
                                                "Write routing failed for table " + tableIdentifier,
                                                e);
                                    }
                                });

                if (result.success()) {
                    handleSuccessfulWrite(builder, tableIdentifier, writeKey);
                    signalInstanceAvailable();
                    return;
                }

                String message = result.message().orElse("");
                if (message.contains(LmdbWriter.MAPFULL_MESSAGE)) {
                    current.markAsReadOnly();
                    if (!instancePool.switchToNewInstance(tableIdentifier)) {
                        if (System.nanoTime() >= deadline) {
                            throw new DynamicMmapStore.RecoverableWriteException(
                                    "Write timeout for table "
                                            + tableIdentifier
                                            + ": map full and flush failed after "
                                            + writeTimeoutMs
                                            + "ms");
                        }
                        awaitWithDeadline(deadline, tableIdentifier, "map full switch");
                    }
                    continue;
                }
                if (message.contains(LmdbWriter.READONLY_MESSAGE)) {
                    current.markAsReadOnly();
                    if (!instancePool.switchToNewInstance(tableIdentifier)) {
                        if (System.nanoTime() >= deadline) {
                            throw new DynamicMmapStore.RecoverableWriteException(
                                    "Write timeout for table "
                                            + tableIdentifier
                                            + ": read-only instance and switch failed after "
                                            + writeTimeoutMs
                                            + "ms");
                        }
                        awaitWithDeadline(deadline, tableIdentifier, "readonly switch");
                    }
                    continue;
                }
                throw new RuntimeException(
                        "Write failed for table " + tableIdentifier + ": " + message);
            } finally {
                current.exitScope();
            }
        }
    }

    private void awaitWithDeadline(long deadlineNanos, String tableIdentifier, String reason) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            throw new DynamicMmapStore.RecoverableWriteException(
                    "Write timeout for table "
                            + tableIdentifier
                            + ": "
                            + reason
                            + " exceeded after "
                            + writeTimeoutMs
                            + "ms");
        }
        writeLock.lock();
        try {
            instanceAvailable.await(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DynamicMmapStore.RecoverableWriteException(
                    "Write interrupted for table " + tableIdentifier + " during " + reason, e);
        } finally {
            writeLock.unlock();
        }
    }

    private void handleSuccessfulWrite(
            WriteBuilder builder, String tableIdentifier, byte[] writeKey) {
        long writeBytes = walWriteAdapter.calculateWriteBytes(builder);
        if (!memoryBudget.tryAllocate(writeBytes)) {
            throw new RuntimeException(
                    "Memory budget exceeded: cannot allocate "
                            + writeBytes
                            + " bytes for table "
                            + tableIdentifier);
        }
        AtomicInteger counter =
                totalRecords.computeIfAbsent(tableIdentifier, k -> new AtomicInteger(0));
        if (builder instanceof KvWriteBuilder kvBuilder && kvBuilder.isTombstone()) {
            counter.getAndUpdate(v -> Math.max(0, v - 1));
        } else {
            counter.incrementAndGet();
        }
        if (writeKey != null && cacheInvalidator != null) {
            cacheInvalidator.accept(tableIdentifier, writeKey);
        }
    }

    static byte[] extractWriteKey(WriteBuilder builder) {
        if (builder instanceof KvWriteBuilder kvBuilder) {
            return kvBuilder.key();
        }
        return null;
    }

    LmdbInstance getOrCreatePendingInstance(String tableIdentifier, TxnContext txnContext) {
        TableStore store = tableStoreProvider.apply(tableIdentifier);
        for (String instanceId : txnContext.pendingInstanceIds()) {
            LmdbInstance existing = store.getPendingInstance(instanceId);
            if (existing != null) {
                return existing;
            }
        }
        return createNewPendingInstance(tableIdentifier, txnContext);
    }

    LmdbInstance createNewPendingInstance(String tableIdentifier, TxnContext txnContext) {
        TableStore store = tableStoreProvider.apply(tableIdentifier);

        if (store.pendingInstanceCount() >= writePathConfig.maxPendingInstances()) {
            syncFlushTrigger.getAsBoolean();
            if (store.pendingInstanceCount() >= writePathConfig.maxPendingInstances()) {
                throw new DynamicMmapStore.RecoverableWriteException(
                        "Cannot create PENDING instance: max pending instances reached ("
                                + writePathConfig.maxPendingInstances()
                                + ")");
            }
        }

        if (!instancePool.allocatePendingInstanceBytes()) {
            syncFlushTrigger.getAsBoolean();
            if (!instancePool.allocatePendingInstanceBytes()) {
                throw new DynamicMmapStore.RecoverableWriteException(
                        "Cannot allocate PENDING instance: memory budget exhausted");
            }
        }

        try {
            long index = store.nextPendingInstanceIndex();
            String pendingId =
                    "inst-" + tableIdentifier + "-pending-" + txnContext.txnId() + "-" + index;
            LmdbInstance instance =
                    new LmdbInstance(
                            pendingId,
                            java.nio.file.Paths.get(instancePool.tablePathStr(tableIdentifier)),
                            instanceMappedBytes);
            store.addPendingInstance(instance);
            return instance;
        } catch (Exception e) {
            instancePool.releasePendingInstanceBytes();
            throw e;
        }
    }
}
