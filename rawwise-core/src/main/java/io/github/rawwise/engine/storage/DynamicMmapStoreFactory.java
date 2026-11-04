package io.github.rawwise.engine.storage;

import io.github.rawwise.backend.wal.WAL;
import io.github.rawwise.backend.wal.WALConfig;
import io.github.rawwise.backend.wal.WalComponents;
import io.github.rawwise.backend.wal.WalWriteAdapter;
import io.github.rawwise.engine.storage.flush.DefaultFlushCoordinator;
import io.github.rawwise.engine.storage.flush.FlushCoordinator;
import io.github.rawwise.engine.storage.flush.ParquetHdfsUploader;
import io.github.rawwise.engine.storage.table.StoreState;
import io.github.rawwise.governance.config.ConfigLoader;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.TaskContext;

@Slf4j
public class DynamicMmapStoreFactory {

    private static final ConcurrentHashMap<String, DynamicMmapStore> STORE_INSTANCES =
            new ConcurrentHashMap<>();

    public static DynamicMmapStore getInstance(String basePath, String id) {
        return getInstance(basePath, id, new Configuration(), ConfigLoader.load());
    }

    public static DynamicMmapStore getInstance(
            String basePath, String id, Configuration hadoopConf) {
        return getInstance(basePath, id, hadoopConf, ConfigLoader.load());
    }

    public static DynamicMmapStore getInstance(String basePath, String id, RawWiseConfig config) {
        return getInstance(basePath, id, new Configuration(), config);
    }

    public static DynamicMmapStore getInstance(
            String basePath, String id, Configuration hadoopConf, RawWiseConfig config) {
        String key = basePath + ":" + id;
        synchronized (STORE_INSTANCES) {
            DynamicMmapStore existing = STORE_INSTANCES.get(key);
            if (existing != null && !existing.isClosed()) {
                return existing;
            }
            DynamicMmapStore store =
                    create(
                            basePath,
                            id,
                            hadoopConf != null ? hadoopConf : new Configuration(),
                            config);
            STORE_INSTANCES.put(key, store);
            return store;
        }
    }

    public static DynamicMmapStore create(String basePath, String id, Configuration hadoopConf) {
        return create(basePath, id, hadoopConf, ConfigLoader.load());
    }

    public static DynamicMmapStore create(
            String basePath, String id, Configuration hadoopConf, RawWiseConfig config) {
        String instanceKey = basePath + ":" + id;
        long instanceMappedBytes = resolveInstanceMappedBytes(config);

        Map<String, AtomicInteger> totalRecords = new ConcurrentHashMap<>();

        int writableNum = resolveWritableNum(config);
        int maxDbs = config.get(RawWiseOptions.STORAGE_LMDB_MAX_DBS);
        String remoteBasePath = "";

        AtomicReference<DynamicMmapStore> storeRef = new AtomicReference<>();
        AtomicLong walFailedCount = new AtomicLong();

        WalComponents walComponents = null;
        StoreInfrastructure infrastructure = null;
        StoreComponents components = null;
        WritePathConfig writePathConfig = null;

        try {
            walComponents = createWalComponents(basePath, config, hadoopConf);

            StoreState storeState = new StoreState();
            ScheduledExecutorService monitorService = Executors.newScheduledThreadPool(1);
            ExecutorService flushExecutor = Executors.newFixedThreadPool(1);
            Consumer<DynamicMmapStore.FlushedInfo> metadataUpdater =
                    result -> {
                        if (result.success()) {
                            log.info(
                                    "Meta updated: Instance {} flushed to {}",
                                    result.instanceId(),
                                    result.hdfsPath());
                        }
                    };

            String walHdfsPath = config.get(RawWiseOptions.STORAGE_WAL_HDFS_PATH);
            StorageManager storageManager = new StorageManager(basePath, walHdfsPath, config);

            NativeMemorySource nativeMemorySource = NativeMemorySource.create(config);
            ContainerMemoryGuard containerMemoryGuard =
                    new ContainerMemoryGuard(nativeMemorySource, config);

            long totalMemoryLimitBytes = nativeMemorySource.getMemoryLimitBytes();
            MemoryBudget memoryBudget =
                    new MemoryBudget(totalMemoryLimitBytes, instanceMappedBytes);

            infrastructure =
                    new StoreInfrastructure(
                            monitorService,
                            flushExecutor,
                            metadataUpdater,
                            hadoopConf,
                            storeState,
                            storageManager);

            InstancePool instancePool =
                    new InstancePool(
                            memoryBudget,
                            new InstancePool.FlushTrigger() {
                                @Override
                                public void triggerFlushAsync() {
                                    requireBound(storeRef, "triggerFlushAsync").triggerFlushAsync();
                                }

                                @Override
                                public boolean triggerFlushSync() {
                                    return requireBound(storeRef, "triggerFlushSync")
                                            .triggerFlushSync();
                                }
                            },
                            tid -> requireBound(storeRef, "tableStore").tableStore(tid),
                            basePath,
                            writableNum,
                            instanceMappedBytes,
                            maxDbs,
                            containerMemoryGuard);

            RecoveryManager recoveryManager =
                    createRecoveryManager(
                            storageManager,
                            basePath,
                            instanceMappedBytes,
                            hadoopConf,
                            walComponents,
                            writableNum,
                            tid -> requireBound(storeRef, "tableStore").tableStore(tid),
                            instancePool,
                            memoryBudget,
                            storeRef,
                            config);

            WalWriteAdapter walWriteAdapter =
                    new WalWriteAdapter(
                            walComponents.wal(), walComponents.durabilityLevel(), walFailedCount);

            int maxPendingBatchSize = config.get(RawWiseOptions.MAX_PENDING_BATCH_SIZE);
            long maxPendingBatchBytes = config.get(RawWiseOptions.MAX_PENDING_BATCH_BYTES);
            int maxPendingInstances = config.get(RawWiseOptions.MAX_PENDING_INSTANCES);
            double lmdbUsageThreshold = config.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD);

            writePathConfig =
                    new WritePathConfig(
                            maxPendingBatchSize,
                            maxPendingBatchBytes,
                            maxPendingInstances,
                            lmdbUsageThreshold);
            WritePathExecutor writePathExecutor =
                    new WritePathExecutor(
                            instancePool,
                            walWriteAdapter,
                            memoryBudget,
                            totalRecords,
                            writePathConfig,
                            instanceMappedBytes,
                            () -> requireBound(storeRef, "triggerFlushSync").triggerFlushSync(),
                            tid -> requireBound(storeRef, "tableStore").tableStore(tid),
                            (tableId, key) -> {
                                DynamicMmapStore store = storeRef.get();
                                if (store != null) {
                                    store.getRowCache().invalidate(tableId, key);
                                }
                            },
                            config.get(RawWiseOptions.STORAGE_WRITE_TIMEOUT_MS));

            FlushCoordinator flushCoordinator =
                    new DefaultFlushCoordinator(
                            tid -> requireBound(storeRef, "tableStore").tableStore(tid));

            ParquetHdfsUploader parquetHdfsUploader =
                    new ParquetHdfsUploader(
                            walHdfsPath != null && !walHdfsPath.isBlank()
                                    ? walHdfsPath.replaceFirst("/wal$", "/data")
                                    : "",
                            hadoopConf);

            DeletionManager deletionManager =
                    new DeletionManager(memoryBudget::release, instanceMappedBytes);

            components =
                    new StoreComponents(
                            instancePool,
                            recoveryManager,
                            walWriteAdapter,
                            writePathExecutor,
                            deletionManager,
                            flushCoordinator,
                            parquetHdfsUploader);

            DynamicMmapStore store =
                    new DynamicMmapStore(
                            basePath,
                            memoryBudget,
                            writableNum,
                            remoteBasePath,
                            instanceKey,
                            components,
                            walComponents,
                            writePathConfig,
                            infrastructure);

            storeRef.set(store);
            store.start();

            walComponents = null;
            infrastructure = null;
            components = null;

            return store;
        } catch (Exception e) {
            closeQuietly(components);
            closeQuietly(walComponents);
            closeQuietly(infrastructure);
            throw new RuntimeException("Failed to create DynamicMmapStore for " + instanceKey, e);
        }
    }

    static void removeInstance(String instanceKey, DynamicMmapStore store) {
        STORE_INSTANCES.remove(instanceKey, store);
    }

    private static DynamicMmapStore requireBound(
            AtomicReference<DynamicMmapStore> ref, String callbackName) {
        DynamicMmapStore store = ref.get();
        if (store == null) {
            throw new IllegalStateException(
                    "Deferred reference not bound: "
                            + callbackName
                            + " called before store initialization.");
        }
        if (store.isClosed()) {
            throw new IllegalStateException("DynamicMmapStore already closed: " + callbackName);
        }
        return store;
    }

    private static WalComponents createWalComponents(
            String basePath, RawWiseConfig config, Configuration hadoopConf) {
        String durabilityLevelStr = config.get(RawWiseOptions.STORAGE_DURABILITY_LEVEL);
        String walHdfsPath = config.get(RawWiseOptions.STORAGE_WAL_HDFS_PATH);

        DurabilityLevel durabilityLevel;
        if (durabilityLevelStr == null || durabilityLevelStr.isBlank()) {
            durabilityLevel = DurabilityLevel.PROCESS;
        } else {
            durabilityLevel = DurabilityLevel.valueOf(durabilityLevelStr.toUpperCase());
        }

        if (durabilityLevel.isWalRequired()) {
            if (walHdfsPath == null || walHdfsPath.isBlank()) {
                throw new IllegalStateException(
                        "WAL requires HDFS configuration (STORAGE_WAL_HDFS_PATH). "
                                + "If HDFS is not available, set STORAGE_DURABILITY_LEVEL=PROCESS to use LMDB-only persistence.");
            }
            String compressionCodec = config.get(RawWiseOptions.STORAGE_WAL_COMPRESSION_CODEC);
            String storeId = config.get(RawWiseOptions.STORAGE_STORE_ID);
            String executorId = resolveExecutorId();
            String fileFormat = config.get(RawWiseOptions.STORAGE_WAL_FILE_FORMAT);
            // WAL group-commit + sync mode can be overridden via system property (default: sync
            // write, no group-commit)
            // Enabling group-commit + async write can significantly reduce write p99, but OS crash
            // may lose writes within 100ms
            boolean walSyncOnWrite =
                    !"false"
                            .equalsIgnoreCase(
                                    System.getProperty("rawwise.wal.sync-on-write", "true"));
            boolean walGroupCommit =
                    "true"
                            .equalsIgnoreCase(
                                    System.getProperty(
                                            "rawwise.wal.group-commit.enabled", "false"));
            int walGroupCommitBatchSize =
                    Integer.parseInt(
                            System.getProperty("rawwise.wal.group-commit.batch-size", "1000"));
            long walGroupCommitFlushIntervalMs =
                    Long.parseLong(
                            System.getProperty(
                                    "rawwise.wal.group-commit.flush-interval-ms", "100"));
            WALConfig walConfig =
                    new WALConfig(
                            basePath + "/wal",
                            "wal",
                            config.get(RawWiseOptions.STORAGE_WAL_MAX_SIZE_BYTES),
                            walSyncOnWrite,
                            config.get(RawWiseOptions.STORAGE_WAL_BUFFER_SIZE),
                            compressionCodec != null ? compressionCodec : "NONE",
                            storeId != null ? storeId : "default",
                            executorId,
                            walHdfsPath,
                            config.get(RawWiseOptions.STORAGE_WAL_SYNC_INTERVAL_MS),
                            30000L,
                            walGroupCommit,
                            walGroupCommitBatchSize,
                            walGroupCommitFlushIntervalMs);
            WAL wal = new WAL(walConfig, fileFormat, hadoopConf);
            log.info(
                    "WAL enabled with durability=NODE, format={}, bufferSize={}, maxSize={}, hdfs={}, syncOnWrite={}, groupCommit={}",
                    fileFormat,
                    walConfig.bufferSize(),
                    walConfig.segmentSize(),
                    walConfig.hdfsBasePath(),
                    walSyncOnWrite,
                    walGroupCommit);
            return new WalComponents(wal, walConfig, durabilityLevel);
        } else {
            log.info("WAL disabled (durability level=PROCESS). Data is persisted in LMDB only.");
            return new WalComponents(null, null, durabilityLevel);
        }
    }

    private static RecoveryManager createRecoveryManager(
            StorageManager storageManager,
            String basePath,
            long instanceMappedBytes,
            Configuration hadoopConf,
            WalComponents walComponents,
            int writableNum,
            java.util.function.Function<String, io.github.rawwise.engine.storage.table.TableStore>
                    tableStoreFactory,
            InstancePool instancePool,
            MemoryBudget memoryBudget,
            AtomicReference<DynamicMmapStore> storeRef,
            RawWiseConfig config) {
        boolean failOnError = config.get(RawWiseOptions.STORAGE_RECOVERY_FAIL_ON_ERROR);
        RecoveryManager.RecoveryContext recoveryCtx =
                RecoveryManager.RecoveryContext.builder()
                        .storageManager(storageManager)
                        .basePath(basePath)
                        .instanceMappedBytes(instanceMappedBytes)
                        .hadoopConf(hadoopConf)
                        .walConfig(walComponents.walConfig())
                        .durabilityLevel(walComponents.durabilityLevel())
                        .failOnError(failOnError)
                        .writableNumSupplier(() -> writableNum)
                        .tableStoreFactory(tableStoreFactory)
                        .writableInstanceRegistrar(
                                (tid, inst) ->
                                        tableStoreFactory.apply(tid).addWritableInstance(inst))
                        .readOnlyInstanceRegistrar(
                                (tid, inst) ->
                                        tableStoreFactory.apply(tid).addReadOnlyInstance(inst))
                        .memoryTracker((used, ignored) -> memoryBudget.allocate(used))
                        .currentWritableProvider(
                                tid -> tableStoreFactory.apply(tid).getCurrentWritableInstance())
                        .instanceRegistrar(
                                (instId, cf) -> {
                                    String tableId = RecoveryManager.parseTableIdentifier(instId);
                                    instancePool.registerInstance(instId, tableId);
                                })
                        .newInstanceCreator(
                                (tid, mappedBytes) -> {
                                    if (!memoryBudget.canAllocateOneInstance()) return null;
                                    String instanceId = LmdbInstance.generateInstanceId(tid);
                                    return new LmdbInstance(
                                            instanceId,
                                            java.nio.file.Paths.get(basePath).resolve(tid),
                                            mappedBytes);
                                })
                        .memoryAvailableChecker(memoryBudget::canAllocateOneInstance)
                        .syncFlushTrigger(
                                () -> requireBound(storeRef, "triggerFlushSync").triggerFlushSync())
                        .build();
        return new RecoveryManager(recoveryCtx);
    }

    private static long resolveInstanceMappedBytes(RawWiseConfig config) {
        long initialMb = config.get(RawWiseOptions.STORAGE_LMDB_INITIAL_MAP_SIZE_MB);
        return initialMb * 1024L * 1024L;
    }

    private static int resolveWritableNum(RawWiseConfig config) {
        int configured = config.get(RawWiseOptions.STORAGE_WRITABLE_INSTANCES);
        if (configured > 0) {
            return configured;
        }
        return Math.min(Runtime.getRuntime().availableProcessors(), 4);
    }

    private static String resolveExecutorId() {
        try {
            TaskContext taskContext = TaskContext.get();
            if (taskContext != null) {
                String executorId = taskContext.getLocalProperty("executorId");
                if (executorId != null && !executorId.isBlank()) {
                    return "executor-" + executorId;
                }
            }
        } catch (Exception e) {
            log.debug("Failed to get executorId from TaskContext, using fallback", e);
        }
        return "executor-unknown-" + ProcessHandle.current().pid();
    }

    static void closeQuietly(StoreComponents components) {
        if (components == null) return;
        closeQuietly(components.deletionManager());
        closeQuietly(components.parquetHdfsUploader());
        closeQuietly(components.flushCoordinator());
        closeQuietly(components.writePathExecutor());
        closeQuietly(components.walWriteAdapter());
        closeQuietly(components.recoveryManager());
        closeQuietly(components.instancePool());
    }

    static void closeQuietly(WalComponents walComponents) {
        if (walComponents == null) return;
        closeQuietly(walComponents.wal());
    }

    static void closeQuietly(StoreInfrastructure infrastructure) {
        if (infrastructure == null) return;
        shutdownExecutor(infrastructure.flushExecutor());
        shutdownExecutor(infrastructure.monitorService());
        closeQuietly(infrastructure.storageManager());
    }

    static void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                log.warn("Failed to close resource", e);
            }
        }
    }

    static void closeQuietly(WAL wal) {
        if (wal != null) {
            try {
                wal.close();
            } catch (Exception e) {
                log.warn("Failed to close WAL", e);
            }
        }
    }

    static void closeQuietly(ParquetHdfsUploader uploader) {
        if (uploader != null) {
            try {
                uploader.close();
            } catch (Exception e) {
                log.warn("Failed to close ParquetHdfsUploader", e);
            }
        }
    }

    static void closeQuietly(StorageManager storageManager) {
        if (storageManager != null) {
            try {
                storageManager.close();
            } catch (Exception e) {
                log.warn("Failed to close StorageManager", e);
            }
        }
    }

    static void closeQuietly(InstancePool instancePool) {
        if (instancePool != null) {
            try {
                instancePool.stop();
            } catch (Exception e) {
                log.warn("Failed to stop InstancePool", e);
            }
        }
    }

    static void closeQuietly(RecoveryManager recoveryManager) {}

    static void closeQuietly(WalWriteAdapter walWriteAdapter) {}

    static void closeQuietly(WritePathExecutor writePathExecutor) {}

    static void closeQuietly(FlushCoordinator flushCoordinator) {}

    private static void shutdownExecutor(ExecutorService executor) {
        if (executor == null) return;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Closes all registered DynamicMmapStore instances. Should be called during JVM shutdown to
     * flush vector hotBuffers to disk and release LMDB file locks.
     */
    public static void closeAll() {
        for (var entry : STORE_INSTANCES.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                log.warn(
                        "Failed to close DynamicMmapStore for key {}: {}",
                        entry.getKey(),
                        e.getMessage(),
                        e);
            }
        }
        STORE_INSTANCES.clear();
    }
}
