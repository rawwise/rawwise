package io.github.rawwise.engine.storage;

import io.github.rawwise.axiom.pipeline.AxiomEngineRegistry;
import io.github.rawwise.axiom.pipeline.StorageWriteFacade;
import io.github.rawwise.axiom.pipeline.cognition.CognitionEngine;
import io.github.rawwise.axiom.pipeline.derivation.DerivationEngine;
import io.github.rawwise.axiom.pipeline.derivation.KeywordIndex;
import io.github.rawwise.axiom.pipeline.entity.EntityDerivationStage;
import io.github.rawwise.axiom.pipeline.evaluation.EvaluationEngine;
import io.github.rawwise.axiom.pipeline.feedback.FeedbackLoopCloser;
import io.github.rawwise.axiom.pipeline.ingestion.IngestionEngine;
import io.github.rawwise.backend.routing.ShardRecoveryRecord;
import io.github.rawwise.backend.wal.WAL;
import io.github.rawwise.backend.wal.WalComponents;
import io.github.rawwise.common.Lazy;
import io.github.rawwise.common.catalog.Keys;
import io.github.rawwise.common.spi.TxnStoreBridge;
import io.github.rawwise.engine.compaction.CompactionManager;
import io.github.rawwise.engine.compaction.merger.ImmutableAwareMergeFunction;
import io.github.rawwise.engine.compaction.merger.MergeFunction;
import io.github.rawwise.engine.compaction.strategy.CompactionStrategy;
import io.github.rawwise.engine.compaction.strategy.TieredStrategy;
import io.github.rawwise.engine.storage.cache.RowCache;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.flush.FlushCoordinator;
import io.github.rawwise.engine.storage.flush.FlushPlan;
import io.github.rawwise.engine.storage.flush.ParquetHdfsUploader;
import io.github.rawwise.engine.storage.ingestion.IngestionDataSink;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.storage.mvcc.LmdbVersionChainStore;
import io.github.rawwise.engine.storage.mvcc.VersionChainCodec;
import io.github.rawwise.engine.storage.recovery.StoreRecoveryService;
import io.github.rawwise.engine.storage.table.StoreState;
import io.github.rawwise.engine.storage.table.TableStorage;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.txn.DefaultTxnStoreBridge;
import io.github.rawwise.engine.storage.write.LmdbWritePathCoordinator;
import io.github.rawwise.engine.storage.write.SinkWritePathCoordinator;
import io.github.rawwise.engine.storage.write.WritePathCoordinator;
import io.github.rawwise.engine.txn.RecordHeaderProbe;
import io.github.rawwise.engine.txn.ShardVersionTracker;
import io.github.rawwise.engine.txn.SimpleTableRef;
import io.github.rawwise.engine.txn.TxnReadCache;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnContextHolder;
import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.VectorColdStore;
import io.github.rawwise.engine.vector.VectorFlushListener;
import io.github.rawwise.engine.vector.VectorIndex;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.model.op.*;
import io.github.rawwise.model.record.DefaultBody;
import io.github.rawwise.model.record.DefaultRecord;
import io.github.rawwise.model.record.ImmutabilitySource;
import io.github.rawwise.model.record.PrimaryKey;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.Records;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.SemanticRecord;
import io.github.rawwise.model.record.TableId;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

@Slf4j
public class DynamicMmapStore implements InstancePool.FlushTrigger, StorageWriteFacade {
    public static class RecoverableWriteException extends RuntimeException {
        public RecoverableWriteException(String message) {
            super(message);
        }

        public RecoverableWriteException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private final String basePath;
    private final MemoryBudget memoryBudget;
    private final int writableNum;
    private final int embeddedPrecreateNum;
    private final String remoteBasePath;
    private final String instanceKey;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    @Getter private volatile boolean started;

    private final ConcurrentHashMap<String, TableStore> tableStores = new ConcurrentHashMap<>();

    private final ThreadLocal<Boolean> inPreCreate = ThreadLocal.withInitial(() -> Boolean.FALSE);

    private final InstancePool instancePool;
    private final WritePathExecutor writePathExecutor;
    private final ConcurrentHashMap<String, ShardVersionTracker> versionTrackers =
            new ConcurrentHashMap<>();
    private final Lazy<TxnStoreBridge> txnBridge = new Lazy<>(this::createDefaultTxnBridge);
    @Getter private final WAL wal;

    private final StoreComponents components;
    private final WalComponents walComponents;
    private final WritePathConfig writePathConfig;
    private final StoreInfrastructure infrastructure;

    @Getter private final RowCache rowCache;

    @Setter @Getter private IngestionDataSink ingestionDataSink;

    @Getter private volatile LmdbVersionChainStore versionChainStore;

    @Getter private volatile SnapshotManager snapshotManager;

    @Getter private volatile Metastore metastore;
    @Setter private volatile TxnStoreBridge txnStoreBridge;

    public void setMetastore(Metastore metastore) {
        this.metastore = metastore;
        if (components != null) {
            if (components.deletionManager() != null) {
                components.deletionManager().setMetastore(metastore);
                components.deletionManager().setBasePath(basePath);
                components.deletionManager().recoverPendingDeletions();
            }
            if (components.recoveryManager() != null) {
                components.recoveryManager().setMetastore(metastore);
            }
        }
        if (metastore != null && instanceGarbageCollector == null) {
            instanceGarbageCollector =
                    new InstanceGarbageCollector(basePath, metastore, getSnapshotManager());
            instanceGarbageCollector.start();
        }
    }

    private InstanceGarbageCollector instanceGarbageCollector;

    private final AxiomEngineRegistry axiomRegistry;

    private CompactionManager compactionManager;

    private final ImmutablePromotionService immutablePromotionService;
    private final AlterTableAiNativeHandler alterTableAiNativeHandler;
    private final StoreRecoveryService storeRecoveryService;
    private final WritePathCoordinator writePathCoordinator;

    public static DynamicMmapStore getInstance(String basePath, String id) {
        return DynamicMmapStoreFactory.getInstance(basePath, id);
    }

    public static DynamicMmapStore getInstance(
            String basePath, String id, Configuration hadoopConf) {
        return DynamicMmapStoreFactory.getInstance(basePath, id, hadoopConf);
    }

    public static DynamicMmapStore getInstance(String basePath, String id, RawWiseConfig config) {
        return DynamicMmapStoreFactory.getInstance(basePath, id, config);
    }

    public static DynamicMmapStore getInstance(
            String basePath, String id, Configuration hadoopConf, RawWiseConfig config) {
        return DynamicMmapStoreFactory.getInstance(basePath, id, hadoopConf, config);
    }

    DynamicMmapStore(
            String basePath,
            MemoryBudget memoryBudget,
            int writableNum,
            String remoteBasePath,
            String instanceKey,
            StoreComponents components,
            WalComponents walComponents,
            WritePathConfig writePathConfig,
            StoreInfrastructure infrastructure) {
        this.basePath = basePath;
        this.memoryBudget = memoryBudget;
        this.writableNum = writableNum;
        this.embeddedPrecreateNum = resolveEmbeddedPrecreateNum();
        this.remoteBasePath = remoteBasePath;
        this.instanceKey = instanceKey;

        this.components = components;
        this.walComponents = walComponents;
        this.writePathConfig = writePathConfig;
        this.infrastructure = infrastructure;

        this.instancePool = components.instancePool();
        this.writePathExecutor = components.writePathExecutor();
        this.wal = walComponents.wal();

        this.rowCache = new RowCache(1024, 60_000L, true);

        this.immutablePromotionService = new ImmutablePromotionService(this);
        this.alterTableAiNativeHandler =
                new AlterTableAiNativeHandler(this, immutablePromotionService);

        this.storeRecoveryService =
                new StoreRecoveryService(
                        components.recoveryManager(),
                        () -> this.snapshotManager,
                        this::tableStore,
                        memoryBudget,
                        () -> this.triggerFlushSync());

        String sinkType = System.getProperty("rawwise.ingestion.sink-type", "LMDB");
        if ("PARQUET".equalsIgnoreCase(sinkType)) {
            log.info("WritePathCoordinator: using SinkWritePathCoordinator (Parquet mode)");
            this.writePathCoordinator = new SinkWritePathCoordinator(this.ingestionDataSink);
        } else {
            this.writePathCoordinator = new LmdbWritePathCoordinator(this::checkLmdbWritable);
        }

        this.axiomRegistry =
                new AxiomEngineRegistry(
                        basePath, this, this::getMetastore, immutablePromotionService);

        log.info(
                "Backend DynamicMmapStore initialized. SnapshotManager is managed by the frontend Catalog.");
    }

    public boolean isClosed() {
        return closed.get();
    }

    public void start() {
        instancePool.start(infrastructure.monitorService());
        components
                .recoveryManager()
                .loadExistInstance(
                        memoryBudget::canAllocateOneInstance,
                        this::triggerFlushSync,
                        this::tableStore);

        for (String tableId : tableStores.keySet()) {
            components.recoveryManager().cleanupPendingInstances(tableId);
        }

        started = true;

        // Periodically flush writable instances to cold storage for all tables.
        // Covers instances added via addWritableInstance() (benchmark/local path)
        // that bypass the InstancePool-managed lifecycle.
        infrastructure
                .monitorService()
                .scheduleWithFixedDelay(
                        () -> {
                            if (closed.get()) return;
                            for (String tid : tableStores.keySet()) {
                                try {
                                    flushAllWritable(tid);
                                } catch (Exception e) {
                                    log.debug(
                                            "Periodic flush check for {} skipped: {}",
                                            tid,
                                            e.getMessage());
                                }
                            }
                        },
                        60,
                        60,
                        java.util.concurrent.TimeUnit.SECONDS);
    }

    public StoreState currentState() {
        return infrastructure.storeState();
    }

    public InstancePool instancePool() {
        return instancePool;
    }

    public TableStore tableStore(String tableIdentifier) {
        if (closed.get())
            throw new IllegalStateException(
                    "DynamicMmapStore already closed, cannot access tableStore for "
                            + tableIdentifier);
        TableStore ts =
                tableStores.computeIfAbsent(tableIdentifier, id -> new TableStore(id, writableNum));
        preCreateWritableInstances(tableIdentifier, ts);
        return ts;
    }

    public TableStore tableStore(String tableIdentifier, PrimaryKey primaryKey) {
        if (closed.get())
            throw new IllegalStateException(
                    "DynamicMmapStore already closed, cannot access tableStore for "
                            + tableIdentifier);
        TableStore ts =
                tableStores.computeIfAbsent(
                        tableIdentifier, id -> new TableStore(id, writableNum, primaryKey));
        preCreateWritableInstances(tableIdentifier, ts);
        return ts;
    }

    public TableStore tableStore(String tableIdentifier, List<String> primaryKeyFields) {
        if (closed.get())
            throw new IllegalStateException(
                    "DynamicMmapStore already closed, cannot access tableStore for "
                            + tableIdentifier);
        TableStore ts =
                tableStores.computeIfAbsent(
                        tableIdentifier, id -> new TableStore(id, writableNum, primaryKeyFields));
        preCreateWritableInstances(tableIdentifier, ts);
        return ts;
    }

    public TableStore tableStore(
            String tableIdentifier,
            List<String> primaryKeyFields,
            java.util.function.Supplier<io.github.rawwise.model.record.RecordCodec.SchemaResolver>
                    schemaResolverSupplier) {
        if (closed.get())
            throw new IllegalStateException(
                    "DynamicMmapStore already closed, cannot access tableStore for "
                            + tableIdentifier);
        TableStore ts =
                tableStores.computeIfAbsent(
                        tableIdentifier,
                        id ->
                                new TableStore(
                                        id,
                                        writableNum,
                                        TableStore.createKeyCodec(primaryKeyFields),
                                        schemaResolverSupplier));
        if (schemaResolverSupplier != null) {
            ts.setSchemaResolverSupplier(schemaResolverSupplier);
        }
        preCreateWritableInstances(tableIdentifier, ts);
        return ts;
    }

    private static int resolveEmbeddedPrecreateNum() {
        if (!"embedded".equals(System.getProperty("rawwise.deploy.mode"))) return 1;
        String val = System.getProperty("rawwise.storage.embedded.writable-instances");
        if (val == null || val.isBlank()) return 1;
        try {
            int n = Integer.parseInt(val.trim());
            return Math.max(n, 1);
        } catch (NumberFormatException e) {
            log.warn(
                    "Invalid rawwise.storage.embedded.writable-instances value: {}, fallback to 1",
                    val);
            return 1;
        }
    }

    private void preCreateWritableInstances(String tableIdentifier, TableStore store) {
        if (embeddedPrecreateNum <= 1) return;
        if (inPreCreate.get()) return;
        if (store.isWritable()) return;
        inPreCreate.set(Boolean.TRUE);
        try {
            int upperBound = this.writableNum;
            int actual = Math.min(embeddedPrecreateNum, upperBound);
            if (actual < embeddedPrecreateNum) {
                log.warn(
                        "Table[{}] precreate {} capped by writableNum {}",
                        tableIdentifier,
                        embeddedPrecreateNum,
                        upperBound);
            }
            int created = 0;
            for (int i = 0; i < actual; i++) {
                try {
                    instancePool.createNewInstance(tableIdentifier);
                    created++;
                } catch (Exception e) {
                    log.warn(
                            "Table[{}] precreate instance {} failed: {}",
                            tableIdentifier,
                            i,
                            e.getMessage());
                    break;
                }
            }
            log.info(
                    "Table[{}] precreated {} writable instances (target={}, actual={})",
                    tableIdentifier,
                    created,
                    embeddedPrecreateNum,
                    actual);
        } finally {
            inPreCreate.remove();
        }
    }

    public void setTableAiNative(String tableIdentifier, boolean aiNative) {
        tableStore(tableIdentifier).setAiNative(aiNative);
    }

    public boolean isTableAiNative(String tableIdentifier) {
        return tableStore(tableIdentifier).isAiNative();
    }

    public void setTableChunkingStrategy(String tableIdentifier, String strategy) {
        tableStore(tableIdentifier).setChunkingStrategy(strategy);
    }

    public String getTableChunkingStrategy(String tableIdentifier) {
        return tableStore(tableIdentifier).getChunkingStrategy();
    }

    public void setTableSemanticPattern(String tableIdentifier, String pattern) {
        tableStore(tableIdentifier).setSemanticPattern(pattern);
    }

    public String getTableSemanticPattern(String tableIdentifier) {
        return tableStore(tableIdentifier).getSemanticPattern();
    }

    /**
     * 判断表是否已在 Metastore 注册（存在 db:table: TableMeta key，即执行过 CREATE TABLE）。 ingest 写入前用它做
     * fail-fast，避免向未注册表静默写入（schema 退化为通用 binary body、meta 进程重启即失）。
     */
    public boolean isTableRegistered(String tableIdentifier) {
        try {
            io.github.rawwise.metastore.Metastore ms = getMetastore();
            if (ms == null) return false;
            String[] parts = tableIdentifier.split("\\.", 2);
            if (parts.length < 2) return false;
            byte[] metaKey =
                    (parts[0] + ":" + parts[1] + ":")
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            return ms.get(metaKey).isPresent();
        } catch (Exception e) {
            log.warn(
                    "isTableRegistered check failed for {}: {}",
                    tableIdentifier,
                    e.getMessage(),
                    e);
            return false;
        }
    }

    public void applyTablePropertiesFromMetastore(String tableIdentifier) {
        TableStore ts = tableStore(tableIdentifier);
        if (ts.getPropertiesAppliedFromMeta()) return;
        try {
            io.github.rawwise.metastore.Metastore ms = getMetastore();
            if (ms == null) return;
            String[] parts = tableIdentifier.split("\\.", 2);
            if (parts.length < 2) return;
            byte[] metaKey =
                    (parts[0] + ":" + parts[1] + ":")
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            java.util.Optional<byte[]> raw = ms.get(metaKey);
            if (raw.isEmpty()) return;
            io.github.rawwise.meta.PbTableMetadata pbMeta =
                    io.github.rawwise.meta.PbTableMetadata.parseFrom(raw.get());
            Map<String, String> props = pbMeta.getPropertiesMap();
            String chunkingStrategy = props.get("chunking.strategy");
            if (chunkingStrategy != null && !chunkingStrategy.isBlank()) {
                ts.setChunkingStrategy(chunkingStrategy);
            }
            String semanticPattern = props.get("semantic.pattern");
            if (semanticPattern != null && !semanticPattern.isBlank()) {
                ts.setSemanticPattern(semanticPattern);
            }
            String derivationStrategy = props.get("derivation.strategy");
            if (derivationStrategy != null && !derivationStrategy.isBlank()) {
                ts.setDerivationStrategy(
                        switch (derivationStrategy.toLowerCase()) {
                            case "manual" -> TableStore.DerivationStrategy.MANUAL;
                            case "none" -> TableStore.DerivationStrategy.NONE;
                            default -> TableStore.DerivationStrategy.AUTO;
                        });
            }
            ts.setPropertiesAppliedFromMeta(true);
            log.info(
                    "Applied table properties from Metastore: table={} chunking={} semantic.pattern.len={} derivation={}",
                    tableIdentifier,
                    chunkingStrategy,
                    semanticPattern != null ? semanticPattern.length() : 0,
                    derivationStrategy);
        } catch (Exception e) {
            log.warn(
                    "Failed to apply table properties from Metastore for table={}: {}",
                    tableIdentifier,
                    e.getMessage());
        }
    }

    public ImmutablePromotionService immutablePromotionService() {
        return immutablePromotionService;
    }

    public AlterTableAiNativeHandler alterTableAiNativeHandler() {
        return alterTableAiNativeHandler;
    }

    public Set<String> getAllTableIdentifiers() {
        return tableStores.keySet();
    }

    @Override
    public Collection<TableStore> allTableStores() {
        return tableStores.values();
    }

    public String findTableIdentifierByNumericId(long numericTableId) {
        for (var entry : tableStores.entrySet()) {
            TableStore ts = entry.getValue();
            if (ts != null && ts.getNumericTableId() == numericTableId) {
                return entry.getKey();
            }
        }
        return null;
    }

    public boolean updateRecordHeader(
            String tableIdentifier, Record.Key key, Record.Header newHeader) {
        TableStore ts = tableStore(tableIdentifier);
        byte[] encodedKey = key.encoded();
        byte[] rawValue = ts.getRawValue(encodedKey);
        if (rawValue == null) {
            log.warn(
                    "updateRecordHeader: record not found for table={}, key={}",
                    tableIdentifier,
                    key);
            return false;
        }
        try {
            if (NullableValueCodec.isTombstone(rawValue)) {
                log.debug(
                        "updateRecordHeader: record is tombstone for table={}, key={}",
                        tableIdentifier,
                        key);
                return false;
            }
            byte[] decoded = NullableValueCodec.decode(rawValue);
            if (decoded == null) {
                log.warn(
                        "updateRecordHeader: null decoded value for table={}, key={}",
                        tableIdentifier,
                        key);
                return false;
            }
            io.github.rawwise.model.record.RecordCodec recordCodec = ts.getRecordCodec();
            if (!recordCodec.isRecordCodecFormat(decoded)) {
                log.warn(
                        "updateRecordHeader: value not in RecordCodec format for table={}",
                        tableIdentifier);
                return false;
            }
            io.github.rawwise.model.record.RecordCodec.SchemaResolver schemaResolver =
                    ts.getSchemaResolver();
            Record record = recordCodec.decode(decoded, schemaResolver);
            Record updatedRecord = new DefaultRecord(record.key(), record.body(), newHeader);
            byte[] updatedBytes = recordCodec.encode(updatedRecord);
            byte[] wrappedBytes = NullableValueCodec.encode(updatedBytes);
            LmdbInstance instance = ts.getWritableInstance(encodedKey);
            if (instance == null) {
                instance = ts.getCurrentWritableInstance();
            }
            if (instance == null || instance.isReadOnly()) {
                log.warn("updateRecordHeader: no writable instance for table={}", tableIdentifier);
                return false;
            }
            LmdbInstance.PutStatus status = instance.put(encodedKey, wrappedBytes);
            if (status != LmdbInstance.PutStatus.SUCCESS) {
                log.warn(
                        "updateRecordHeader: put failed for table={}, status={}",
                        tableIdentifier,
                        status);
                return false;
            }
            return true;
        } catch (Exception e) {
            log.warn(
                    "updateRecordHeader: failed for table={}: {}", tableIdentifier, e.getMessage());
            return false;
        }
    }

    public ReadExecutor readExecutor() {
        return this::read;
    }

    public ShardVersionTracker getOrCreateVersionTracker(String tableIdentifier) {
        return versionTrackers.computeIfAbsent(
                tableIdentifier,
                tid ->
                        new ShardVersionTracker(
                                (tid.hashCode() & 0x7FFFFFFF) % 1024,
                                new io.github.rawwise.engine.txn.clock.AtomicLongClock(0),
                                RecordHeaderProbe.NOOP));
    }

    private TxnStoreBridge ensureTxnBridge() {
        return txnBridge.get();
    }

    private TxnStoreBridge createDefaultTxnBridge() {
        return new DefaultTxnStoreBridge();
    }

    public void setSnapshotManager(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
        FlushCoordinator fc = components.flushCoordinator();
        if (fc != null) {
            fc.setSnapshotManager(snapshotManager);
        }
        if (started) {
            for (String tableId : tableStores.keySet()) {
                recoverColdBlocksForTable(tableId);
            }
        }
    }

    public void setVectorIndex(VectorIndex vectorIndex, VectorColdStore coldStore) {
        axiomRegistry.setVectorIndex(vectorIndex, coldStore);
        FlushCoordinator fc = components.flushCoordinator();
        if (fc != null && vectorIndex != null) {
            fc.addFlushListener(new VectorFlushListener(vectorIndex));
        }
    }

    public VectorIndex getVectorIndex() {
        return axiomRegistry.getVectorIndex();
    }

    public KeywordIndex getKeywordIndex() {
        return axiomRegistry.getKeywordIndex();
    }

    public EvaluationEngine getEvaluationEngine() {
        return axiomRegistry.getEvaluationEngine();
    }

    public void setEvaluationEngine(EvaluationEngine engine) {
        axiomRegistry.setEvaluationEngine(engine);
    }

    public CognitionEngine getCognitionEngine() {
        return axiomRegistry.getCognitionEngine();
    }

    public void setCognitionEngine(CognitionEngine engine) {
        axiomRegistry.setCognitionEngine(engine);
    }

    public FeedbackLoopCloser getFeedbackLoopCloser() {
        return axiomRegistry.getFeedbackLoopCloser();
    }

    public void setFeedbackLoopCloser(FeedbackLoopCloser closer) {
        axiomRegistry.setFeedbackLoopCloser(closer);
    }

    public EntityDerivationStage getEntityDerivationStage() {
        return axiomRegistry.getEntityDerivationStage();
    }

    public void setEntityDerivationStage(EntityDerivationStage stage) {
        axiomRegistry.setEntityDerivationStage(stage);
    }

    public void initAxiomEngines(RawWiseConfig config) {
        axiomRegistry.initAxiomEngines(config);
    }

    public void registerAutoDeriveHook(String tableIdentifier) {
        axiomRegistry.registerAutoDeriveHook(tableIdentifier);
    }

    public IngestionEngine getIngestionEngine() {
        return axiomRegistry.getIngestionEngine();
    }

    public DerivationEngine getDerivationEngine() {
        return axiomRegistry.getDerivationEngine();
    }

    public EmbeddingProvider getEmbeddingProvider() {
        return axiomRegistry.getEmbeddingProvider();
    }

    public KnowledgeStore knowledgeStore() {
        return axiomRegistry.knowledgeStore();
    }

    public io.github.rawwise.common.spi.TxnStoreBridge getTxnBridge() {
        return txnStoreBridge != null ? txnStoreBridge : ensureTxnBridge();
    }

    public void setCompactionManager(CompactionManager cm) {
        this.compactionManager = cm;
    }

    public CompactionManager.CompactionResult compactTable(String tableId) throws IOException {
        if (compactionManager == null) {
            throw new IllegalStateException("CompactionManager not configured");
        }
        CompactionStrategy strategy = new TieredStrategy();
        MergeFunction mergeFn = new ImmutableAwareMergeFunction();
        return compactionManager.compact(tableId, strategy, mergeFn);
    }

    public TableStorage tableStorage(TableId tableId) {
        return tableStores.values().stream()
                .filter(store -> store.tableId().equals(tableId))
                .findFirst()
                .orElse(null);
    }

    public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        DynamicMmapStoreFactory.removeInstance(instanceKey, this);

        // Drain in-flight async derivation tasks, then close vectorIndex (flushes hotBuffers
        // to disk so they can be recovered on restart). Must happen before tableStores close.
        try {
            axiomRegistry.close();
        } catch (Exception e) {
            log.warn("AxiomRegistry close failed: {}", e.getMessage());
        }

        ScheduledExecutorService monitorService = infrastructure.monitorService();
        ExecutorService flushExecutor = infrastructure.flushExecutor();

        CountDownLatch cleanupLatch = new CountDownLatch(2);
        monitorService.execute(
                () -> {
                    try {
                        LmdbInstance.cleanupCurrentThreadDirectBuffers();
                    } finally {
                        cleanupLatch.countDown();
                    }
                });
        flushExecutor.execute(
                () -> {
                    try {
                        LmdbInstance.cleanupCurrentThreadDirectBuffers();
                    } finally {
                        cleanupLatch.countDown();
                    }
                });
        try {
            cleanupLatch.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        monitorService.shutdown();
        try {
            if (!monitorService.awaitTermination(5, TimeUnit.SECONDS)) {
                monitorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            monitorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        flushExecutor.shutdown();
        try {
            if (!flushExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                flushExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            flushExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        DynamicMmapStoreFactory.closeQuietly(components.deletionManager());

        if (wal != null) {
            DynamicMmapStoreFactory.closeQuietly(wal);
        }

        DynamicMmapStoreFactory.closeQuietly(components.parquetHdfsUploader());

        DynamicMmapStoreFactory.closeQuietly(instancePool);

        rowCache.close();

        if (versionChainStore != null) {
            try {
                versionChainStore.close();
            } catch (Exception e) {
                log.warn("Failed to close versionChainStore", e);
            }
        }

        tableStores.forEach(
                (key, tableStore) -> {
                    try {
                        tableStore.close();
                    } catch (Exception e) {
                        log.warn("Failed to close tableStore {}", key, e);
                    }
                });
    }

    @Override
    public void triggerFlushAsync() {
        if (closed.get()) return;
        Optional<String> coldest = instancePool.coldest();
        if (coldest.isEmpty()) return;
        String instanceId = coldest.get();
        if (!instancePool.claimFlush(instanceId)) return;
        try {
            infrastructure
                    .flushExecutor()
                    .submit(
                            () -> {
                                try {
                                    LmdbInstance instance =
                                            instancePool.getLmdbInstance(instanceId);
                                    if (instance != null) {
                                        flushAndScheduleDeletion(instance);
                                    }
                                } finally {
                                    instancePool.releaseFlushClaim(instanceId);
                                }
                            });
        } catch (java.util.concurrent.RejectedExecutionException e) {
            instancePool.releaseFlushClaim(instanceId);
            log.warn("Flush executor rejected, releasing claim for {}", instanceId);
        }
    }

    @Override
    public boolean triggerFlushSync() {
        if (closed.get()) return false;
        Optional<String> coldest = instancePool.coldest();
        if (coldest.isEmpty()) return false;
        String instanceId = coldest.get();
        if (!instancePool.claimFlush(instanceId)) return false;
        try {
            LmdbInstance instance = instancePool.getLmdbInstance(instanceId);
            if (instance == null) return false;
            flushAndScheduleDeletion(instance);
            return true;
        } finally {
            instancePool.releaseFlushClaim(instanceId);
        }
    }

    /**
     * Flush all writable instances of the given table to Parquet cold storage. Used in local
     * benchmark/testing scenarios where instances bypass InstancePool.
     *
     * @return true if at least one instance was flushed
     */
    public boolean flushAllWritable(String tableIdentifier) {
        if (closed.get()) return false;
        TableStore ts = tableStores.get(tableIdentifier);
        if (ts == null) return false;
        List<LmdbInstance> writable = ts.getWritableInstances();
        if (writable.isEmpty()) return false;
        boolean anyFlushed = false;
        for (LmdbInstance instance : writable) {
            if (instance.readOnly()) continue;
            // Skip auto-flush for instances with very little data to avoid
            // triggering HDFS upload failures that corrupt the instance
            if (instance.getTotalRecords() < 1000) {
                continue;
            }
            try {
                flushAndScheduleDeletion(instance);
                anyFlushed = true;
            } catch (Exception e) {
                log.error(
                        "flushAllWritable failed for instance {}: {}",
                        instance.instanceId(),
                        e.getMessage());
            }
        }
        return anyFlushed;
    }

    private void flushAndScheduleDeletion(LmdbInstance instance) {
        String effectiveRemoteBasePath =
                (remoteBasePath != null && !remoteBasePath.isBlank())
                        ? remoteBasePath
                        : "file://" + basePath + "/cold";

        String tableIdentifier = instancePool.findTableIdentifier(instance.instanceId());
        if (tableIdentifier == null) {
            log.error(
                    "Cannot find table identifier for instance {}, skipping flush",
                    instance.instanceId());
            return;
        }
        FlushCoordinator flushCoordinator = components.flushCoordinator();
        FlushPlan plan = flushCoordinator.plan(tableIdentifier, instance, effectiveRemoteBasePath);
        flushCoordinator.beforeFlush(tableIdentifier, instance);

        try {
            log.info(
                    "Planned flush {} for instance {} to {} (remoteBasePath={}, effective={})",
                    plan.planId(),
                    instance.instanceId(),
                    plan.targetLocation().storagePath().location(),
                    remoteBasePath,
                    effectiveRemoteBasePath);
            flushCoordinator.afterFlush(tableIdentifier, instance, plan);

            ParquetHdfsUploader uploader = components.parquetHdfsUploader();
            boolean hdfsUploaded =
                    uploader.uploadFromLocal(
                            instance.path().resolve(instance.instanceId() + ".parquet"),
                            tableIdentifier,
                            instance.instanceId() + ".parquet");

            if (hdfsUploaded) {
                flushCoordinator.commitFlush(tableIdentifier, instance, plan);
                if (walComponents.durabilityLevel().isWalRequired() && wal != null) {
                    wal.insertSyncMarker(tableIdentifier);
                }
                FlushedInstanceMark mark =
                        new FlushedInstanceMark(
                                tableIdentifier,
                                instance.instanceId(),
                                -1L,
                                System.currentTimeMillis(),
                                plan.targetLocation().storagePath().location(),
                                FlushedInstanceMark.FlushStatus.FLUSHED);
                if (metastore != null) {
                    try {
                        metastore.put(
                                Keys.flushedInstanceKey(tableIdentifier, instance.instanceId()),
                                mark.toBytes());
                    } catch (IOException e) {
                        log.warn(
                                "Failed to persist flushed instance mark for {}",
                                instance.instanceId(),
                                e);
                    }
                }
                components.deletionManager().scheduleDeletion(instance, 30_000, mark);
            } else {
                flushCoordinator.onFlushFailure(
                        tableIdentifier,
                        instance,
                        new IOException(
                                "HDFS upload failed for instance " + instance.instanceId()));
                instance.unmarkReadOnly();
                log.warn(
                        "HDFS upload failed for instance {}, LMDB instance reverted to writable",
                        instance.instanceId());
            }
        } catch (Exception e) {
            flushCoordinator.onFlushFailure(tableIdentifier, instance, e);
            try {
                instance.unmarkReadOnly();
            } catch (Exception unmarkErr) {
                log.warn(
                        "Failed to unmark instance {} as readOnly: {}",
                        instance.instanceId(),
                        unmarkErr.getMessage());
            }
            log.error("Flush failed for instance {}", instance.instanceId(), e);
        }
    }

    public LmdbInstance getOrCreatePendingInstance(String tableIdentifier, TxnContext txnContext) {
        return writePathExecutor.getOrCreatePendingInstance(tableIdentifier, txnContext);
    }

    public LmdbInstance createNewPendingInstance(String tableIdentifier, TxnContext txnContext) {
        return writePathExecutor.createNewPendingInstance(tableIdentifier, txnContext);
    }

    public void commitTailBatch(String tableIdentifier) {
        TxnStoreBridge bridge = ensureTxnBridge();
        if (bridge == null || !bridge.isTransactionActive()) {
            log.debug(
                    "commitTailBatch: bridge not available or no active transaction, table={}",
                    tableIdentifier);
            return;
        }
        TxnContext ctx = resolveTxnContext();
        if (ctx == null) return;

        String[] flushedIds =
                bridge.commitTailBatch(
                        tableIdentifier,
                        tid -> getOrCreatePendingInstance(tid, ctx),
                        tid -> createNewPendingInstance(tid, ctx));
        if (flushedIds != null && flushedIds.length > 0) {
            tableStore(tableIdentifier)
                    .commitPendingInstances(Arrays.asList(flushedIds), ctx.txnId(), ctx.txnId());
        }
    }

    public LmdbInstance currentLmdbInstance(String tableIdentifier, byte[] key) {
        if (key != null) {
            return instancePool.keyBasedLmdbInstance(tableIdentifier, key);
        }
        return instancePool.currentLmdbInstance(tableIdentifier);
    }

    private boolean checkLmdbWritable(String tableIdentifier, byte[] key) {
        LmdbInstance instance = currentLmdbInstance(tableIdentifier, key);
        if (instance == null || instance.isReadOnly()) {
            log.warn(
                    "checkLmdbWritable: no writable LMDB instance for table={} keySize={}",
                    tableIdentifier,
                    key != null ? key.length : 0);
            return false;
        }
        return true;
    }

    public boolean ensureWritable(String tableIdentifier, byte[] key) {
        return writePathCoordinator.ensureWritable(tableIdentifier, key);
    }

    public boolean isLmdbBacked() {
        return writePathCoordinator.isLmdbBacked();
    }

    public boolean switchToNewInstance(String tableIdentifier) {
        return instancePool.switchToNewInstance(tableIdentifier);
    }

    public void write(WriteBuilder builder) {
        write(builder, null);
    }

    public void write(WriteBuilder builder, RoutingOwnerContext ownerContext) {
        String tableIdentifier = builder.table().uniqId();

        TxnContext ctx = resolveTxnContext();
        if (ctx != null) {
            if (ctx.isActive()) {
                TxnStoreBridge bridge = ensureTxnBridge();
                if (bridge.isTransactionActive()) {
                    if (builder instanceof KvWriteBuilder kvBuilder) {
                        byte[] key = kvBuilder.key();
                        if (key != null) {
                            byte[] value = kvBuilder.value();
                            long baseVersion =
                                    getOrCreateVersionTracker(tableIdentifier).getCommitTs(key);
                            boolean recorded;
                            if (kvBuilder.isTombstone()) {
                                recorded =
                                        bridge.tryRecordDelete(key, tableIdentifier, baseVersion);
                            } else if (value == null) {
                                recorded =
                                        bridge.tryRecordDelete(key, tableIdentifier, baseVersion);
                            } else {
                                recorded =
                                        bridge.tryRecordWrite(
                                                key, value, baseVersion, tableIdentifier);
                            }
                            if (recorded) {
                                if (ctx.txnMode().isWriteThrough()
                                        && bridge.isPendingBatchFull(
                                                writePathConfig.maxPendingBatchSize(),
                                                writePathConfig.maxPendingBatchBytes())) {
                                    bridge.flushPendingBatch(
                                            tableIdentifier,
                                            tid -> getOrCreatePendingInstance(tid, ctx),
                                            tid -> createNewPendingInstance(tid, ctx));
                                }
                                return;
                            }
                            log.warn(
                                    "Bridge write recording failed for table={}, "
                                            + "falling through to non-transactional path",
                                    tableIdentifier);
                        }
                    }
                }
            } else if (ctx.isCommitted() || ctx.isAborted()) {
                TxnStoreBridge bridge = ensureTxnBridge();
                String[] tailIds =
                        bridge.commitTailBatch(
                                tableIdentifier,
                                tid -> getOrCreatePendingInstance(tid, ctx),
                                tid -> createNewPendingInstance(tid, ctx));
                if (tailIds != null && tailIds.length > 0) {
                    tableStore(tableIdentifier)
                            .commitPendingInstances(
                                    Arrays.asList(tailIds), ctx.txnId(), ctx.txnId());
                }
                throw new IllegalStateException(
                        "Transaction "
                                + ctx.txnId()
                                + " is in terminal state "
                                + ctx.status()
                                + ", writes are not allowed. This may indicate a missing detach() call.");
            } else {
                throw new IllegalStateException(
                        "Transaction "
                                + ctx.txnId()
                                + " is in state "
                                + ctx.status()
                                + ", writes are not allowed");
            }
        }

        writePathExecutor.execute(builder, tableIdentifier, ownerContext);

        // Trigger all registered PostWriteHooks (secondary index sync, etc.).
        // Approach A: pass builder along, so firePostWriteHooks can reverse decode from builder
        // complete Record to Hook (fixes Hook receiving null record in FastPath INSERT path).
        firePostWriteHooks(tableIdentifier, builder);
    }

    /**
     * Trigger all registered PostWriteHooks after non-transactional write completes.
     *
     * <p>Current implementation only calls {@link PostWriteHook#afterWrite} to notify each hook.
     * Specific row decoding, index encoding, and shadow table writes are handled internally by each
     * hook implementation.
     *
     * <p>Transaction path hook triggering in {@link
     * io.github.rawwise.engine.txn.TxnOrchestratorImpl#doCommit} is handled uniformly (to be
     * integrated after cross-table transaction implementation).
     */
    private void firePostWriteHooks(String tableIdentifier, WriteBuilder builder) {
        TableStore ts = tableStore(tableIdentifier);
        java.util.List<PostWriteHook> hooks = ts.getPostWriteHooks();
        if (hooks.isEmpty()) return;

        // Approach A: FastPath/Spark write paths both hold (key, value) bytes uniformly via
        // KvWriteBuilder,
        // Here uses RecordCodec to reverse decode complete Record for Hook, so AutoDerivationHook
        // can obtain
        // real record body to trigger jieba tokenization, embedding, JVector indexing (AI-Native
        // derivation pipeline).
        // For tombstone / value=null (DELETE), record remains null (hook decides how to handle
        // delete events).
        SemanticRecord decodedRecord = tryDecodeRecord(ts, builder);

        for (PostWriteHook hook : hooks) {
            try {
                hook.afterWrite(decodedRecord, tableIdentifier);
            } catch (Exception e) {
                log.warn(
                        "PostWriteHook {} failed for table {}: {}",
                        hook.getClass().getSimpleName(),
                        tableIdentifier,
                        e.getMessage());
            }
        }
    }

    /**
     * Extract (key, value) from KvWriteBuilder and reverse decode via RecordCodec to complete
     * Record.
     *
     * <p>Supports two byte layouts, consistent with {@link
     * io.github.rawwise.engine.storage.writer.LmdbWriter} packaging rules:
     *
     * <ul>
     *   <li><b>RecordCodec path(after Approach A)</b>: value is raw RecordCodec bytes
     *       (magic="REC"), directly {@code recordCodec.decode(value, schemaResolver)}.
     *   <li><b>Legacy JSON path</b>: value is JSON bytes(first byte '{')-- Record not constructed
     *       currently, returns null (consistent with pre-Approach A Hook null record behavior,
     *       avoids breaking old FastPath compatibility path).
     *   <li><b>Tombstone / DELETE</b>: value=null or builder.isTombstone() as true → return null.
     * </ul>
     *
     * @return decoded Record; returns null if decode fails, is tombstone, or value is JSON
     *     compatibility bytes.
     */
    private SemanticRecord tryDecodeRecord(TableStore ts, WriteBuilder builder) {
        if (!(builder instanceof KvWriteBuilder kv)) return null;
        if (kv.isTombstone()) return null;
        byte[] value = kv.value();
        if (value == null || value.length == 0) return null;

        // Approach A write: value is raw RecordCodec bytes (first byte is RecordCodec magic
        // 'R'=0x52).
        // Use RecordCodec.decode to directly restore Record.
        try {
            io.github.rawwise.model.record.RecordCodec recordCodec = ts.getRecordCodec();
            io.github.rawwise.model.record.RecordCodec.SchemaResolver schemaResolver =
                    ts.getSchemaResolver();
            if (recordCodec == null || schemaResolver == null) {
                return null;
            }
            // RecordCodec.decode expects raw RecordCodec bytes (without NullableValueCodec wrap).
            // KvWriteBuilder the value in KvWriteBuilder has not yet been wrapped by
            // NullableValueCodec before LmdbWriter writes,
            // so it can be decoded directly; isRecordCodecFormat validates magic to prevent misuse
            // of legacy JSON data.
            if (!recordCodec.isRecordCodecFormat(value)) {
                // Legacy JSON path: Record is not reverse-constructed currently (remains null, hook
                // handles it)
                return null;
            }
            return recordCodec.decode(value, schemaResolver);
        } catch (Exception e) {
            log.debug(
                    "tryDecodeRecord failed for table {}: {}",
                    ts.getTableIdentifier(),
                    e.getMessage());
            return null;
        }
    }

    private TxnContext resolveTxnContext() {
        TxnContext txnContext = null;
        if (txnContext == null) {
            txnContext = TxnContextHolder.current();
        }
        return txnContext;
    }

    public void delete(String tableIdentifier, byte[] key) {
        delete(tableIdentifier, key, null);
    }

    public void delete(String tableIdentifier, byte[] key, RoutingOwnerContext ownerContext) {
        if (tableIdentifier == null || tableIdentifier.isBlank()) {
            throw new IllegalArgumentException("tableIdentifier must not be blank");
        }
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        // commitTs ≡ txnId (unified strategy). Fallback to 0
        // when no TxnContext is active (legacy / non-transactional delete path).
        TxnContext ctx = resolveTxnContext();
        long commitTs = ctx != null ? ctx.txnId() : 0L;
        KvWriteBuilder kvBuilder =
                KvWriteBuilder.builder()
                        .table(new SimpleTableRef(tableIdentifier))
                        .key(key)
                        .tombstone(commitTs);
        write(kvBuilder, ownerContext);
    }

    public Read.Result read(Read read) throws IOException {
        if (read.getDataBlocks().isEmpty()) {
            return Scan.Result.error("Read routing requires planned DataBlocks", null);
        }
        String tableIdentifier = read.getDataBlocks().getFirst().tableId();
        TxnContext txnContext = resolveTxnContext();

        if (txnContext != null && read instanceof Find findImpl) {
            Map<String, Comparable> pkValues = findImpl.primaryKey().values();
            if (pkValues != null && !pkValues.isEmpty()) {
                try {
                    TableStore ts = tableStore(tableIdentifier);
                    byte[] key = ts.getKeyCodec().encodePrimaryKey(pkValues);
                    TxnStoreBridge bridge = ensureTxnBridge();
                    byte[] bridgeResult = bridge.tryResolveRead(tableIdentifier, key);
                    if (bridgeResult != null) {
                        ShardVersionTracker tracker = getOrCreateVersionTracker(tableIdentifier);
                        long commitTs = tracker.getCommitTs(key);
                        txnContext.operationSet().recordRead(key, commitTs);
                        if (bridgeResult.length == 0) {
                            return Find.Result.notFound();
                        }
                        try {
                            io.github.rawwise.model.record.RecordCodec recordCodec =
                                    ts.getRecordCodec();
                            io.github.rawwise.model.record.RecordCodec.SchemaResolver
                                    schemaResolver = ts.getSchemaResolver();
                            io.github.rawwise.engine.storage.codec.RecordDecoder.DecodeResult
                                    decodeResult =
                                            io.github.rawwise.engine.storage.codec.RecordDecoder
                                                    .decodeNullableWrapped(
                                                            bridgeResult,
                                                            recordCodec,
                                                            schemaResolver);
                            if (decodeResult
                                    instanceof
                                    io.github.rawwise.engine.storage.codec.RecordDecoder
                                                            .DecodeResult.Found
                                                    f) {
                                return Find.Result.found(f.record(), bridgeResult);
                            }
                            return Find.Result.found(
                                    createBridgeFallbackRecord(key, bridgeResult), bridgeResult);
                        } catch (Exception e) {
                            log.warn(
                                    "Bridge read decode/fallback failed for table {}: {}",
                                    tableIdentifier,
                                    e.getMessage(),
                                    e);
                        }
                    }
                    long snapshotTs = resolveSnapshotTs(txnContext);
                    Optional<byte[]> rowCacheHit = rowCache.get(tableIdentifier, key, snapshotTs);
                    if (rowCacheHit.isPresent()) {
                        byte[] cachedData = rowCacheHit.get();
                        io.github.rawwise.model.record.RecordCodec recordCodec =
                                ts.getRecordCodec();
                        io.github.rawwise.model.record.RecordCodec.SchemaResolver schemaResolver =
                                ts.getSchemaResolver();
                        io.github.rawwise.engine.storage.codec.RecordDecoder.DecodeResult
                                decodeResult =
                                        io.github.rawwise.engine.storage.codec.RecordDecoder
                                                .decodeNullableWrapped(
                                                        cachedData, recordCodec, schemaResolver);
                        if (decodeResult
                                instanceof
                                io.github.rawwise.engine.storage.codec.RecordDecoder.DecodeResult
                                                        .Found
                                                f) {
                            return Find.Result.found(f.record(), cachedData);
                        }
                        return Find.Result.found(
                                createBridgeFallbackRecord(key, cachedData), cachedData);
                    }
                } catch (Exception e) {
                    log.warn(
                            "Transaction read resolution failed for table {}, falling through to LMDB: {}",
                            tableIdentifier,
                            e.getMessage());
                }
            }
        }

        Read.Result result = tableStore(tableIdentifier).read(read);

        if (result instanceof Find.Result fr
                && fr.success()
                && !fr.isFound()
                && read instanceof Find findImpl) {
            Map<String, Comparable> pkValues = findImpl.primaryKey().values();
            if (pkValues != null && !pkValues.isEmpty()) {
                try {
                    TableStore ts = tableStore(tableIdentifier);
                    byte[] pkKey = ts.getKeyCodec().encodePrimaryKey(pkValues);
                    byte[] raw = ts.getRawValue(pkKey);
                    if (raw != null && !NullableValueCodec.isTombstone(raw)) {
                        Record fallback = createBridgeFallbackRecord(pkKey, raw);
                        result = Find.Result.found(fallback, raw);
                    }
                } catch (Exception e) {
                    log.warn(
                            "Raw value fallback failed for table {}: {}",
                            tableIdentifier,
                            e.getMessage());
                }
            }
        }

        if (txnContext != null && result.success()) {
            LmdbVersionChainStore localVcs = this.versionChainStore;
            if (localVcs != null
                    && result instanceof Find.Result fr
                    && fr.success()
                    && fr.isFound()
                    && txnContext.readSnapshot() != null) {
                try {
                    if (fr.isFound() && read instanceof Find findImpl) {
                        Map<String, Comparable> pkValues = findImpl.primaryKey().values();
                        if (pkValues != null && !pkValues.isEmpty()) {
                            TableStore ts = tableStore(tableIdentifier);
                            byte[] key = ts.getKeyCodec().encodePrimaryKey(pkValues);
                            long snapshotTs = resolveSnapshotTs(txnContext);

                            Optional<VersionChainCodec.VersionEntry> vcEntry =
                                    localVcs.getAtSnapshot(tableIdentifier, key, snapshotTs);

                            if (vcEntry.isPresent()) {
                                byte[] vcValue = vcEntry.get().valueCopy();
                                byte[] lmdbValue = fr.rawNullableValue();

                                if (!java.util.Arrays.equals(vcValue, lmdbValue)) {
                                    log.debug(
                                            "VersionChain interceptor: table={} keyLen={} "
                                                    + "snapshotTs={} vcCommitTs={} "
                                                    + "returning historical version",
                                            tableIdentifier,
                                            key.length,
                                            snapshotTs,
                                            vcEntry.get().commitTs());

                                    io.github.rawwise.model.record.RecordCodec recordCodec =
                                            ts.getRecordCodec();
                                    io.github.rawwise.model.record.RecordCodec.SchemaResolver
                                            schemaResolver = ts.getSchemaResolver();
                                    try {
                                        io.github.rawwise.engine.storage.codec.RecordDecoder
                                                        .DecodeResult
                                                decodeResult =
                                                        io.github.rawwise.engine.storage.codec
                                                                .RecordDecoder
                                                                .decodeNullableWrapped(
                                                                        vcValue,
                                                                        recordCodec,
                                                                        schemaResolver);
                                        if (decodeResult
                                                instanceof
                                                io.github.rawwise.engine.storage.codec.RecordDecoder
                                                                        .DecodeResult.Found
                                                                f) {
                                            result = Find.Result.found(f.record(), vcValue);
                                        } else {
                                            result =
                                                    Find.Result.found(
                                                            createBridgeFallbackRecord(
                                                                    key, vcValue),
                                                            vcValue);
                                        }
                                    } catch (Exception decodeEx) {
                                        log.warn(
                                                "VersionChain decode failed for table {}, falling through",
                                                tableIdentifier,
                                                decodeEx);
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    log.warn(
                            "VersionChain interceptor failed for table {}, falling through to LMDB result: {}",
                            tableIdentifier,
                            e.getMessage());
                }
            }

            recordRead(read, tableIdentifier, txnContext, result);

            if (read instanceof Find findImpl) {
                Map<String, Comparable> pkValues = findImpl.primaryKey().values();
                if (pkValues != null && !pkValues.isEmpty()) {
                    try {
                        TableStore ts = tableStore(tableIdentifier);
                        byte[] key = ts.getKeyCodec().encodePrimaryKey(pkValues);
                        byte[] value = null;
                        if (result instanceof Find.Result findResult) {
                            value = findResult.rawNullableValue();
                        }
                        cacheReadResult(tableIdentifier, key, value, txnContext);
                        if (value != null) {
                            long snapshotTs = resolveSnapshotTs(txnContext);
                            ShardVersionTracker tracker =
                                    getOrCreateVersionTracker(tableIdentifier);
                            long commitTs = tracker.getCommitTs(key);
                            if (commitTs > 0) {
                                rowCache.put(tableIdentifier, key, value, commitTs);
                            }
                        }
                    } catch (Exception e) {
                        log.warn(
                                "Failed to cache read result for table {}: {}",
                                tableIdentifier,
                                e.getMessage());
                    }
                }
            }
        }

        return result;
    }

    private static long resolveSnapshotTs(TxnContext txnContext) {
        if (txnContext == null) return Long.MAX_VALUE;
        try {
            io.github.rawwise.engine.txn.isolate.ReadSnapshot snapshot = txnContext.readSnapshot();
            return snapshot != null ? snapshot.snapshotTs() : Long.MAX_VALUE;
        } catch (RuntimeException e) {
            log.warn("Failed to resolve snapshot timestamp, falling back to MAX_VALUE", e);
            return Long.MAX_VALUE;
        }
    }

    private void recordRead(
            Read read, String tableIdentifier, TxnContext txnContext, Read.Result result) {
        try {
            if (!result.success()) return;
            if (read instanceof Find findImpl) {
                Map<String, Comparable> pkValues = findImpl.primaryKey().values();
                if (pkValues != null && !pkValues.isEmpty()) {
                    TableStore ts = tableStore(tableIdentifier);
                    byte[] key = ts.getKeyCodec().encodePrimaryKey(pkValues);
                    ShardVersionTracker tracker = getOrCreateVersionTracker(tableIdentifier);
                    long commitTs = tracker.getCommitTs(key);
                    txnContext.operationSet().recordRead(key, commitTs);
                }
            } else if (read instanceof Scan scan) {
                KeyRange keyRange = scan.keyRange();
                if (keyRange != null && !keyRange.isFullRange()) {
                    long snapshotTs = resolveSnapshotTs(txnContext);
                    txnContext
                            .operationSet()
                            .recordRangeRead(
                                    keyRange.startKey() != null
                                            ? keyRange.startKey().clone()
                                            : null,
                                    keyRange.endKey() != null ? keyRange.endKey().clone() : null,
                                    snapshotTs);
                }
            }
        } catch (Exception e) {
            log.warn("recordRead failed for table {}: {}", tableIdentifier, e.getMessage());
        }
    }

    private void cacheReadResult(
            String tableIdentifier, byte[] key, byte[] value, TxnContext txnContext) {
        if (txnContext == null) return;
        TxnReadCache readCache = txnContext.readCache();
        if (readCache == null) return;
        try {
            if (value != null) {
                readCache.put(tableIdentifier, key, value);
            } else {
                readCache.putNotFound(tableIdentifier, key);
            }
        } catch (Exception e) {
            log.debug("cacheReadResult failed for table {}: {}", tableIdentifier, e.getMessage());
        }
    }

    private static final long SYNTHETIC_TABLE_ID = 0L;
    private static final long SYNTHETIC_SCHEMA_VERSION = 0L;
    private static final long SYNTHETIC_SEQUENCE_NUMBER = 0L;
    private static final long SYNTHETIC_COMMIT_TIMESTAMP = 0L;
    private static final Schema BRIDGE_FALLBACK_SCHEMA =
            Schema.builder()
                    .field("value", io.github.rawwise.model.record.Types.binary(true))
                    .build();

    private static Record createBridgeFallbackRecord(byte[] key, byte[] nullableWrappedBytes) {
        byte[] decoded = NullableValueCodec.decode(nullableWrappedBytes);
        if (decoded == null) {
            decoded = nullableWrappedBytes;
        }
        Record.Key recordKey = Records.keyOf(key != null ? key : new byte[0]);
        Record.Body body = new DefaultBody(BRIDGE_FALLBACK_SCHEMA, new Object[] {decoded});
        return new DefaultRecord(
                recordKey,
                body,
                new Record.Header(
                        recordKey,
                        SYNTHETIC_TABLE_ID,
                        SYNTHETIC_SCHEMA_VERSION,
                        SYNTHETIC_SEQUENCE_NUMBER,
                        SYNTHETIC_COMMIT_TIMESTAMP,
                        Record.Kind.INSERT,
                        true,
                        false,
                        ImmutabilitySource.MUTABLE));
    }

    public record FlushedInfo(String instanceId, String hdfsPath, boolean success, Throwable t) {}

    public void recoverFromLocalLMDB(String localBasePath, String tableIdentifier) {
        storeRecoveryService.recoverFromLocalLMDB(localBasePath, tableIdentifier);
    }

    public void recoverFromRemoteWAL(String walHdfsDir, String tableIdentifier) {
        storeRecoveryService.recoverFromRemoteWAL(walHdfsDir, tableIdentifier);
    }

    public void recoverShard(ShardRecoveryRecord record) {
        storeRecoveryService.recoverShard(record);
    }

    public int recoverColdBlocksForTable(String tableIdentifier) {
        return storeRecoveryService.recoverColdBlocksForTable(tableIdentifier);
    }

    public void cleanupPendingInstancesForTable(String tableIdentifier) {
        TableStore ts = tableStore(tableIdentifier);
        Set<String> pendingIds = ts.pendingInstanceIds();
        for (String instanceId : pendingIds) {
            try {
                ts.rollbackPendingInstance(instanceId);
                log.info(
                        "Cleaned up pending instance {} for table {}", instanceId, tableIdentifier);
            } catch (Exception e) {
                log.warn(
                        "Failed to cleanup pending instance {} for table {}: {}",
                        instanceId,
                        tableIdentifier,
                        e.getMessage());
            }
        }
    }
}
