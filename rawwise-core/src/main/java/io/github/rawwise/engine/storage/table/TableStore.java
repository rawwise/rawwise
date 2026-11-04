package io.github.rawwise.engine.storage.table;

import io.github.rawwise.engine.storage.CommitMarker;
import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.PostWriteHook;
import io.github.rawwise.engine.storage.block.ColdDataBlock;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.block.HotDataBlock;
import io.github.rawwise.engine.storage.block.catalog.BlockCatalog;
import io.github.rawwise.engine.storage.block.catalog.InMemoryBlockCatalog;
import io.github.rawwise.engine.storage.block.index.PrimaryKeyIndex;
import io.github.rawwise.engine.storage.block.read.ColdDataBlockReader;
import io.github.rawwise.engine.storage.block.read.LmdbDataBlockReader;
import io.github.rawwise.engine.storage.codec.BodyCodec;
import io.github.rawwise.engine.storage.codec.BodyCodecRegistry;
import io.github.rawwise.engine.storage.codec.DefaultValueCodec;
import io.github.rawwise.engine.storage.codec.ValueCodec;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.storage.reader.ReaderConfig;
import io.github.rawwise.engine.storage.reader.ReaderContext;
import io.github.rawwise.engine.storage.reader.ReaderFactoryBootstrap;
import io.github.rawwise.engine.storage.reader.ReaderFactoryRegistry;
import io.github.rawwise.engine.storage.reader.ReaderType;
import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.engine.storage.table.key.DefaultKeyCodec;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import io.github.rawwise.engine.storage.table.key.PrimaryKeyKeyCodec;
import io.github.rawwise.engine.storage.table.read.BlockRoutingTableReader;
import io.github.rawwise.engine.storage.table.read.TableReader;
import io.github.rawwise.engine.storage.table.write.LmdbTableWriter;
import io.github.rawwise.engine.storage.table.write.TableWriter;
import io.github.rawwise.model.op.Read;
import io.github.rawwise.model.op.Write;
import io.github.rawwise.model.op.WriteBuilder;
import io.github.rawwise.model.record.DefaultSchemaResolver;
import io.github.rawwise.model.record.PrimaryKey;
import io.github.rawwise.model.record.RecordCodec;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.TableId;
import io.github.rawwise.model.record.impl.DefaultRecordCodec;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TableStore implements TableStorage {

    @Getter private final String tableIdentifier;
    private final TableId tableId;
    private final int writableNum;
    private final Semaphore writableSemaphore;
    @Getter private final KeyCodec keyCodec;
    private final TableReader reader;
    private final TableWriter writer;
    private final BlockCatalog blockCatalog;
    private final ConcurrentNavigableMap<String, LmdbInstance> readOnlyInstances =
            new ConcurrentSkipListMap<>();
    private final ConcurrentNavigableMap<String, LmdbInstance> writableInstances =
            new ConcurrentSkipListMap<>();
    private final ConcurrentHashMap<String, LmdbInstance> pendingInstances =
            new ConcurrentHashMap<>();
    private final AtomicLong pendingInstanceCounter = new AtomicLong(0);
    private volatile CommitMarker commitMarker;
    private volatile RecordCodec recordCodec;
    private volatile RecordCodec.SchemaResolver schemaResolver;
    private final Supplier<RecordCodec.SchemaResolver> schemaResolverSupplier;
    @Getter private final String bodyCodecName;
    private final long numericTableId;
    private volatile Schema initialSchema;
    private volatile SnapshotManager snapshotManager;
    private volatile boolean aiNative = false;
    private volatile DerivationStrategy derivationStrategy = DerivationStrategy.AUTO;
    private volatile String chunkingStrategy;
    private volatile String semanticPattern;
    private volatile boolean propertiesAppliedFromMeta = false;

    private final java.util.List<PostWriteHook> postWriteHooks =
            new java.util.concurrent.CopyOnWriteArrayList<>();

    public enum DerivationStrategy {
        AUTO,
        MANUAL,
        NONE
    }

    public void registerPostWriteHook(PostWriteHook hook) {
        postWriteHooks.add(hook);
    }

    public java.util.List<PostWriteHook> getPostWriteHooks() {
        return postWriteHooks;
    }

    /**
     * Key-Based routing table: uses consistent hashing to route writes of different keys to
     * different LmdbInstances. volatile ensures thread-safe publication, rebuild() returns new
     * instance then replaces via CAS.
     */
    private volatile KeyRoutingTable keyRoutingTable = new KeyRoutingTable();

    private volatile RangeIndex rangeIndex = new RangeIndex();
    private final PrimaryKeyIndex primaryKeyIndex = new PrimaryKeyIndex();

    /**
     * Routing table version number generator, increments on each routing change. Used to rebuild
     * routing table during addWritableInstance/removeInstance.
     */
    private final AtomicLong routingTableVersionGenerator = new AtomicLong(0);

    public TableStore(String tableIdentifier, int writableNum) {
        this(tableIdentifier, writableNum, new DefaultKeyCodec());
    }

    public TableStore(String tableIdentifier, int writableNum, PrimaryKey primaryKey) {
        this(tableIdentifier, writableNum, createKeyCodec(primaryKey));
    }

    public TableStore(String tableIdentifier, int writableNum, List<String> primaryKeyFields) {
        this(tableIdentifier, writableNum, createKeyCodec(primaryKeyFields));
    }

    public TableStore(String tableIdentifier, int writableNum, KeyCodec keyCodec) {
        this(tableIdentifier, writableNum, keyCodec, null);
    }

    public TableStore(
            String tableIdentifier,
            int writableNum,
            KeyCodec keyCodec,
            Supplier<RecordCodec.SchemaResolver> schemaResolverSupplier) {
        this(tableIdentifier, writableNum, keyCodec, schemaResolverSupplier, "fixed-offset", 0L);
    }

    public TableStore(
            String tableIdentifier,
            int writableNum,
            KeyCodec keyCodec,
            Supplier<RecordCodec.SchemaResolver> schemaResolverSupplier,
            String bodyCodecName,
            long numericTableId) {
        this(
                tableIdentifier,
                writableNum,
                keyCodec,
                schemaResolverSupplier,
                bodyCodecName,
                numericTableId,
                null);
    }

    public TableStore(
            String tableIdentifier,
            int writableNum,
            KeyCodec keyCodec,
            Supplier<RecordCodec.SchemaResolver> schemaResolverSupplier,
            String bodyCodecName,
            long numericTableId,
            Schema initialSchema) {
        this.tableIdentifier = tableIdentifier;
        this.tableId = parseTableId(tableIdentifier);
        this.writableNum = writableNum;
        this.writableSemaphore = new Semaphore(writableNum);
        this.keyCodec = keyCodec;
        this.schemaResolverSupplier = schemaResolverSupplier;
        this.bodyCodecName = bodyCodecName;
        this.numericTableId = numericTableId;
        this.initialSchema = initialSchema;
        this.writer = new LmdbTableWriter();
        this.blockCatalog = new InMemoryBlockCatalog(tableIdentifier);
        this.reader = createReader();
    }

    public RecordCodec getRecordCodec() {
        RecordCodec local = recordCodec;
        if (local == null) {
            synchronized (this) {
                local = recordCodec;
                if (local == null) {
                    ValueCodec valueCodec = new DefaultValueCodec(keyCodec);
                    BodyCodec resolvedBodyCodec = resolveBodyCodec();
                    local = new DefaultRecordCodec(valueCodec, resolvedBodyCodec);
                    recordCodec = local;
                }
            }
        }
        return local;
    }

    /**
     * Resolve the {@link BodyCodec} for this table.
     *
     * <p>Looks up the registered codec by {@code bodyCodecName} and creates a cached instance using
     * {@link BodyCodecRegistry}. Returns {@code null} if no body codec is configured, preserving
     * the default ValueCodec path.
     */
    private BodyCodec resolveBodyCodec() {
        if (bodyCodecName == null || bodyCodecName.isEmpty()) return null;
        if (!BodyCodecRegistry.isRegistered(bodyCodecName)) {
            log.warn(
                    "Unknown BodyCodec '{}' for table {}, falling back to ValueCodec",
                    bodyCodecName,
                    tableIdentifier);
            return null;
        }
        if (schemaResolverSupplier == null) {
            if (initialSchema != null) {
                return BodyCodecRegistry.create(bodyCodecName, initialSchema);
            }
            return null;
        }
        try {
            RecordCodec.SchemaResolver resolver = getSchemaResolver();
            if (resolver == null) return null;
            if (numericTableId <= 0) {
                return null;
            }
            Schema schema = resolver.resolve(numericTableId, 0L);
            if (schema == null) return null;
            return BodyCodecRegistry.create(bodyCodecName, schema);
        } catch (Exception e) {
            log.warn(
                    "Failed to create BodyCodec '{}' for table {}: {}",
                    bodyCodecName,
                    tableIdentifier,
                    e.getMessage());
            return null;
        }
    }

    public RecordCodec.SchemaResolver getSchemaResolver() {
        RecordCodec.SchemaResolver local = schemaResolver;
        if (local == null) {
            synchronized (this) {
                local = schemaResolver;
                if (local == null) {
                    if (schemaResolverSupplier != null) {
                        local = new DefaultSchemaResolver(schemaResolverSupplier.get());
                    } else {
                        local =
                                (tableId, schemaVersion) -> {
                                    throw new UnsupportedOperationException(
                                            "No SchemaResolver configured for table "
                                                    + tableIdentifier);
                                };
                    }
                    schemaResolver = local;
                }
            }
        }
        return local;
    }

    public SnapshotManager getSnapshotManager() {
        return snapshotManager;
    }

    public void setSnapshotManager(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }

    public void setSchemaResolverSupplier(Supplier<RecordCodec.SchemaResolver> supplier) {
        // Inject SchemaResolver supplier; FastPath side (ExecutorPointQueryServiceImpl) can
        // load schema into TableStore upon receiving the first request containing schema, so
        // subsequent read/write paths
        // can all parse RecordCodec bytes.
        synchronized (this) {
            if (supplier != null) {
                this.schemaResolver =
                        new io.github.rawwise.model.record.DefaultSchemaResolver(supplier.get());
            }
        }
    }

    public void setAiNative(boolean aiNative) {
        this.aiNative = aiNative;
    }

    public boolean isAiNative() {
        return aiNative;
    }

    public void setDerivationStrategy(DerivationStrategy strategy) {
        this.derivationStrategy = strategy != null ? strategy : DerivationStrategy.AUTO;
    }

    public DerivationStrategy getDerivationStrategy() {
        return derivationStrategy;
    }

    public void setChunkingStrategy(String strategy) {
        this.chunkingStrategy = strategy;
    }

    public String getChunkingStrategy() {
        return chunkingStrategy;
    }

    public void setSemanticPattern(String pattern) {
        this.semanticPattern = pattern;
    }

    public String getSemanticPattern() {
        return semanticPattern;
    }

    public void setPropertiesAppliedFromMeta(boolean applied) {
        this.propertiesAppliedFromMeta = applied;
    }

    public boolean getPropertiesAppliedFromMeta() {
        return propertiesAppliedFromMeta;
    }

    public long getNumericTableId() {
        return numericTableId;
    }

    private BlockRoutingTableReader createReader() {
        ReaderFactoryBootstrap.ensureInitialized();
        ReaderFactoryRegistry registry = ReaderFactoryRegistry.getInstance();
        try {
            BodyCodec bc = resolveBodyCodec();
            RecordCodec.SchemaResolver sr = getSchemaResolver();
            ReaderContext lmdbCtx = ReaderContext.empty().put("tableStore", this);
            if (bc != null) {
                lmdbCtx.put("bodyCodec", bc);
            }
            if (sr != null) {
                lmdbCtx.put("schemaResolver", sr);
            }
            LmdbDataBlockReader lmdbReader =
                    registry.create(
                            ReaderType.BLOCK_LMDB,
                            LmdbDataBlockReader.class,
                            ReaderConfig.builder(ReaderType.BLOCK_LMDB).build(),
                            lmdbCtx);
            ReaderContext coldCtx = ReaderContext.empty().put("tableStore", this);
            if (bc != null) {
                coldCtx.put("bodyCodec", bc);
            }
            if (sr != null) {
                coldCtx.put("schemaResolver", sr);
            }
            if (snapshotManager != null) {
                coldCtx.put("snapshotManager", snapshotManager);
            }
            // Resolve recordCodec eagerly (forum-calls getRecordCodec for DCL init)
            // so it is available for legacy Parquet "key"/"value" decode.
            RecordCodec rc = getRecordCodec();
            if (rc != null) {
                coldCtx.put("recordCodec", rc);
            }
            // Pass snapshotManager from this store's SnapshotManager reference
            // (This is set externally via the TableStore's SnapshotManager provider)
            ColdDataBlockReader coldReader =
                    registry.create(
                            ReaderType.BLOCK_COLD,
                            ColdDataBlockReader.class,
                            ReaderConfig.builder(ReaderType.BLOCK_COLD).build(),
                            coldCtx);
            ReaderContext ctx =
                    ReaderContext.empty()
                            .put("tableStore", this)
                            .put("dataBlockReaders", List.of(lmdbReader, coldReader));
            return registry.create(
                    ReaderType.TABLE_ROUTING,
                    BlockRoutingTableReader.class,
                    ReaderConfig.builder(ReaderType.TABLE_ROUTING).build(),
                    ctx);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create BlockRoutingTableReader via factory", e);
        }
    }

    public TableReader reader() {
        return reader;
    }

    @Override
    public TableId tableId() {
        return tableId;
    }

    public LmdbInstance getCurrentWritableInstance() {
        // Skip READ_ONLY instances: flush coordinator may mark instances as READ_ONLY in the
        // background,
        // but instances not yet removed from writableInstances (rotateWritableInstance not yet
        // executed).
        for (LmdbInstance instance : writableInstances.values()) {
            if (!instance.readOnly() && !instance.isPendingReadOnly()) {
                return instance;
            }
        }
        return null;
    }

    /**
     * Route to a specific writable LmdbInstance based on key.
     *
     * <p>Uses KeyRoutingTable's consistent hash ring to compute which instance the key should route
     * to. If routing table is empty (no writable instances), returns null.
     *
     * @param key write key byte array
     * @return routed LmdbInstance, or null if no available instance
     */
    public LmdbInstance getWritableInstance(byte[] key) {
        if (keyRoutingTable.isEmpty()) {
            return getCurrentWritableInstance();
        }
        String instanceId = keyRoutingTable.route(key);
        LmdbInstance instance = writableInstances.get(instanceId);
        if (instance != null && !instance.readOnly() && !instance.isPendingReadOnly()) {
            return instance;
        }
        return getCurrentWritableInstance();
    }

    /**
     * Get current KeyRoutingTable. Used to check whether routing table version changed during
     * transaction commit.
     *
     * @return current KeyRoutingTable instance
     */
    public KeyRoutingTable getKeyRoutingTable() {
        return keyRoutingTable;
    }

    public RangeIndex getRangeIndex() {
        return rangeIndex;
    }

    public PrimaryKeyIndex getPrimaryKeyIndex() {
        return primaryKeyIndex;
    }

    public void registerPrimaryKeyLocation(
            byte[] encodedKey, String blockId, int rowGroupIndex, long snapshotId) {
        primaryKeyIndex.put(encodedKey, blockId, rowGroupIndex, snapshotId);
    }

    public void unregisterBlockFromPrimaryKeyIndex(String blockId) {
        primaryKeyIndex.removeBlock(blockId);
    }

    public void addWritableInstance(LmdbInstance instance) {
        if (writableInstances.size() >= writableNum) {
            log.warn(
                    "Table[{}] writable instances limit reached ({}), reject add: {}",
                    tableIdentifier,
                    writableNum,
                    instance.instanceId());
            return;
        }

        LmdbInstance prev = writableInstances.putIfAbsent(instance.instanceId(), instance);
        if (prev != null) {
            log.warn("Instance {} already exists in writable map", instance.instanceId());
            return;
        }
        blockCatalog.upsert(toHotDataBlock(instance));
        rebuildRoutingTable();
    }

    public void addReadOnlyInstance(LmdbInstance instance) {
        readOnlyInstances.put(instance.instanceId(), instance);
        blockCatalog.upsert(toHotDataBlock(instance));
        rebuildRangeIndex();
    }

    private void addReadOnlyInstanceWithoutUpsert(LmdbInstance instance) {
        readOnlyInstances.put(instance.instanceId(), instance);
    }

    private static final int MAX_PENDING_INSTANCES_WARN = 1000;

    public LmdbInstance addPendingInstance(LmdbInstance instance) {
        pendingInstances.put(instance.instanceId(), instance);
        if (pendingInstances.size() >= MAX_PENDING_INSTANCES_WARN) {
            log.warn(
                    "Table[{}] has {} pending instances (limit={}), consider freezing old instances",
                    tableIdentifier,
                    pendingInstances.size(),
                    MAX_PENDING_INSTANCES_WARN);
        }
        blockCatalog.upsert(toHotDataBlock(instance, DataBlock.Visibility.PENDING));
        return instance;
    }

    public void removePendingInstanceReference(String instanceId) {
        pendingInstances.remove(instanceId);
        blockCatalog.remove(instanceId);
    }

    public void commitPendingInstances(List<String> instanceIds, long txnId, long commitTs) {
        List<LmdbInstance> existing = new ArrayList<>();
        for (String instanceId : instanceIds) {
            LmdbInstance instance = pendingInstances.get(instanceId);
            if (instance != null) {
                existing.add(instance);
            }
        }

        if (!existing.isEmpty()) {
            if (commitMarker != null) {
                commitMarker.writeCommitMarker(txnId, instanceIds, commitTs);
            }

            blockCatalog.batchUpdateVisibility(
                    instanceIds, DataBlock.Visibility.PENDING, DataBlock.Visibility.ACTIVE);

            rebuildRoutingTable();
            rebuildRangeIndex();

            if (commitMarker != null) {
                commitMarker.deleteCommitMarker(txnId);
            }
        }
    }

    /**
     * Freeze a pending instance: mark it as read-only and migrate from pendingInstances to
     * readOnlyInstances. This is the LSM-style memtable rotation step, distinct from transaction
     * commit. Triggered when the instance reaches a size threshold or MAP_FULL, not on every
     * commit.
     */
    public void freezeInstance(String instanceId) {
        LmdbInstance instance = pendingInstances.remove(instanceId);
        if (instance == null) {
            return;
        }
        instance.markAsReadOnly();
        addReadOnlyInstanceWithoutUpsert(instance);
        rebuildRoutingTable();
        rebuildRangeIndex();
    }

    public void rollbackPendingInstance(String instanceId) {
        LmdbInstance instance = pendingInstances.remove(instanceId);
        if (instance != null) {
            blockCatalog.updateVisibility(instanceId, DataBlock.Visibility.DELETED);
            try {
                instance.destroy();
            } catch (Exception e) {
                log.error(
                        "Failed to destroy pending instance {} during rollback: {}",
                        instanceId,
                        e.getMessage());
            }
        }
    }

    public void recoverCommittedInstance(String instanceId) {
        LmdbInstance instance = pendingInstances.remove(instanceId);
        if (instance == null) {
            instance = readOnlyInstances.get(instanceId);
        }
        if (instance != null) {
            instance.markAsReadOnly();
            blockCatalog.batchUpdateVisibility(
                    List.of(instanceId), DataBlock.Visibility.PENDING, DataBlock.Visibility.ACTIVE);
            addReadOnlyInstanceWithoutUpsert(instance);
            rebuildRoutingTable();
            rebuildRangeIndex();
        }
    }

    public LmdbInstance getPendingInstance(String instanceId) {
        return pendingInstances.get(instanceId);
    }

    public int pendingInstanceCount() {
        return pendingInstances.size();
    }

    public Set<String> pendingInstanceIds() {
        return Collections.unmodifiableSet(pendingInstances.keySet());
    }

    public Map<String, LmdbInstance> pendingInstances() {
        return Collections.unmodifiableMap(pendingInstances);
    }

    public long nextPendingInstanceIndex() {
        return pendingInstanceCounter.incrementAndGet();
    }

    public CommitMarker commitMarker() {
        return commitMarker;
    }

    public void setCommitMarker(CommitMarker marker) {
        this.commitMarker = marker;
    }

    public void rotateWritableInstance() {
        Map.Entry<String, LmdbInstance> entry = writableInstances.pollFirstEntry();
        if (entry != null) {
            LmdbInstance old = entry.getValue();
            // Do not call markAsReadOnly(): instance naturally rejects new writes after removal
            // from writableInstances.
            // markAsReadOnly() attempts to acquire writeLock, if another thread holds readLock
            // (currently writing),
            // tryLock failure sets pendingReadOnly=true, triggering unnecessary instance switching
            // and performance degradation.
            // Instance state remains OPEN, still readable after being placed in readOnlyInstances.
            readOnlyInstances.put(entry.getKey(), old);
            blockCatalog.upsert(toHotDataBlock(old));
            rebuildRoutingTable();
            rebuildRangeIndex();
            log.info("Rotated instance {} to readonly", entry.getKey());
        }
    }

    public boolean isWritable() {
        return !writableInstances.isEmpty();
    }

    public int remainingWritable() {
        return writableNum - writableInstances.size();
    }

    @Override
    public Write.Result write(Write write) throws IOException {
        Objects.requireNonNull(write, "write cannot be null");
        return writer.write(write);
    }

    public Write.Result write(WriteBuilder builder, LmdbInstance instance) throws IOException {
        return writer.write(builder, new LmdbTableWriter.LmdbWriteContext(instance, this.keyCodec));
    }

    /**
     * Reads the raw byte value for a given key from any instance (writable first, then readonly).
     *
     * <p>Used by tests and internal operations to verify tombstone entries without going through
     * the full Read path which would decode the value.
     *
     * @param key the key to look up (already encoded by KeyCodec)
     * @return raw byte[] value, or null if not found
     */
    public byte[] getRawValue(byte[] key) {
        for (LmdbInstance instance : writableInstances.values()) {
            byte[] value = instance.get(key);
            if (value != null) return value;
        }
        for (LmdbInstance instance : readOnlyInstances.values()) {
            byte[] value = instance.get(key);
            if (value != null) return value;
        }
        for (LmdbInstance instance : pendingInstances.values()) {
            byte[] value = instance.get(key);
            if (value != null) return value;
        }
        return null;
    }

    @Override
    public Read.Result read(Read read) throws IOException {
        Objects.requireNonNull(read, "read cannot be null");
        return reader.read(read);
    }

    @Override
    public List<DataBlock> currentDataBlocks() {
        return blockCatalog.currentBlocks();
    }

    @Override
    public void flush() throws IOException {
        for (LmdbInstance instance : writableInstances.values()) {
            if (!instance.readOnly()) {
                instance.markAsReadOnly();
            }
        }
    }

    public void forEachInstance(Consumer<LmdbInstance> consumer) {
        Objects.requireNonNull(consumer);
        readOnlyInstances.values().forEach(consumer);
        writableInstances.values().forEach(consumer);
    }

    public Stream<LmdbInstance> streamInstances() {
        return Stream.concat(
                readOnlyInstances.values().stream(), writableInstances.values().stream());
    }

    public LmdbInstance getInstance(String instanceId) {
        LmdbInstance instance = writableInstances.get(instanceId);
        if (instance != null) {
            return instance;
        }
        instance = readOnlyInstances.get(instanceId);
        if (instance != null) {
            return instance;
        }
        return pendingInstances.get(instanceId);
    }

    public List<LmdbInstance> getAllInstancesDescending() {
        List<LmdbInstance> all = new ArrayList<>();
        all.addAll(writableInstances.descendingMap().values());
        all.addAll(readOnlyInstances.descendingMap().values());
        return all;
    }

    public List<LmdbInstance> getReadOnlyInstances() {
        return new ArrayList<>(readOnlyInstances.values());
    }

    public List<LmdbInstance> getWritableInstances() {
        return new ArrayList<>(writableInstances.values());
    }

    public void removeInstance(LmdbInstance wrapper) {
        writableInstances.remove(wrapper.instanceId());
        readOnlyInstances.remove(wrapper.instanceId());
        blockCatalog.remove(wrapper.instanceId());
        rebuildRoutingTable();
        rebuildRangeIndex();
    }

    public void registerColdBlock(ColdDataBlock dataBlock) {
        blockCatalog.upsert(dataBlock);
    }

    public void markBlockVisibility(String blockId, DataBlock.Visibility visibility) {
        blockCatalog.updateVisibility(blockId, visibility);
    }

    public BlockCatalog blockCatalog() {
        return blockCatalog;
    }

    public void close() {
        getAllInstancesDescending()
                .forEach(
                        instance -> {
                            try {
                                instance.close();
                            } catch (Exception e) {
                                log.warn("Failed to close instance {}", instance.instanceId(), e);
                            }
                        });
    }

    private HotDataBlock toHotDataBlock(LmdbInstance instance) {
        DataBlock.Visibility visibility =
                instance.isMarkedForDeletion()
                        ? DataBlock.Visibility.FLUSHING
                        : DataBlock.Visibility.ACTIVE;
        return toHotDataBlock(instance, visibility);
    }

    private HotDataBlock toHotDataBlock(LmdbInstance instance, DataBlock.Visibility visibility) {
        return new HotDataBlock(
                instance.instanceId(),
                tableIdentifier,
                instance.readOnly() ? DataBlock.Kind.IMMUTABLE_HOT : DataBlock.Kind.MUTABLE_HOT,
                DataBlock.Format.LMDB,
                new DataBlock.Location(
                        resolveBlockHost(),
                        new Storage.StoragePath("file", instance.path().toString()),
                        0L,
                        instance.getTotalUsageSize()),
                new KeyRange(instance.minKey(), true, instance.maxKey(), true),
                instance.getTotalRecords(),
                instance.getTotalUsageSize(),
                0L,
                visibility,
                0L,
                0L,
                0L,
                instance.getTotalUsageSize(),
                instance.instanceId());
    }

    private static String resolveBlockHost() {
        String configured = System.getProperty("rawwise.block.host");
        if (configured != null && !configured.isBlank()) {
            return configured.trim();
        }
        String envHost = System.getenv("SPARK_LOCAL_HOSTNAME");
        if (envHost == null || envHost.isBlank()) {
            envHost = System.getenv("HOSTNAME");
        }
        if (envHost != null && !envHost.isBlank()) {
            return envHost.trim();
        }
        return "localhost";
    }

    private static TableId parseTableId(String tableIdentifier) {
        String[] parts = tableIdentifier.split("\\.");
        if (parts.length >= 3) {
            return new TableId(parts[1], parts[2]);
        }
        if (parts.length == 2) {
            return new TableId(parts[0], parts[1]);
        }
        return new TableId("default", tableIdentifier);
    }

    public static KeyCodec createKeyCodec(PrimaryKey primaryKey) {
        if (primaryKey == null || primaryKey.isEmpty()) {
            return new DefaultKeyCodec();
        }
        return new PrimaryKeyKeyCodec(primaryKey.fields());
    }

    public static KeyCodec createKeyCodec(List<String> primaryKeyFields) {
        if (primaryKeyFields == null || primaryKeyFields.isEmpty()) {
            return new DefaultKeyCodec();
        }
        return new PrimaryKeyKeyCodec(primaryKeyFields);
    }

    /**
     * Rebuild KeyRoutingTable.
     *
     * <p>Call this method when writableInstances change (add/remove/rotate). Rebuild consistent
     * hash ring using current writableInstances index list, version number auto-increments.
     *
     * <p>Thread safety: ensures thread-safe publication via volatile reference replacement. Threads
     * reading old routing table are unaffected, new writes will use new routing table.
     */
    private void rebuildRoutingAndRangeIndex() {
        List<String> activeWritableIds = new ArrayList<>(writableInstances.keySet());
        List<LmdbInstance> readOnlyList = new ArrayList<>(readOnlyInstances.values());
        readOnlyList.addAll(pendingInstances.values());
        List<LmdbInstance> writableList = new ArrayList<>(writableInstances.values());

        if (activeWritableIds.isEmpty()) {
            this.keyRoutingTable = new KeyRoutingTable();
        } else {
            this.keyRoutingTable = keyRoutingTable.rebuild(activeWritableIds);
        }
        this.rangeIndex = RangeIndex.rebuild(readOnlyList, writableList);

        log.debug(
                "Table[{}] routing+range index rebuilt: routingVersion={}, rangeSize={}",
                tableIdentifier,
                keyRoutingTable.version(),
                rangeIndex.size());
    }

    private void rebuildRoutingTable() {
        rebuildRoutingAndRangeIndex();
    }

    private void rebuildRangeIndex() {
        rebuildRoutingAndRangeIndex();
    }
}
