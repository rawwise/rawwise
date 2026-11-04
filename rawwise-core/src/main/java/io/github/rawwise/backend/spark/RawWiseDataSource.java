package io.github.rawwise.backend.spark;

import io.github.rawwise.backend.DefaultPlanner;
import io.github.rawwise.backend.ReadPlanner;
import io.github.rawwise.backend.SparkV1FilterConverter;
import io.github.rawwise.backend.lazyupdate.LazyUpdateReadEnhancer;
import io.github.rawwise.backend.lazyupdate.UpdateLogEntry;
import io.github.rawwise.backend.read.DataBlockListProvider;
import io.github.rawwise.backend.read.LocalTableStoreBlockListProvider;
import io.github.rawwise.backend.read.ReadOptimizerChain;
import io.github.rawwise.backend.read.ReadPruning;
import io.github.rawwise.backend.routing.ShardAssignment;
import io.github.rawwise.backend.server.txn.ServerTxnCoordinator;
import io.github.rawwise.backend.spark.routing.RoutingWriteClient;
import io.github.rawwise.backend.spark.routing.ShardOwnerCache;
import io.github.rawwise.backend.spark.routing.SparkMurmur3ShardResolver;
import io.github.rawwise.backend.spark.routing.StableBatchId;
import io.github.rawwise.common.catalog.Catalog;
import io.github.rawwise.common.catalog.CatalogProviderRegistry;
import io.github.rawwise.common.spi.TxnStoreBridge;
import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnContextHolder;
import io.github.rawwise.governance.config.RawWiseOptions;
import io.github.rawwise.governance.config.TxnEngineConfig;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.op.Find;
import io.github.rawwise.model.op.KvScan;
import io.github.rawwise.model.op.Read;
import io.github.rawwise.model.record.*;
import io.github.rawwise.model.record.Record;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.unsafe.types.UTF8String;

@Slf4j
public final class RawWiseDataSource implements TableProvider, DataSourceRegister {
    public static final String OPTION_PATH = "path";
    public static final String OPTION_TABLE = "table";
    public static final String OPTION_STORE_ID = "storeId";
    public static final String KEY_COLUMN = "key";
    public static final String VALUE_COLUMN = "value";
    static final boolean ASSUME_PARTITION_IS_SHARD_DEFAULT = false;

    public static void installLazyUpdateReadEnhancer(LazyUpdateReadEnhancer enhancer) {
        RawWiseScanBuilder.installLazyUpdateReadEnhancer(enhancer);
    }

    private static final StructType DEFAULT_SCHEMA =
            new StructType(
                    new StructField[] {
                        new StructField(KEY_COLUMN, DataTypes.StringType, false, Metadata.empty()),
                        new StructField(VALUE_COLUMN, DataTypes.StringType, true, Metadata.empty())
                    });

    /**
     * Safely get SparkConf from SparkEnv, with fallback to default values if SparkEnv is not
     * available.
     */
    private static SparkConf getSparkConfOrDefault() {
        if (SparkEnv.get() != null && SparkEnv.get().conf() != null) {
            return SparkEnv.get().conf();
        }
        // Return a empty conf that will use default values
        return new SparkConf();
    }

    public static boolean isRawWiseTable(org.apache.spark.sql.connector.catalog.Table table) {
        return table instanceof RawWiseSparkTable;
    }

    public static boolean isRawWiseWrite(Write write) {
        return write instanceof RawWiseBatchWrite;
    }

    public static int resolveShardCount(SparkConf conf) {
        Objects.requireNonNull(conf, "conf must not be null");
        int configured = conf.getInt(RawWiseOptions.ROUTING_SHARD_COUNT.key(), -1);
        if (configured > 0) {
            return configured;
        }
        int executorCores = conf.getInt("spark.executor.cores", 1);
        int numExecutors = conf.getInt("spark.executor.instances", -1);
        if (numExecutors <= 0) {
            // todo throw exception directly
            numExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors", 1);
        }
        int totalCores = executorCores * numExecutors;
        return Math.clamp(totalCores, 1, 65536);
    }

    @Override
    public String shortName() {
        return "rawwise";
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        String tableIdentifier = options.get(OPTION_TABLE);
        if (tableIdentifier != null && !tableIdentifier.isEmpty()) {
            StructType resolved = resolveSchemaFromCatalog(tableIdentifier, options);
            if (resolved != null) {
                return resolved;
            }
            throw new IllegalArgumentException(
                    "Cannot resolve schema for table: "
                            + tableIdentifier
                            + ". Please provide schema explicitly or check catalog configuration.");
        }
        return DEFAULT_SCHEMA;
    }

    private StructType resolveSchemaFromCatalog(
            String tableIdentifier, CaseInsensitiveStringMap options) {
        try {
            String catalogKind =
                    options.getOrDefault(
                            RawWiseOptions.CATALOG_IMPLEMENTATION.key(),
                            RawWiseOptions.CATALOG_IMPLEMENTATION.defaultValue());
            CatalogProviderRegistry registry = CatalogProviderRegistry.getInstance();
            if (!registry.isRegistered(catalogKind)) {
                registry.registerDefaults();
            }
            java.util.Map<String, String> catalogProps = new java.util.HashMap<>();
            catalogProps.put("catalog-name", "rawwise-datasource");
            catalogProps.put("base-path", options.getOrDefault(OPTION_PATH, "/tmp/rawwise"));
            try (Catalog catalog = registry.create(catalogKind, catalogProps)) {
                TableId tableId = TableId.parse(tableIdentifier);
                if (!catalog.tableExists(tableId)) {
                    return null;
                }
                RawWiseTable table = catalog.loadTable(tableId);
                Schema rawwiseSchema = table.meta().schema();
                if (rawwiseSchema != null) {
                    return SparkSchemaConverter.toSparkSchema(rawwiseSchema);
                }
            }
        } catch (Exception e) {
            log.warn(
                    "Catalog schema resolution failed for table {}: {}",
                    tableIdentifier,
                    e.getMessage());
        }
        return null;
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }

    @Override
    public Table getTable(
            StructType schema, Transform[] partitions, Map<String, String> properties) {
        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(properties);
        String warehousePath = io.github.rawwise.governance.config.WarehousePathResolver.resolve();
        String basePath = options.getOrDefault(OPTION_PATH, warehousePath);
        String tableIdentifier = options.getOrDefault(OPTION_TABLE, "");
        String storeId = options.getOrDefault(OPTION_STORE_ID, "spark");
        StructType effectiveSchema = schema != null ? schema : DEFAULT_SCHEMA;
        TableMeta tableMeta = buildTableMeta(tableIdentifier, effectiveSchema, properties);
        return new RawWiseSparkTable(
                basePath, storeId, tableIdentifier, effectiveSchema, tableMeta);
    }

    private static TableMeta buildTableMeta(
            String tableIdentifier, StructType schema, Map<String, String> properties) {
        String effectiveTableName = tableIdentifier;
        if (effectiveTableName == null || effectiveTableName.isBlank()) {
            effectiveTableName =
                    properties != null
                            ? properties.getOrDefault("path", "").replaceAll(".*/", "")
                            : "";
            if (effectiveTableName.isEmpty()) effectiveTableName = "spark_table";
        }
        String pkStr =
                properties != null
                        ? properties.getOrDefault(
                                "primaryKey", properties.getOrDefault("primary-key", ""))
                        : null;
        PrimaryKey pk;
        if (pkStr != null && !pkStr.isBlank()) {
            pk =
                    new PrimaryKey(
                            Arrays.stream(pkStr.split(","))
                                    .map(String::trim)
                                    .filter(s -> !s.isEmpty())
                                    .toList());
        } else {
            pk = new PrimaryKey(List.of(KEY_COLUMN));
        }
        return TableMeta.builder()
                .withTableId(0L)
                .withTableName(effectiveTableName)
                .withTableType(TableMeta.TableType.PRIMARY_KEY)
                .withSchema(SparkSchemaConverter.toRawWiseSchema(schema))
                .withPrimaryKey(pk)
                .build();
    }

    public static Set<String> primaryKeyColumns(
            org.apache.spark.sql.connector.catalog.Table table) {
        if (table instanceof RawWiseSparkTable nebulaTable) {
            PrimaryKey pk = nebulaTable.tableMeta().primaryKey();
            if (pk != null && !pk.fields().isEmpty()) {
                return new java.util.LinkedHashSet<>(pk.fields());
            }
        }
        return Set.of(KEY_COLUMN);
    }

    private static String required(CaseInsensitiveStringMap options, String key) {
        String value = options.get(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Missing required option: " + key);
        }
        return value;
    }

    static final class RawWiseScanBuilder
            implements ScanBuilder,
                    org.apache.spark.sql.connector.read.Scan,
                    Batch,
                    SupportsPushDownFilters,
                    SupportsPushDownRequiredColumns {
        private final String basePath;
        private final String storeId;
        private final String tableIdentifier;
        private final StructType schema;
        private final TableMeta tableMeta;

        private final DataBlockListProvider blockListProvider;
        private final DefaultPlanner readPlanner;
        private org.apache.spark.sql.sources.Filter[] filtersForPruning =
                new org.apache.spark.sql.sources.Filter[0];
        private ReadPruning.KeyPruningSpec cachedPruningSpec =
                ReadPruning.KeyPruningSpec.NO_PRUNING;
        private StructType prunedSchema;

        // Lazy update read enhancer bridge — allows the write interceptor pipeline
        // to inject a read enhancer that can skip data file scans when UpdateLogs
        // fully cover the query predicate.
        private static final AtomicReference<LazyUpdateReadEnhancer> ENHANCER =
                new AtomicReference<>();

        /**
         * Install a {@link LazyUpdateReadEnhancer} that will be used by all {@link
         * RawWiseScanBuilder} instances for lazy-update-aware scanning.
         *
         * @param enhancer the read enhancer, or {@code null} to disable
         */
        public static void installLazyUpdateReadEnhancer(LazyUpdateReadEnhancer enhancer) {
            ENHANCER.set(enhancer);
        }

        /**
         * Returns the currently installed {@link LazyUpdateReadEnhancer}, or {@code null} if
         * lazy-update-aware scanning is disabled.
         */
        public static LazyUpdateReadEnhancer lazyUpdateReadEnhancer() {
            return ENHANCER.get();
        }

        RawWiseScanBuilder(
                String basePath,
                String storeId,
                String tableIdentifier,
                StructType schema,
                TableMeta tableMeta) {
            this(
                    basePath,
                    storeId,
                    tableIdentifier,
                    schema,
                    tableMeta,
                    new LocalTableStoreBlockListProvider());
        }

        private RawWiseScanBuilder(
                String basePath,
                String storeId,
                String tableIdentifier,
                StructType schema,
                TableMeta tableMeta,
                DataBlockListProvider blockListProvider) {
            this.basePath = basePath;
            this.storeId = storeId;
            this.tableIdentifier =
                    tableIdentifier != null && !tableIdentifier.isBlank()
                            ? tableIdentifier
                            : (tableMeta != null
                                            && tableMeta.tableName() != null
                                            && !tableMeta.tableName().isBlank()
                                    ? tableMeta.tableName()
                                    : "unknown");
            this.schema = schema;
            this.tableMeta = tableMeta;
            this.blockListProvider = blockListProvider;
            this.readPlanner = new DefaultPlanner(blockListProvider, new ReadOptimizerChain());
            log.info(
                    "RawWiseScanBuilder constructed: basePath={}, storeId={}, tableIdentifier={}, tableMeta.tableName={}",
                    basePath,
                    storeId,
                    this.tableIdentifier,
                    tableMeta != null ? tableMeta.tableName() : "null");
        }

        @Override
        public org.apache.spark.sql.connector.read.Scan build() {
            return this;
        }

        @Override
        public StructType readSchema() {
            return prunedSchema != null ? prunedSchema : schema;
        }

        @Override
        public void pruneColumns(StructType requiredSchema) {
            this.prunedSchema = requiredSchema;
        }

        @Override
        public Batch toBatch() {
            return this;
        }

        @Override
        public InputPartition[] planInputPartitions() {
            log.info(
                    "planInputPartitions called: basePath={}, storeId={}, tableIdentifier={}",
                    basePath,
                    storeId,
                    tableIdentifier);
            LazyUpdateReadEnhancer enhancer = ENHANCER.get();
            Expression rawwiseCondition = convertSparkFilters(filtersForPruning);

            this.cachedPruningSpec =
                    filtersForPruning != null && filtersForPruning.length > 0
                            ? ReadPruning.extractKeyPruningSpec(filtersForPruning, KEY_COLUMN)
                            : ReadPruning.KeyPruningSpec.NO_PRUNING;

            List<DataBlock> debugBlocks =
                    new LocalTableStoreBlockListProvider()
                            .listBlocks(basePath, storeId, tableIdentifier);
            log.debug(
                    "planInputPartitions: table={} blocks={}", tableIdentifier, debugBlocks.size());

            List<UpdateLogEntry> updateLogs = null;
            if (enhancer != null) {
                try {
                    long snapshotId =
                            enhancer.lazyUpdateManager()
                                    .getSnapshotManager()
                                    .latestSnapshot(tableIdentifier)
                                    .map(io.github.rawwise.model.record.Snapshot::snapshotId)
                                    .orElse(0L);
                    updateLogs =
                            enhancer.lazyUpdateManager()
                                    .loadUpdateLogs(tableIdentifier, snapshotId);
                } catch (Exception e) {
                    updateLogs = null;
                }
            }

            List<String> projectedColumns = extractProjectedColumns(prunedSchema);

            ReadPlanner.ScanContext scanContext =
                    new ReadPlanner.ScanContext(
                            basePath,
                            storeId,
                            tableIdentifier,
                            tableMeta,
                            rawwiseCondition,
                            null,
                            false,
                            cachedPruningSpec,
                            updateLogs,
                            enhancer,
                            projectedColumns);

            io.github.rawwise.backend.read.ReadPlan plan = readPlanner.planReads(scanContext);

            if (plan.isEmpty()) {
                return new InputPartition[0];
            }

            if (plan.skipScanWithRowGeneration() && plan.generatedRows() != null) {
                return new InputPartition[] {
                    new RawWiseUpdateLogInputPartition(plan.generatedRows())
                };
            }

            List<io.github.rawwise.backend.read.ReadPartition> readPartitions = plan.partitions();
            List<io.github.rawwise.backend.read.ReadPartition> effectivePartitions = readPartitions;
            if (readPartitions.size() == 1) {
                io.github.rawwise.backend.read.ReadPartition single = readPartitions.get(0);
                if (single.blocks().size() > 1) {
                    effectivePartitions =
                            single.blocks().stream()
                                    .map(
                                            block ->
                                                    new io.github.rawwise.backend.read
                                                            .ReadPartition(
                                                            single.preferredHosts(), List.of(block),
                                                            single.tableMeta(),
                                                                    single.updateLogs()))
                                    .toList();
                    log.debug(
                            "planInputPartitions: split {} blocks into {} partitions for table={}",
                            single.blocks().size(),
                            effectivePartitions.size(),
                            tableIdentifier);
                }
            }

            InputPartition[] partitions = new InputPartition[effectivePartitions.size()];
            for (int i = 0; i < effectivePartitions.size(); i++) {
                io.github.rawwise.backend.read.ReadPartition rp = effectivePartitions.get(i);
                partitions[i] =
                        new RawWiseInputPartition(
                                rp.preferredHosts(),
                                rp.blocks(),
                                rp.tableMeta(),
                                rp.updateLogs(),
                                projectedColumns);
            }
            return partitions;
        }

        private static List<String> extractProjectedColumns(StructType prunedSchema) {
            if (prunedSchema == null) return List.of();
            return List.of(prunedSchema.fieldNames());
        }

        private static Expression convertSparkFilters(
                org.apache.spark.sql.sources.Filter[] filters) {
            return SparkV1FilterConverter.convertAll(filters);
        }

        @Override
        public PartitionReaderFactory createReaderFactory() {
            StructType effectiveSchema = prunedSchema != null ? prunedSchema : schema;
            return new RawWisePartitionReaderFactory(
                    basePath,
                    storeId,
                    tableIdentifier,
                    effectiveSchema,
                    cachedPruningSpec,
                    tableMeta);
        }

        @Override
        public org.apache.spark.sql.sources.Filter[] pushFilters(
                org.apache.spark.sql.sources.Filter[] filters) {
            // Used for block selection/range pruning only; we do not claim pushdown
            // so Spark continues to evaluate filters in upper layers for correctness.
            this.filtersForPruning =
                    filters == null ? new org.apache.spark.sql.sources.Filter[0] : filters;
            return filters;
        }

        @Override
        public org.apache.spark.sql.sources.Filter[] pushedFilters() {
            return new org.apache.spark.sql.sources.Filter[0];
        }
    }

    private record RawWiseInputPartition(
            String[] preferredHosts,
            List<DataBlock> blocks,
            TableMeta tableMeta,
            List<UpdateLogEntry> updateLogs,
            List<String> projectedColumns)
            implements InputPartition {
        @Override
        public String[] preferredLocations() {
            return preferredHosts == null ? new String[0] : preferredHosts;
        }
    }

    private record RawWisePartitionReaderFactory(
            String basePath,
            String storeId,
            String tableIdentifier,
            StructType schema,
            ReadPruning.KeyPruningSpec pruningSpec,
            TableMeta tableMeta)
            implements PartitionReaderFactory {

        @Override
        public PartitionReader<InternalRow> createReader(InputPartition partition) {
            if (partition
                    instanceof
                    RawWiseUpdateLogInputPartition(List<Map<String, Comparable<?>>> rows)) {
                return new UpdateLogPartitionReader(rows, schema);
            }
            RawWiseInputPartition p = (RawWiseInputPartition) partition;
            enforceLocalLmdbRead(p);
            DynamicMmapStore store = DynamicMmapStore.getInstance(basePath, storeId);
            TableStore tableStore = store.tableStore(tableIdentifier);
            return new RawWisePartitionReader(
                    store,
                    tableStore,
                    p.blocks(),
                    tableMeta,
                    schema,
                    p.updateLogs(),
                    pruningSpec,
                    p.projectedColumns());
        }

        private void enforceLocalLmdbRead(RawWiseInputPartition partition) {
            if (partition == null
                    || partition.preferredHosts() == null
                    || partition.preferredHosts().length == 0) {
                return;
            }
            List<DataBlock> blocks = partition.blocks();
            if (blocks == null || blocks.isEmpty()) {
                return;
            }
            boolean hasLmdb =
                    blocks.stream().anyMatch(b -> b != null && b.format() == DataBlock.Format.LMDB);
            if (!hasLmdb) {
                return;
            }
            String expectedHost = partition.preferredHosts()[0];
            String actualHost = null;
            try {
                if (SparkEnv.get() != null
                        && SparkEnv.get().rpcEnv() != null
                        && SparkEnv.get().rpcEnv().address() != null) {
                    actualHost = SparkEnv.get().rpcEnv().address().host();
                }
            } catch (Exception e) {
                log.debug("Failed to get Spark RPC address for locality check", e);
            }
            if (actualHost == null) {
                return;
            }
            if (isSameHost(expectedHost, actualHost)) {
                return;
            }
            throw new IllegalStateException(
                    "LMDB blocks must be read on preferred host. expected="
                            + expectedHost
                            + ", actual="
                            + actualHost);
        }

        private static boolean isSameHost(String host1, String host2) {
            if (host1.equals(host2)) return true;
            try {
                var addr1 = java.net.InetAddress.getByName(host1);
                var addr2 = java.net.InetAddress.getByName(host2);
                if (addr1.equals(addr2)) return true;
                if (addr1.isLoopbackAddress() && addr2.isLoopbackAddress()) return true;
                try {
                    var localHost = java.net.InetAddress.getLocalHost();
                    if (addr1.equals(localHost) || addr2.equals(localHost)) return true;
                    for (var iface :
                            java.util.Collections.list(
                                    java.net.NetworkInterface.getNetworkInterfaces())) {
                        for (var addr : java.util.Collections.list(iface.getInetAddresses())) {
                            if (addr.equals(addr1) || addr.equals(addr2)) {
                                return true;
                            }
                        }
                    }
                } catch (Exception ignored) {
                }
            } catch (Exception e) {
                return false;
            }
            return false;
        }
    }

    private static final class RawWisePartitionReader implements PartitionReader<InternalRow> {
        private static final int BATCH_SIZE = 4096;
        private final DynamicMmapStore store;
        private final TableStore tableStore;
        private final List<DataBlock> blocks;
        private final TableMeta tableMeta;
        private final StructType schema;
        private final List<UpdateLogEntry> updateLogs;
        private final ReadPruning.KeyPruningSpec pruningSpec;
        private final int fieldCount;
        private List<Record> currentRecords = List.of();
        private List<InternalRow> currentBatch = List.of();
        private List<InternalRow> pendingRows = List.of();
        private int index = -1;
        private int blockIndex = 0;
        private boolean exhausted = false;

        private final List<String> projectedColumns;

        private RawWisePartitionReader(
                DynamicMmapStore store,
                TableStore tableStore,
                List<DataBlock> blocks,
                TableMeta tableMeta,
                StructType schema,
                List<UpdateLogEntry> updateLogs,
                ReadPruning.KeyPruningSpec pruningSpec,
                List<String> projectedColumns) {
            this.store = store;
            this.tableStore = tableStore;
            this.blocks = blocks;
            this.tableMeta = tableMeta;
            this.schema = schema;
            this.updateLogs = updateLogs != null ? updateLogs : List.of();
            this.pruningSpec =
                    pruningSpec != null ? pruningSpec : ReadPruning.KeyPruningSpec.NO_PRUNING;
            this.projectedColumns = projectedColumns != null ? projectedColumns : List.of();
            this.fieldCount = schema != null ? schema.fields().length : 2;
        }

        @Override
        public boolean next() {
            Records.evictAll(currentRecords);
            index++;
            if (index < currentBatch.size()) {
                return true;
            }
            if (exhausted && pendingRows.isEmpty()) {
                return false;
            }
            return fetchNextBatch();
        }

        private boolean fetchNextBatch() {
            index = 0;
            currentBatch = List.of();

            if (!pendingRows.isEmpty()) {
                int end = Math.min(BATCH_SIZE, pendingRows.size());
                currentBatch = new java.util.ArrayList<>(pendingRows.subList(0, end));
                pendingRows =
                        end < pendingRows.size()
                                ? new java.util.ArrayList<>(
                                        pendingRows.subList(end, pendingRows.size()))
                                : List.of();
                Records.evictAll(currentRecords);
                currentRecords = List.of();
                return true;
            }

            List<byte[]> pointKeys = pruningSpec.pointKeys();
            boolean singleKey =
                    pointKeys.size() == 1
                            && tableMeta.primaryKey() != null
                            && !tableMeta.primaryKey().isEmpty();

            while (blockIndex < blocks.size()) {
                DataBlock block = blocks.get(blockIndex);
                blockIndex++;
                RawWiseTable tableRef =
                        new SparkRawWiseTableRef(
                                tableIdentifierForMeta(), tableMeta, DynamicTableInfo.EMPTY);
                try {
                    Read.Result result;
                    if (singleKey) {
                        String keyField = tableMeta.primaryKey().fields().get(0);
                        @SuppressWarnings("unchecked")
                        Map<String, Comparable> pkMap =
                                (Map<String, Comparable>) (Map) Map.of(keyField, pointKeys.get(0));
                        Find find =
                                Find.builder()
                                        .withTable(tableRef)
                                        .withDataBlocks(List.of(block))
                                        .withPrimaryKey(pkMap)
                                        .withReader(tableStore.reader())
                                        .build();
                        result = store.read(find);
                    } else {
                        KeyRange kr = block.keyRange();
                        if (kr != null
                                && kr.startKey() != null
                                && kr.startKey().length == 0
                                && kr.endKey() != null
                                && kr.endKey().length == 0) {
                            kr = KeyRange.FULL_RANGE;
                        }
                        KvScan scan =
                                KvScan.builder()
                                        .withTable(tableRef)
                                        .withDataBlocks(List.of(block))
                                        .withKeyRange(kr)
                                        .withProjections(projectedColumns)
                                        .withReader(tableStore.reader())
                                        .build();
                        result = store.read(scan);
                    }
                    log.debug(
                            "fetchNextBatch: table={} block={} success={} dataSize={}",
                            tableIdentifierForMeta(),
                            block.blockId(),
                            result.success(),
                            result.data() != null ? result.data().size() : -1);
                    if (!result.success()) {
                        continue;
                    }
                    List<Record> records = result.data();
                    currentRecords = records;
                    List<InternalRow> rows;
                    try {
                        rows =
                                records.stream()
                                        .map(
                                                record ->
                                                        SparkRecordConverter.toInternalRow(
                                                                record, schema))
                                        .filter(this::matchesPruning)
                                        .toList();
                    } catch (Exception e) {
                        Records.evictAll(currentRecords);
                        currentRecords = List.of();
                        throw e;
                    }
                    if (!rows.isEmpty()) {
                        List<InternalRow> allRows = applyUpdateLogsToRows(rows);
                        int end = Math.min(BATCH_SIZE, allRows.size());
                        currentBatch = new java.util.ArrayList<>(allRows.subList(0, end));
                        pendingRows =
                                end < allRows.size()
                                        ? new java.util.ArrayList<>(
                                                allRows.subList(end, allRows.size()))
                                        : List.of();
                        if (pendingRows.isEmpty()) {
                            Records.evictAll(currentRecords);
                            currentRecords = List.of();
                        }
                        return true;
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to read block " + block.blockId(), e);
                }
            }
            exhausted = true;
            return false;
        }

        private boolean matchesPruning(InternalRow row) {
            if (pruningSpec.noMatch()) {
                return false;
            }
            if (pruningSpec.fullScan() || pruningSpec == ReadPruning.KeyPruningSpec.NO_PRUNING) {
                return true;
            }
            List<byte[]> pointKeys = pruningSpec.pointKeys();
            if (pointKeys != null && !pointKeys.isEmpty()) {
                byte[] rowKey = rowKeyBytes(row);
                if (rowKey == null) return true;
                for (byte[] pk : pointKeys) {
                    if (java.util.Arrays.equals(rowKey, pk)) {
                        return true;
                    }
                }
                return false;
            }
            return true;
        }

        private byte[] rowKeyBytes(InternalRow row) {
            if (fieldCount == 0 || schema.fields().length == 0) return null;
            try {
                Object keyVal = row.get(0, schema.fields()[0].dataType());
                if (keyVal instanceof UTF8String utf8) {
                    return utf8.getBytes();
                }
                if (keyVal instanceof byte[] bytes) {
                    return bytes;
                }
                return null;
            } catch (Exception e) {
                log.trace("Failed to extract row key", e);
                return null;
            }
        }

        private String tableIdentifierForMeta() {
            if (blocks.isEmpty()) return tableMeta.tableName();
            // block id format: "{tableIdentifier}-{blockNum}-{start}-{end}"
            String id = blocks.get(0).blockId();
            int firstDash = id.indexOf('-');
            return firstDash > 0 ? id.substring(0, firstDash) : tableMeta.tableName();
        }

        private static final String RAWWISE_DELETED_MARKER = "_rawwise_deleted";

        @SuppressWarnings({"rawtypes", "unchecked"})
        private List<InternalRow> applyUpdateLogsToRows(List<InternalRow> rows) {
            if (updateLogs.isEmpty()) {
                return rows;
            }
            try {
                List<InternalRow> result = new java.util.ArrayList<>(rows.size());
                for (InternalRow row : rows) {
                    Map rowMap = new java.util.LinkedHashMap();
                    for (int i = 0; i < fieldCount; i++) {
                        String fieldName = schema.fields()[i].name();
                        Object val =
                                row.isNullAt(i) ? null : row.get(i, schema.fields()[i].dataType());
                        if (val instanceof UTF8String utf8) {
                            rowMap.put(fieldName, utf8.toString());
                        } else {
                            rowMap.put(fieldName, val);
                        }
                    }
                    boolean deleted = false;
                    for (UpdateLogEntry entry : updateLogs) {
                        if (entry.predicate() == null
                                || entry.predicate().evaluate(MapRecord.fromMap(rowMap))) {
                            if (entry.updates().isEmpty()) {
                                deleted = true;
                                break;
                            }
                            rowMap.putAll(entry.updates());
                        }
                    }
                    if (!deleted) {
                        result.add(mapToInternalRow(rowMap));
                    }
                }
                return result;
            } catch (Exception e) {
                log.error("Failed to apply update logs for {} rows, failing task", rows.size(), e);
                throw new RuntimeException("UpdateLog application failed: " + e.getMessage(), e);
            }
        }

        private GenericInternalRow mapToInternalRow(Map<String, Object> rowMap) {
            Object[] values = new Object[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
                String fieldName = schema.fields()[i].name();
                Object raw = rowMap.get(fieldName);
                values[i] = convertToSparkType(raw, schema.fields()[i].dataType());
            }
            return new GenericInternalRow(values);
        }

        @Override
        public InternalRow get() {
            return currentBatch.get(index);
        }

        @Override
        public void close() {
            Records.evictAll(currentRecords);
            currentBatch = List.of();
        }
    }

    private static final class UpdateLogPartitionReader implements PartitionReader<InternalRow> {
        private final java.util.List<java.util.Map<String, Comparable<?>>> rows;
        private final StructType schema;
        private final java.util.List<InternalRow> internalRows;
        private int index = -1;

        UpdateLogPartitionReader(
                java.util.List<java.util.Map<String, Comparable<?>>> rows, StructType schema) {
            this.rows = rows;
            this.schema = schema;
            this.internalRows = new java.util.ArrayList<>(rows.size());
            for (java.util.Map<String, Comparable<?>> row : rows) {
                internalRows.add(toInternalRow(row));
            }
        }

        @Override
        public boolean next() {
            index++;
            return index < internalRows.size();
        }

        @Override
        public InternalRow get() {
            return internalRows.get(index);
        }

        @Override
        public void close() {
            internalRows.clear();
        }

        private InternalRow toInternalRow(java.util.Map<String, Comparable<?>> row) {
            Object[] values = new Object[schema.fields().length];
            for (int i = 0; i < schema.fields().length; i++) {
                String fieldName = schema.fields()[i].name();
                Comparable<?> value = row.get(fieldName);
                values[i] = convertToSparkType(value, schema.fields()[i].dataType());
            }
            return new GenericInternalRow(values);
        }
    }

    private static Object convertToSparkType(
            Object value, org.apache.spark.sql.types.DataType dataType) {
        if (value == null) return null;
        if (dataType instanceof org.apache.spark.sql.types.StringType) {
            return UTF8String.fromString(String.valueOf(value));
        }
        if (dataType instanceof org.apache.spark.sql.types.IntegerType) {
            if (value instanceof Number n) return n.intValue();
            return Integer.parseInt(String.valueOf(value));
        }
        if (dataType instanceof org.apache.spark.sql.types.LongType) {
            if (value instanceof Number n) return n.longValue();
            return Long.parseLong(String.valueOf(value));
        }
        if (dataType instanceof org.apache.spark.sql.types.DoubleType) {
            if (value instanceof Number n) return n.doubleValue();
            return Double.parseDouble(String.valueOf(value));
        }
        if (dataType instanceof org.apache.spark.sql.types.FloatType) {
            if (value instanceof Number n) return n.floatValue();
            return Float.parseFloat(String.valueOf(value));
        }
        if (dataType instanceof org.apache.spark.sql.types.ShortType) {
            if (value instanceof Number n) return n.shortValue();
            return Short.parseShort(String.valueOf(value));
        }
        if (dataType instanceof org.apache.spark.sql.types.ByteType) {
            if (value instanceof Number n) return n.byteValue();
            return Byte.parseByte(String.valueOf(value));
        }
        if (dataType instanceof org.apache.spark.sql.types.BooleanType) {
            if (value instanceof Boolean b) return b;
            return Boolean.parseBoolean(String.valueOf(value));
        }
        if (dataType instanceof org.apache.spark.sql.types.DecimalType) {
            if (value instanceof java.math.BigDecimal bd) {
                return org.apache.spark.sql.types.Decimal.apply(bd);
            }
            return org.apache.spark.sql.types.Decimal.apply(
                    new java.math.BigDecimal(String.valueOf(value)));
        }
        if (dataType instanceof org.apache.spark.sql.types.DateType) {
            if (value instanceof Number n) return n.intValue();
            return Integer.parseInt(String.valueOf(value));
        }
        if (dataType instanceof org.apache.spark.sql.types.TimestampType) {
            if (value instanceof Number n) return n.longValue();
            return Long.parseLong(String.valueOf(value));
        }
        throw new UnsupportedOperationException("Unsupported Spark type: " + dataType);
    }

    record RawWiseWriteBuilder(
            String basePath,
            String storeId,
            String tableIdentifier,
            StructType schema,
            TableMeta tableMeta,
            TxnEngineConfig txnEngineConfig,
            ServerTxnCoordinator serverTxnCoordinator)
            implements WriteBuilder {

        @Override
        public BatchWrite buildForBatch() {
            return new RawWiseBatchWrite(
                    basePath,
                    storeId,
                    tableIdentifier,
                    schema,
                    tableMeta,
                    txnEngineConfig,
                    serverTxnCoordinator);
        }
    }

    static final class RawWiseBatchWrite implements BatchWrite, RequiresDistributionAndOrdering {
        private final String basePath;
        private final String storeId;
        private final String tableIdentifier;
        private final StructType schema;
        private final TableMeta tableMeta;
        private final TxnEngineConfig txnEngineConfig;
        private final ServerTxnCoordinator serverTxnCoordinator;

        RawWiseBatchWrite(
                String basePath,
                String storeId,
                String tableIdentifier,
                StructType schema,
                TableMeta tableMeta,
                TxnEngineConfig txnEngineConfig,
                ServerTxnCoordinator serverTxnCoordinator) {
            this.basePath = basePath;
            this.storeId = storeId;
            this.tableIdentifier = tableIdentifier;
            this.schema = schema;
            this.tableMeta = tableMeta;
            this.txnEngineConfig =
                    txnEngineConfig != null ? txnEngineConfig : new TxnEngineConfig();
            this.serverTxnCoordinator = serverTxnCoordinator;
        }

        @Override
        public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
            return new RawWiseDataWriterFactory(
                    basePath, storeId, tableIdentifier, schema, tableMeta, 0, txnEngineConfig);
        }

        @Override
        public Distribution requiredDistribution() {
            SparkConf conf = getSparkConfOrDefault();
            int shardCount = resolveShardCount(conf);
            if (shardCount <= 1) {
                return Distributions.unspecified();
            }
            String[] pkColumnNames = tableMeta.primaryKey().fields().toArray(new String[0]);
            return Distributions.clustered(
                    new org.apache.spark.sql.connector.expressions.Expression[] {
                        Expressions.bucket(shardCount, pkColumnNames)
                    });
        }

        @Override
        public boolean distributionStrictlyRequired() {
            return true;
        }

        @Override
        public int requiredNumPartitions() {
            SparkConf conf = getSparkConfOrDefault();
            return resolveShardCount(conf);
        }

        @Override
        public SortOrder[] requiredOrdering() {
            return new SortOrder[0];
        }

        @Override
        public void commit(WriterCommitMessage[] messages) {
            log.info(
                    "Batch write commit for table {}: {} writer messages received, engine={}",
                    tableIdentifier,
                    messages != null ? messages.length : 0,
                    txnEngineConfig.getEngine());

            Set<String> batchIds = new LinkedHashSet<>();
            long totalRows = 0;
            if (messages != null) {
                for (WriterCommitMessage msg : messages) {
                    if (msg instanceof RawWiseWriterCommitMessage m) {
                        if (m.batchId() != null && !m.batchId().isEmpty()) {
                            batchIds.add(m.batchId());
                        }
                        totalRows += m.count();
                    }
                }
            }
            for (String batchId : batchIds) {
                if (serverTxnCoordinator == null) {
                    log.warn(
                            "ServerTxnCoordinator not yet initialized, skipping batch commit for {} batches",
                            batchIds.size());
                    break;
                }
                var result = serverTxnCoordinator.commitBatch(batchId);
                log.info("ServerTxnCoordinator.commitBatch: batchId={} result={}", batchId, result);
            }
            log.info(
                    "Batch write plan-a commit completed for table {}: {} batches, {} total rows",
                    tableIdentifier,
                    batchIds.size(),
                    totalRows);
        }

        @Override
        public void abort(WriterCommitMessage[] messages) {
            log.warn(
                    "Ingestion batch aborted for table {}, per-shard aborts already sent by DataWriter.abort(). "
                            + "Timeout eviction will clean up any remaining orphaned transactions.",
                    tableIdentifier);
        }
    }

    private record RawWiseDataWriterFactory(
            String basePath,
            String storeId,
            String tableIdentifier,
            StructType schema,
            TableMeta tableMeta,
            int maxBatchRows,
            TxnEngineConfig txnEngineConfig)
            implements DataWriterFactory {

        @Override
        public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
            log.info(
                    "DataWriterFactory.createWriter: basePath={}, storeId={}, tableIdentifier={}, engine={}",
                    basePath,
                    storeId,
                    tableIdentifier,
                    txnEngineConfig != null ? txnEngineConfig.getEngine() : "null");
            DynamicMmapStore store = DynamicMmapStore.getInstance(basePath, storeId);
            log.info(
                    "DataWriterFactory: DynamicMmapStore key={}, existingTableStores={}",
                    basePath + ":" + storeId,
                    store.getAllTableIdentifiers());
            return new RawWiseDataWriter(
                    store,
                    basePath,
                    storeId,
                    tableIdentifier,
                    partitionId,
                    tableMeta,
                    schema,
                    maxBatchRows,
                    txnEngineConfig);
        }
    }

    private static final class RawWiseDataWriter implements DataWriter<InternalRow> {
        private static final long DEFAULT_INGESTION_BATCH_MAX_BYTES = 64L * 1024 * 1024;

        private final DynamicMmapStore store;
        private final String basePath;
        private final String storeId;
        private final String tableIdentifier;
        private final int partitionId;
        private final TableMeta tableMeta;
        private final KeyCodec keyCodec;
        private final RecordCodec recordCodec;
        private final io.github.rawwise.model.record.Schema rawwiseSchema;
        private final StructType sparkSchema;
        private final boolean localWrite;
        private long count;
        private final int shardCount;
        private final boolean assumePartitionIsShard;
        private final int maxBatchRows;
        private final SparkMurmur3ShardResolver shardResolver;
        private final RoutingWriteClient routingWriteClient;
        private final ShardOwnerCache shardOwnerCache;
        private final String currentExecutorId;
        private final String currentNodeId;
        private long flushSequence = 0L;

        private final HashMap<Integer, KvBatch> buffers = new HashMap<>();
        private int bufferedRows = 0;

        private TxnStoreBridge txnBridge;
        private String currentBatchId;

        private TxnStoreBridge txnBridge() {
            TxnStoreBridge current = store.getTxnBridge();
            if (current != this.txnBridge) {
                this.txnBridge = current;
                log.info("txnBridge refreshed: {}", current.getClass().getSimpleName());
            }
            return current;
        }

        private LmdbInstance getWritableInstance() {
            LmdbInstance instance = store.instancePool().currentLmdbInstance(tableIdentifier);
            if (instance == null) {
                throw new RuntimeException(
                        "Cannot acquire writable instance for table " + tableIdentifier);
            }
            return instance;
        }

        private LmdbInstance switchAndGetNewInstance() {
            if (!store.instancePool().switchToNewInstance(tableIdentifier)) {
                throw new RuntimeException(
                        "Cannot switch to new writable instance for table " + tableIdentifier);
            }
            LmdbInstance instance = store.instancePool().currentLmdbInstance(tableIdentifier);
            if (instance == null) {
                throw new RuntimeException(
                        "Cannot acquire writable instance after switch for table "
                                + tableIdentifier);
            }
            return instance;
        }

        private final long taskAttemptId;
        private final String taskAttemptIdStr;
        private final Set<Integer> writtenShards = new LinkedHashSet<>();
        private final Map<Integer, Long> shardEpochs = new HashMap<>();
        private final Map<Integer, Long> shardTxnIds = new HashMap<>();

        private RawWiseDataWriter(
                DynamicMmapStore store,
                String basePath,
                String storeId,
                String tableIdentifier,
                int partitionId,
                TableMeta tableMeta,
                StructType sparkSchema,
                int maxBatchRows,
                TxnEngineConfig txnEngineConfig) {
            this.store = store;
            this.basePath = basePath;
            this.storeId = storeId;
            this.tableIdentifier =
                    tableIdentifier != null && !tableIdentifier.isBlank()
                            ? tableIdentifier
                            : (tableMeta != null
                                            && tableMeta.tableName() != null
                                            && !tableMeta.tableName().isBlank()
                                    ? tableMeta.tableName()
                                    : "unknown");
            this.partitionId = partitionId;
            this.tableMeta = tableMeta;
            this.sparkSchema = sparkSchema;
            this.rawwiseSchema = tableMeta.schema();
            this.keyCodec = TableStore.createKeyCodec(tableMeta.primaryKey());
            List<String> pkFields =
                    tableMeta.primaryKey() != null ? tableMeta.primaryKey().fields() : List.of();
            RecordCodec.SchemaResolver schemaResolver = (tableId, schemaVersion) -> rawwiseSchema;
            TableStore ts = store.tableStore(this.tableIdentifier, pkFields, () -> schemaResolver);
            this.recordCodec = ts.getRecordCodec();
            SparkConf conf = getSparkConfOrDefault();
            this.localWrite = conf.getBoolean("spark.rawwise.write.local", false);
            this.shardCount = resolveShardCount(conf);
            this.assumePartitionIsShard =
                    conf.getBoolean(
                            RawWiseOptions.ROUTING_WRITE_ASSUME_PARTITION_IS_SHARD.key(),
                            RawWiseOptions.ROUTING_WRITE_ASSUME_PARTITION_IS_SHARD.defaultValue());
            this.maxBatchRows =
                    maxBatchRows > 0
                            ? maxBatchRows
                            : conf.getInt(
                                    RawWiseOptions.ROUTING_WRITE_BATCH_ROWS.key(),
                                    RawWiseOptions.ROUTING_WRITE_BATCH_ROWS.defaultValue());
            this.shardResolver = new SparkMurmur3ShardResolver(shardCount);

            this.routingWriteClient = localWrite ? null : new RoutingWriteClient();
            this.shardOwnerCache =
                    localWrite ? null : new ShardOwnerCache(routingWriteClient::lookupOwners);
            this.currentBatchId =
                    StableBatchId.forPendingBatches(this.tableIdentifier, partitionId, 0, Map.of());
            log.info(
                    "RawWiseDataWriter initialized: table={} partitionId={} batchId={}",
                    this.tableIdentifier,
                    partitionId,
                    currentBatchId);

            this.currentExecutorId = SparkEnv.get() != null ? SparkEnv.get().executorId() : null;
            this.currentNodeId = resolveCurrentNodeId();
            this.taskAttemptId =
                    TaskContext.get() != null ? TaskContext.get().taskAttemptId() : -1L;
            this.taskAttemptIdStr = String.valueOf(taskAttemptId);

            beginTransaction();
        }

        private void beginTransaction() {
            try {
                if (TxnContextHolder.hasActive()) {
                    log.warn(
                            "TxnContextHolder already active on write() thread, "
                                    + "skipping begin for table={}",
                            tableIdentifier);
                    return;
                }
                TxnContextHolder.ensureClockSet();
                TxnContextHolder.begin(
                        Set.of(tableIdentifier),
                        IsolationLevel.READ_COMMITTED,
                        null,
                        taskAttemptIdStr);
                TxnContext ctx = TxnContextHolder.current();
                ctx.setTxnMode(io.github.rawwise.engine.txn.TxnMode.WRITE_THROUGH);
                log.info(
                        "Transaction begun for write: table={} txnId={} mode=WRITE_THROUGH",
                        tableIdentifier,
                        ctx.txnId());
            } catch (Exception e) {
                log.error(
                        "Failed to begin transaction for table={}: {}",
                        tableIdentifier,
                        e.getMessage(),
                        e);
                throw new RuntimeException("Failed to begin write transaction", e);
            }
        }

        private long syntheticRowId = 0;
        private long sequenceNumber = 0;

        @Override
        public void write(InternalRow record) {
            Map<String, Object> fieldValues = extractFieldValues(record, sparkSchema);
            Map<String, Comparable> pkValues;
            if (tableMeta.primaryKey() == null || tableMeta.primaryKey().isEmpty()) {
                long rowId = (taskAttemptId << 20) | (syntheticRowId++ & 0xFFFFF);
                pkValues = Map.of("_rowid", rowId);
            } else {
                pkValues = extractPkValues(fieldValues, tableMeta.primaryKey());
            }
            byte[] keyBytes;
            try {
                keyBytes = keyCodec.encodePrimaryKey(pkValues);
            } catch (IOException e) {
                throw new RuntimeException("Failed to encode primary key", e);
            }

            byte[] valueBytes;
            Record.Key key = Records.keyOf(keyBytes);
            Object[] values = new Object[rawwiseSchema.fields().size()];
            for (int i = 0; i < rawwiseSchema.fields().size(); i++) {
                values[i] = fieldValues.get(rawwiseSchema.fields().get(i).name());
            }
            Record.Body body = new DefaultBody(rawwiseSchema, values);
            // commitTs ≡ txnId (unified strategy, see TxnOrchestratorImpl#L132).
            // TxnContext is guaranteed active here because beginTransaction() runs in init().
            long commitTs = TxnContextHolder.current().txnId();
            Record rec =
                    Records.create(
                            key,
                            body,
                            tableMeta.tableId(),
                            sequenceNumber++,
                            commitTs,
                            Record.Kind.INSERT,
                            true,
                            tableMeta.isAiNative(),
                            tableMeta.defaultImmutabilitySource());
            byte[] encoded = recordCodec.encode(rec);
            valueBytes = NullableValueCodec.encode(encoded);

            int shardId = resolveShardId(keyBytes);

            KvBatch batch = buffers.computeIfAbsent(shardId, ignored -> new KvBatch());
            batch.add(keyBytes, valueBytes);
            writtenShards.add(shardId);
            bufferedRows++;

            txnBridge().tryRecordWrite(keyBytes, valueBytes, 0, tableIdentifier);
            if (txnBridge().isPendingBatchFull(maxBatchRows, DEFAULT_INGESTION_BATCH_MAX_BYTES)) {
                flushBufferedWrites();
            }
        }

        private static Map<String, Object> extractFieldValues(
                InternalRow record, StructType schema) {
            StructField[] fields = schema.fields();
            Map<String, Object> result = new LinkedHashMap<>();
            for (int i = 0; i < fields.length; i++) {
                String fieldName = fields[i].name();
                if (record.isNullAt(i)) {
                    result.put(fieldName, null);
                } else {
                    org.apache.spark.sql.types.DataType dt = fields[i].dataType();
                    Object value = safeGet(record, i, dt);
                    value = normalizeToSchemaType(value, dt);
                    if (value instanceof UTF8String utf8) {
                        result.put(fieldName, utf8.toString());
                    } else {
                        result.put(fieldName, value);
                    }
                }
            }
            return result;
        }

        private static Object safeGet(
                InternalRow record, int ordinal, org.apache.spark.sql.types.DataType dt) {
            try {
                return record.get(ordinal, dt);
            } catch (ClassCastException e) {
                // Spark 4.0 may store Double for IntegerType / DateType columns
                // during expression evaluation (e.g. CAST(id AS INT) in INSERT SELECT).
                // getDouble() handles all numeric conversions safely.
                return record.getDouble(ordinal);
            }
        }

        private static Object normalizeToSchemaType(
                Object value, org.apache.spark.sql.types.DataType sparkType) {
            if (value == null) return null;
            if (sparkType instanceof org.apache.spark.sql.types.IntegerType
                    && value instanceof Number n
                    && !(value instanceof Integer)) {
                return n.intValue();
            }
            if (sparkType instanceof org.apache.spark.sql.types.LongType
                    && value instanceof Number n
                    && !(value instanceof Long)) {
                return n.longValue();
            }
            if (sparkType instanceof org.apache.spark.sql.types.FloatType
                    && value instanceof Number n
                    && !(value instanceof Float)) {
                return n.floatValue();
            }
            if (sparkType instanceof org.apache.spark.sql.types.DoubleType
                    && value instanceof Number n
                    && !(value instanceof Double)) {
                return n.doubleValue();
            }
            if (sparkType instanceof org.apache.spark.sql.types.ShortType
                    && value instanceof Number n
                    && !(value instanceof Short)) {
                return n.shortValue();
            }
            if (sparkType instanceof org.apache.spark.sql.types.ByteType
                    && value instanceof Number n
                    && !(value instanceof Byte)) {
                return n.byteValue();
            }
            if (sparkType instanceof org.apache.spark.sql.types.DateType
                    && value instanceof Number n
                    && !(value instanceof Integer)) {
                return n.intValue();
            }
            if (sparkType instanceof org.apache.spark.sql.types.DecimalType
                    && value instanceof Number n
                    && !(value instanceof java.math.BigDecimal)) {
                return new java.math.BigDecimal(n.toString());
            }
            return value;
        }

        private static Map<String, Comparable> extractPkValues(
                Map<String, Object> fieldValues, PrimaryKey pk) {
            Map<String, Comparable> result = new LinkedHashMap<>();
            for (String pkField : pk.fields()) {
                Object val = fieldValues.get(pkField);
                if (val == null) {
                    throw new IllegalArgumentException(
                            "Primary key field '" + pkField + "' must not be null");
                }
                if (!(val instanceof Comparable)) {
                    throw new IllegalArgumentException(
                            "Primary key field '"
                                    + pkField
                                    + "' value must be Comparable, got: "
                                    + val.getClass().getName());
                }
                result.put(pkField, (Comparable) val);
            }
            return result;
        }

        @Override
        public WriterCommitMessage commit() {
            flushBufferedWrites();
            txnBridge()
                    .commitTailBatch(
                            tableIdentifier,
                            tid -> getWritableInstance(),
                            tid -> switchAndGetNewInstance());

            TxnContextHolder.clear();
            return new RawWiseWriterCommitMessage(count, currentBatchId);
        }

        @Override
        public void abort() {
            TxnContextHolder.clear();
        }

        @Override
        public void close() {
            try {
                TxnContextHolder.clear();
            } catch (Exception e) {
                log.warn("Failed to close batch coordinator: {}", e.getMessage());
            }
        }

        private int resolveShardId(byte[] keyBytes) {
            if (assumePartitionIsShard
                    && shardCount > 1
                    && partitionId >= 0
                    && partitionId < shardCount) {
                return partitionId;
            }
            return shardResolver.resolve(keyBytes).value();
        }

        private void flushBufferedWrites() {
            if (buffers.isEmpty()) {
                return;
            }

            HashMap<Integer, KvBatch> pending = new HashMap<>(buffers);
            buffers.clear();
            bufferedRows = 0;

            txnBridge()
                    .flushPendingBatch(
                            tableIdentifier,
                            tid -> getWritableInstance(),
                            tid -> switchAndGetNewInstance());

            for (Map.Entry<Integer, KvBatch> e : pending.entrySet()) {
                count += e.getValue().size;
            }

            log.debug(
                    "flushBufferedWrites: table={} flushed {} shards",
                    tableIdentifier,
                    pending.size());
        }

        private boolean isSameNode(ShardAssignment assignment) {
            return assignment != null
                    && assignment.nodeSlot() != null
                    && currentNodeId != null
                    && currentNodeId.equals(assignment.nodeSlot().nodeId());
        }

        private static String resolveCurrentNodeId() {
            if (SparkEnv.get() == null
                    || SparkEnv.get().rpcEnv() == null
                    || SparkEnv.get().rpcEnv().address() == null) {
                return null;
            }
            return SparkEnv.get().rpcEnv().address().host();
        }
    }

    private static final class RawWiseWriterCommitMessage implements WriterCommitMessage {
        private final long count;
        private final String batchId;

        RawWiseWriterCommitMessage(long count, String batchId) {
            this.count = count;
            this.batchId = batchId;
        }

        long count() {
            return count;
        }

        String batchId() {
            return batchId;
        }
    }

    public static final class ServerTxnCoordinatorHolder {
        private static final AtomicReference<ServerTxnCoordinator> COORDINATOR =
                new AtomicReference<>();
        private static final AtomicReference<TxnEngineConfig> CONFIG = new AtomicReference<>();

        public static void setCoordinator(ServerTxnCoordinator coordinator) {
            COORDINATOR.set(coordinator);
        }

        public static ServerTxnCoordinator getCoordinator() {
            return COORDINATOR.get();
        }

        public static void setConfig(TxnEngineConfig config) {
            CONFIG.set(config);
        }

        public static TxnEngineConfig getConfig() {
            TxnEngineConfig config = CONFIG.get();
            return config != null ? config : new TxnEngineConfig();
        }

        public static synchronized void shutdown() {
            COORDINATOR.set(null);
            CONFIG.set(null);
        }
    }

    private static final class KvBatch implements StableBatchId.BatchLike {
        private byte[][] keys = new byte[128][];
        private byte[][] values = new byte[128][];
        private int size = 0;

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

        private byte[][] keysSlice() {
            return java.util.Arrays.copyOf(keys, size);
        }

        private byte[][] valuesSlice() {
            return java.util.Arrays.copyOf(values, size);
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public byte[] keyAt(int index) {
            return keys[index];
        }

        @Override
        public byte[] valueAt(int index) {
            return values[index];
        }
    }

    private record SparkRawWiseTableRef(String uniqId, TableMeta meta, DynamicTableInfo dynamicInfo)
            implements RawWiseTable {

        @Override
        public void close() {}

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void refresh() {}

        @Override
        public DynamicTableInfo dynamicInfo() {
            return dynamicInfo;
        }

        @Override
        public Snapshot currentSnapshot() {
            return null;
        }

        @Override
        public Snapshot snapshot(long snapshotId) {
            return null;
        }

        @Override
        public List<Snapshot> snapshots() {
            return List.of();
        }
    }
}
