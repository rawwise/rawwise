package io.github.rawwise.engine.txn.orchestrate;

import io.github.rawwise.backend.wal.WAL;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.ShardVersionTracker;
import io.github.rawwise.engine.txn.clock.AtomicLongClock;
import io.github.rawwise.engine.txn.clock.LogicalClock;
import io.github.rawwise.engine.txn.clock.MetastoreClockImpl;
import io.github.rawwise.engine.txn.state.TxnContextHolder;
import io.github.rawwise.engine.txn.store.VersionChainStore;
import io.github.rawwise.engine.txn.wal.InMemoryTxnWALWriter;
import io.github.rawwise.engine.txn.wal.NoOpTxnWALWriter;
import io.github.rawwise.engine.txn.wal.ScanBackedTxnWALReader;
import io.github.rawwise.engine.txn.wal.TxnWALReader;
import io.github.rawwise.engine.txn.wal.TxnWALWriter;
import io.github.rawwise.engine.txn.wal.WalBackedTxnWALWriter;
import io.github.rawwise.governance.config.TxnEngineConfig;
import io.github.rawwise.metastore.Metastore;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory wiring all dependencies for TxnOrchestrator.
 *
 * <p><b>IMPORTANT</b>: Each (catalog, database, table) triplet MUST share the same TxnOrchestrator
 * instance. Creating multiple instances for the same table leads to clock fork and silent data
 * corruption.
 *
 * <p>Uses {@link TxnEngineConfig} to select clock type:
 *
 * <ul>
 *   <li>{@link TxnEngineConfig.Engine#PLAN_A}: {@link MetastoreClockImpl} for cross-node
 *       consistency
 *   <li>{@link TxnEngineConfig.Engine#LEGACY}: {@link AtomicLongClock} for single-node/testing
 * </ul>
 */
@Slf4j
public final class TxnOrchestratorFactory {

    private static final String CLOCK_KEY_TEMPLATE = "/logical_clock/%s/%s/%s";

    private TxnOrchestratorFactory() {}

    /**
     * Create a fully-wired TxnOrchestrator.
     *
     * @param config transaction engine configuration
     * @param metastore Metastore instance
     * @param lmdbInstance LMDB instance
     * @param catalogName catalog name
     * @param databaseName database name
     * @param tableName table name (used for clock key in Plan A mode)
     * @return a ready-to-use TxnOrchestrator
     */
    public static TxnOrchestrator create(
            TxnEngineConfig config,
            Metastore metastore,
            LmdbInstance lmdbInstance,
            String catalogName,
            String databaseName,
            String tableName) {
        return create(config, metastore, lmdbInstance, catalogName, databaseName, tableName, null);
    }

    public static TxnOrchestrator create(
            TxnEngineConfig config,
            Metastore metastore,
            LmdbInstance lmdbInstance,
            String catalogName,
            String databaseName,
            String tableName,
            WAL wal) {
        return create(
                config, metastore, lmdbInstance, catalogName, databaseName, tableName, wal, null);
    }

    /**
     * Create orchestrator with optional per-key OCC tracker.
     *
     * @param tracker ShardVersionTracker for per-key OCC; null for table-level fallback
     */
    public static TxnOrchestrator create(
            TxnEngineConfig config,
            Metastore metastore,
            LmdbInstance lmdbInstance,
            String catalogName,
            String databaseName,
            String tableName,
            WAL wal,
            ShardVersionTracker tracker) {
        return create(
                config,
                metastore,
                lmdbInstance,
                catalogName,
                databaseName,
                tableName,
                wal,
                tracker,
                io.github.rawwise.engine.txn.OccConfig.DISABLED);
    }

    /** Create orchestrator with OCC config for per-key vs table-level switching. */
    public static TxnOrchestrator create(
            TxnEngineConfig config,
            Metastore metastore,
            LmdbInstance lmdbInstance,
            String catalogName,
            String databaseName,
            String tableName,
            WAL wal,
            ShardVersionTracker tracker,
            io.github.rawwise.engine.txn.OccConfig occConfig) {
        return createWithDurableStage(
                config,
                metastore,
                lmdbInstance,
                catalogName,
                databaseName,
                tableName,
                wal,
                tracker,
                occConfig,
                new DefaultDurableStage(true));
    }

    private static TxnOrchestrator createWithDurableStage(
            TxnEngineConfig config,
            Metastore metastore,
            LmdbInstance lmdbInstance,
            String catalogName,
            String databaseName,
            String tableName,
            WAL wal,
            ShardVersionTracker tracker,
            io.github.rawwise.engine.txn.OccConfig occConfig,
            DurableStage durableStage) {

        LogicalClock clock = createClock(config, metastore, catalogName, databaseName, tableName);

        TxnContextHolder.setClock(clock);
        log.info("LogicalClock set to TxnContextHolder for engine={}", config.getEngine());

        TxnWALWriter walWriter = selectWalWriter(config, wal);

        TxnWALReader walReader = new ScanBackedTxnWALReader(walWriter);

        VersionChainStore versionChainStore = new VersionChainStore();

        ValidateStage validateStage = new ValidateStage();
        WriteStage writeStage = new DefaultWriteStage();
        DefaultPublishStage publishStage = new DefaultPublishStage();

        if (tracker != null) {
            validateStage.setTracker(tracker);
            validateStage.setOccConfig(occConfig);
            validateStage.setWal(wal);
            publishStage.setTracker(tracker);
            log.info(
                    "ShardVersionTracker wired to ValidateStage + PublishStage for table={} (perKeyOCC={})",
                    tableName,
                    occConfig.isPerKey());
        }

        TxnOrchestratorImpl orchestrator =
                new TxnOrchestratorImpl(
                        clock,
                        metastore,
                        lmdbInstance,
                        versionChainStore,
                        walWriter,
                        validateStage,
                        writeStage,
                        durableStage,
                        publishStage,
                        catalogName,
                        databaseName,
                        config.getTxnTimeoutMs());
        orchestrator.setWalReader(walReader);
        if (tracker != null) {
            orchestrator.setTracker(tracker);
        }
        return orchestrator;
    }

    /**
     * create Catalog-level TxnOrchestrator.
     *
     * <p>this entry point accepts multiple tables' LMDB instances and ShardVersionTrackers,
     * construct a unified orchestrator. current stage(B1)internally still delegates to single-table
     * delegate orchestrator, B2 stage will refactor to dispatch by tableId-dispatched multi-table
     * commit pipeline.
     *
     * <h3>delegate choicestrategy</h3>
     *
     * <p>from resources.tables() choose first by tableId lexicographic order as delegate
     * bound"representative table", ensure semantic consistency across multiple creations.
     *
     * @param config transaction engine configuration
     * @param metastore Metastore instance
     * @param wal WAL (may be null, by walMode choice)
     * @param resources multi-table registered resources(catalogName / databaseName / tables Map)
     * @return CatalogLevelTxnOrchestrator instance
     */
    public static CatalogLevelTxnOrchestrator createCatalogLevel(
            TxnEngineConfig config,
            Metastore metastore,
            WAL wal,
            CatalogLevelTxnOrchestrator.CatalogResources resources) {
        if (resources.tables().isEmpty()) {
            throw new IllegalArgumentException("resources.tables() must not be empty");
        }
        // choose delegate table by tableId lexicographic order(deterministic)
        String delegateTableId =
                resources.tables().keySet().stream().min(Comparator.naturalOrder()).orElseThrow();
        CatalogLevelTxnOrchestrator.TableResource delegateRes =
                resources.tables().get(delegateTableId);

        log.info(
                "createCatalogLevel: catalog={}, db={}, registered tables={}, delegate-bound-to={}",
                resources.catalogName(),
                resources.databaseName(),
                resources.tables().keySet(),
                delegateTableId);

        TxnOrchestrator delegate =
                createWithDurableStage(
                        config,
                        metastore,
                        delegateRes.getLmdbInstance(),
                        resources.catalogName(),
                        resources.databaseName(),
                        tableNameOnly(delegateTableId),
                        wal,
                        delegateRes.getTracker(),
                        io.github.rawwise.engine.txn.OccConfig.DISABLED,
                        new DefaultDurableStage(false));

        return new CatalogLevelTxnOrchestratorImpl(resources, delegate, delegateTableId);
    }

    /** extract table segment from catalog.database.table. */
    private static String tableNameOnly(String tableId) {
        int idx = tableId.lastIndexOf('.');
        return idx < 0 ? tableId : tableId.substring(idx + 1);
    }

    /**
     * Create the appropriate LogicalClock based on engine config.
     *
     * <p>Plan A uses MetastoreClockImpl with CAS-based cross-node allocation. Legacy uses
     * AtomicLongClock for single-node deployments.
     */
    static LogicalClock createClock(
            TxnEngineConfig config, Metastore metastore, String catalog, String db, String table) {
        byte[] clockKey =
                String.format(CLOCK_KEY_TEMPLATE, catalog, db, table)
                        .getBytes(StandardCharsets.UTF_8);
        return switch (config.getEngine()) {
            case PLAN_A -> {
                int batchSize = config.getClockBatchSize();
                log.info(
                        "Creating MetastoreClockImpl for Plan A engine, key={}, batchSize={}",
                        new String(clockKey, StandardCharsets.UTF_8),
                        batchSize);
                yield new MetastoreClockImpl(metastore, clockKey, batchSize);
            }
            case LEGACY -> {
                log.info("Creating AtomicLongClock for Legacy engine (single-node)");
                yield new AtomicLongClock(0);
            }
        };
    }

    /**
     * by {@link TxnEngineConfig.WalMode} choose WAL implementation(H-6).
     *
     * <ul>
     *   <li>{@code NONE} → {@link NoOpTxnWALWriter}: completely skip WAL write
     *   <li>{@code LMDB_LOCAL} → {@link WalBackedTxnWALWriter}(if wal != null) or {@link
     *       InMemoryTxnWALWriter}(fallback, only test scenario)
     *   <li>{@code HDFS} → not yet implemented(fallback to LMDB_LOCAL behavior and alert)
     * </ul>
     */
    static TxnWALWriter selectWalWriter(TxnEngineConfig config, WAL wal) {
        TxnEngineConfig.WalMode mode = config.getWalMode();
        return switch (mode) {
            case NONE -> {
                log.info(
                        "WAL mode=NONE → NoOpTxnWALWriter (no persistence; relying on LMDB marker)");
                yield NoOpTxnWALWriter.INSTANCE;
            }
            case LMDB_LOCAL -> {
                if (wal != null) {
                    log.info("WAL mode=LMDB_LOCAL → WalBackedTxnWALWriter");
                    yield new WalBackedTxnWALWriter(wal);
                }
                log.warn(
                        "WAL mode=LMDB_LOCAL but no WAL provided → InMemoryTxnWALWriter (test-only fallback)");
                yield new InMemoryTxnWALWriter();
            }
            case HDFS -> {
                log.warn(
                        "WAL mode=HDFS not yet implemented; "
                                + "falling back to LMDB_LOCAL behavior");
                if (wal != null) {
                    yield new WalBackedTxnWALWriter(wal);
                }
                yield new InMemoryTxnWALWriter();
            }
        };
    }
}
