package io.github.rawwise.engine.storage;

import com.google.common.base.Throwables;
import io.github.rawwise.backend.routing.ShardRecoveryRecord;
import io.github.rawwise.backend.wal.DefaultWALRecord;
import io.github.rawwise.backend.wal.NwlWALReader;
import io.github.rawwise.backend.wal.TxnWALRecord;
import io.github.rawwise.backend.wal.TxnWALRecordCodec;
import io.github.rawwise.backend.wal.WALConfig;
import io.github.rawwise.backend.wal.WALRecord;
import io.github.rawwise.backend.wal.WALRecordCodec;
import io.github.rawwise.backend.wal.WALWriter;
import io.github.rawwise.engine.storage.block.ColdDataBlock;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.block.catalog.ManifestBackedBlockCatalog;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.storage.reader.ReaderConfig;
import io.github.rawwise.engine.storage.reader.ReaderContext;
import io.github.rawwise.engine.storage.reader.ReaderFactoryBootstrap;
import io.github.rawwise.engine.storage.reader.ReaderFactoryRegistry;
import io.github.rawwise.engine.storage.reader.ReaderType;
import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.engine.storage.table.TableStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

@Slf4j
public class RecoveryManager {

    public record RecoveryContext(
            StorageManager storageManager,
            String basePath,
            long instanceMappedBytes,
            Configuration hadoopConf,
            WALConfig walConfig,
            DurabilityLevel durabilityLevel,
            boolean failOnError,
            Supplier<Integer> writableNumSupplier,
            Function<String, TableStore> tableStoreFactory,
            BiConsumer<String, LmdbInstance> writableInstanceRegistrar,
            BiConsumer<String, LmdbInstance> readOnlyInstanceRegistrar,
            BiConsumer<Long, Long> memoryTracker,
            Function<String, LmdbInstance> currentWritableProvider,
            BiConsumer<String, CompletableFuture<String>> instanceRegistrar,
            BiFunction<String, Long, LmdbInstance> newInstanceCreator,
            Supplier<Boolean> memoryAvailableChecker,
            Runnable syncFlushTrigger) {
        public static RecoveryContextBuilder builder() {
            return new RecoveryContextBuilder();
        }
    }

    private final RecoveryContext ctx;
    private volatile io.github.rawwise.metastore.Metastore metastore;

    public RecoveryManager(RecoveryContext ctx) {
        this.ctx = ctx;
    }

    public void setMetastore(io.github.rawwise.metastore.Metastore metastore) {
        this.metastore = metastore;
    }

    public void loadExistInstance(
            Supplier<Boolean> memoryAvailableChecker,
            Runnable syncFlushTrigger,
            Function<String, TableStore> tableStoreLookup) {
        log.info("Starting to load existing LMDB instances from {}", ctx.basePath());
        try {
            if (!ctx.storageManager().localDirectoryExists(ctx.basePath())) {
                return;
            }
            List<String> instanceDirs = ctx.storageManager().listAllInstanceDirs(ctx.basePath());
            for (String instanceDir : instanceDirs) {
                String instanceId = instanceDir.substring(instanceDir.lastIndexOf('/') + 1);
                String tableIdentifier = parseTableIdentifier(instanceId);
                try {
                    if (memoryAvailableChecker != null && !memoryAvailableChecker.get()) {
                        if (syncFlushTrigger != null) {
                            syncFlushTrigger.run();
                        }
                        if (memoryAvailableChecker != null && !memoryAvailableChecker.get()) {
                            log.error("Still no memory after flush. Skip loading {}", instanceId);
                            continue;
                        }
                    }
                    Path instancePath = Paths.get(ctx.basePath(), instanceDir);
                    LmdbInstance instance =
                            new LmdbInstance(
                                    instanceId,
                                    instancePath.getParent(),
                                    ctx.instanceMappedBytes());
                    ctx.memoryTracker().accept(instance.getTotalUsageSize(), 0L);
                    ctx.instanceRegistrar()
                            .accept(instanceId, CompletableFuture.completedFuture(instanceId));

                    int writableNum = ctx.writableNumSupplier().get();
                    TableStore tableStore = tableStoreLookup.apply(tableIdentifier);

                    if (instance.readOnly()) {
                        ctx.readOnlyInstanceRegistrar().accept(tableIdentifier, instance);
                    } else if (tableStore.remainingWritable() < writableNum) {
                        ctx.writableInstanceRegistrar().accept(tableIdentifier, instance);
                    } else {
                        instance.markAsReadOnly();
                        ctx.readOnlyInstanceRegistrar().accept(tableIdentifier, instance);
                        log.warn(
                                "Max writable limit reached. Loaded instance {} as read-only.",
                                instanceId);
                    }
                    log.info("Loaded existing instance: {}", instanceId);
                } catch (Exception e) {
                    log.error(
                            "Failed to load instance: {}, exception is {}",
                            instanceId,
                            Throwables.getStackTraceAsString(e));
                }
            }
        } catch (IOException e) {
            log.error("Failed to scan existing instances from {}", ctx.basePath(), e);
        }

        if (ctx.walConfig() != null && ctx.durabilityLevel().isWalRequired()) {
            recoverFromWAL(ctx.failOnError());
        } else if (ctx.durabilityLevel() == DurabilityLevel.PROCESS) {
            log.info(
                    "Durability level is PROCESS, skipping WAL recovery. Data in LMDB is intact after process restart.");
        } else {
            log.warn(
                    "Durability level is NODE but WAL config is not available. "
                            + "This indicates a configuration inconsistency. WAL recovery skipped.");
        }

        recoverFastPathFromWAL();
    }

    public void recoverFastPathFromWAL() {
        String fastPathWalDir = ctx.basePath() + "/wal/fastpath";
        try {
            if (!ctx.storageManager().localDirectoryExists(fastPathWalDir)) {
                log.info(
                        "FastPath WAL directory does not exist, skipping recovery: {}",
                        fastPathWalDir);
                return;
            }
        } catch (IOException e) {
            log.info("FastPath WAL directory check failed, skipping recovery: {}", fastPathWalDir);
            return;
        }

        log.info("Starting FastPath WAL recovery from {}", fastPathWalDir);
        try {
            io.github.rawwise.engine.txn.wal.NwlBackedFastPathWalReader reader =
                    new io.github.rawwise.engine.txn.wal.NwlBackedFastPathWalReader(
                            fastPathWalDir, ctx.hadoopConf());
            List<io.github.rawwise.engine.txn.wal.FastPathWalReader.FastPathReplayEntry> entries =
                    reader.scanReplayEntries();

            if (entries.isEmpty()) {
                log.info("No FastPath WAL replay entries found");
                return;
            }

            int successCount = 0;
            int failureCount = 0;
            int skippedCount = 0;

            for (io.github.rawwise.engine.txn.wal.FastPathWalReader.FastPathReplayEntry entry :
                    entries) {
                try {
                    TableStore tableStore = ctx.tableStoreFactory().apply(entry.tableId());
                    if (tableStore == null) {
                        log.warn(
                                "FastPath WAL replay: no TableStore for table={}, skipping",
                                entry.tableId());
                        skippedCount++;
                        continue;
                    }

                    if ("DELETE".equals(entry.writeType()) || entry.valueBytes() == null) {
                        tableStore.getRawValue(entry.keyBytes());
                        skippedCount++;
                        continue;
                    }

                    LmdbInstance instance = ctx.currentWritableProvider().apply(entry.tableId());
                    if (instance == null) {
                        log.warn(
                                "FastPath WAL replay: no LMDB instance for table={}, skipping",
                                entry.tableId());
                        skippedCount++;
                        continue;
                    }

                    io.github.rawwise.engine.storage.LmdbInstance.PutStatus putStatus =
                            instance.put(entry.keyBytes(), entry.valueBytes());
                    if (putStatus
                            == io.github.rawwise.engine.storage.LmdbInstance.PutStatus.SUCCESS) {
                        successCount++;
                    } else {
                        log.warn(
                                "FastPath WAL replay: put failed for table={} keyLen={} status={}",
                                entry.tableId(),
                                entry.keyBytes().length,
                                putStatus);
                        failureCount++;
                    }
                } catch (Exception e) {
                    log.warn(
                            "FastPath WAL replay: failed for table={} commitTs={}: {}",
                            entry.tableId(),
                            entry.commitTs(),
                            e.getMessage());
                    failureCount++;
                }
            }

            log.info(
                    "FastPath WAL recovery completed — total={}, success={}, failure={}, skipped={}",
                    entries.size(),
                    successCount,
                    failureCount,
                    skippedCount);
        } catch (Exception e) {
            log.error("FastPath WAL recovery failed", e);
        }
    }

    public void recoverFromWAL() {
        recoverFromWAL(true);
    }

    public void recoverFromWAL(boolean failOnError) {
        String walPath;
        boolean isHdfs = ctx.walConfig() != null && ctx.walConfig().isHdfsEnabled();

        if (isHdfs) {
            walPath = ctx.walConfig().hdfsBasePath() + "/" + ctx.walConfig().executorId();
            log.info("Starting WAL recovery from HDFS: {}", walPath);
        } else {
            walPath = ctx.basePath() + "/wal";
            try {
                if (!ctx.storageManager().localDirectoryExists(walPath)) {
                    log.info("WAL directory does not exist, skipping recovery: {}", walPath);
                    return;
                }
            } catch (IOException e) {
                log.info("WAL directory check failed, skipping recovery: {}", walPath);
                return;
            }
            log.info("Starting WAL recovery from local: {}", walPath);
        }

        int totalRecords = 0;
        int successCount = 0;
        int failureCount = 0;
        int skippedCount = 0;

        try {
            NwlWALReader reader = createWALReader(walPath, ctx.hadoopConf());
            List<WALRecord> unflushedRecords = reader.readUnflushed();
            reader.close();

            if (unflushedRecords.isEmpty()) {
                log.info("No unflushed WAL records found");
                return;
            }

            totalRecords = unflushedRecords.size();
            log.info("Found {} unflushed WAL records to replay", totalRecords);

            Map<String, List<WALRecord>> recordsByTable =
                    unflushedRecords.stream()
                            .filter(r -> r.value().length > 0)
                            .collect(
                                    Collectors.groupingBy(
                                            record -> {
                                                String tableId =
                                                        extractTableIdFromWALRecord(record.value());
                                                return tableId != null ? tableId : "unknown";
                                            }));

            ReplayResult replayResult = replayWALRecordsWithStats(recordsByTable);
            successCount = replayResult.successCount;
            failureCount = replayResult.failureCount;
            skippedCount = replayResult.skippedCount;

            if (!isHdfs) {
                ctx.storageManager().cleanupWALFiles("wal");
            }
            log.info(
                    "WAL recovery completed — total={}, success={}, failure={}, skipped={}"
                            + (isHdfs
                                    ? " (HDFS files retained for cross-node recovery)"
                                    : " and cleanup done"),
                    totalRecords,
                    successCount,
                    failureCount,
                    skippedCount);

        } catch (Exception e) {
            log.error("WAL recovery failed", e);
            if (failOnError && ctx.durabilityLevel().isWalRequired()) {
                throw new RuntimeException(
                        "WAL recovery failed in NODE durability mode, blocking startup", e);
            }
        }

        if (failOnError && ctx.durabilityLevel().isWalRequired() && failureCount > 0) {
            throw new RuntimeException(
                    "WAL recovery had "
                            + failureCount
                            + " failures in NODE durability mode, blocking startup. "
                            + "Set -Drawwise.storage.recovery.fail-on-error=false to continue.");
        }
    }

    private record ReplayResult(int successCount, int failureCount, int skippedCount) {}

    private ReplayResult replayWALRecordsWithStats(Map<String, List<WALRecord>> recordsByTable) {
        int totalRecords = 0;
        int failedRecords = 0;
        int skippedRecords = 0;

        for (Map.Entry<String, List<WALRecord>> entry : recordsByTable.entrySet()) {
            String tableIdentifier = entry.getKey();
            List<WALRecord> records = entry.getValue();

            log.info("Replaying {} WAL records for table {}", records.size(), tableIdentifier);

            TableStore tableStore = ctx.tableStoreFactory().apply(tableIdentifier);
            LmdbInstance instance = ctx.currentWritableProvider().apply(tableIdentifier);
            if (instance == null) {
                if (ctx.newInstanceCreator() != null) {
                    if (ctx.memoryAvailableChecker() != null
                            && !ctx.memoryAvailableChecker().get()) {
                        if (ctx.syncFlushTrigger() != null) {
                            ctx.syncFlushTrigger().run();
                        }
                        if (ctx.memoryAvailableChecker() != null
                                && !ctx.memoryAvailableChecker().get()) {
                            log.warn(
                                    "Cannot create new instance for table {}: memory unavailable",
                                    tableIdentifier);
                            skippedRecords += records.size();
                            continue;
                        }
                    }
                    try {
                        instance =
                                ctx.newInstanceCreator()
                                        .apply(tableIdentifier, ctx.instanceMappedBytes());
                        if (instance != null) {
                            ctx.memoryTracker().accept(instance.getTotalUsageSize(), 0L);
                            ctx.instanceRegistrar()
                                    .accept(
                                            instance.instanceId(),
                                            CompletableFuture.completedFuture(
                                                    instance.instanceId()));
                            ctx.writableInstanceRegistrar().accept(tableIdentifier, instance);
                            log.info(
                                    "Created new instance for table {} during WAL replay",
                                    tableIdentifier);
                        }
                    } catch (Exception e) {
                        log.error(
                                "Failed to create new instance for table {} during WAL replay: {}",
                                tableIdentifier,
                                e.getMessage());
                    }
                }
                if (instance == null) {
                    log.warn(
                            "Cannot replay WAL for table {}: no writable instance available and newInstanceCreator failed, skipping {} records",
                            tableIdentifier,
                            records.size());
                    skippedRecords += records.size();
                    continue;
                }
            }

            for (WALRecord record : records) {
                totalRecords++;
                try {
                    byte[] key = extractKeyFromWALRecord(record);
                    byte[] encodedValue = extractValueFromWALRecord(record.value());
                    LmdbInstance.PutStatus status = instance.put(key, encodedValue);
                    if (status != LmdbInstance.PutStatus.SUCCESS) {
                        log.warn(
                                "WAL replay put returned {} for key in table {} at seq={}",
                                status,
                                tableIdentifier,
                                record.sequence());
                        failedRecords++;
                    }
                } catch (Exception e) {
                    log.error(
                            "Failed to replay WAL record for table {} at seq={}: {}",
                            tableIdentifier,
                            record.sequence(),
                            e.getMessage());
                    failedRecords++;
                }
            }
        }

        if (totalRecords > 0 && (double) failedRecords / totalRecords > 0.1) {
            throw new RuntimeException(
                    "WAL replay failure rate exceeded 10%: "
                            + failedRecords
                            + "/"
                            + totalRecords
                            + " records failed. "
                            + "Aborting recovery to prevent inconsistent state.");
        }

        if (failedRecords > 0) {
            log.warn(
                    "WAL replay completed with {} failures out of {} records",
                    failedRecords,
                    totalRecords);
        }

        return new ReplayResult(
                totalRecords - failedRecords - skippedRecords, failedRecords, skippedRecords);
    }

    public void recoverWithTxnBoundaries(
            Function<Long, Boolean> txnCommitChecker,
            WALWriter walWriter,
            BiConsumer<String, Long> versionTrackerRestorer) {
        String walPath;
        boolean isHdfs = ctx.walConfig() != null && ctx.walConfig().isHdfsEnabled();

        if (isHdfs) {
            walPath = ctx.walConfig().hdfsBasePath() + "/" + ctx.walConfig().executorId();
            log.info("Starting txn-boundary WAL recovery from HDFS: {}", walPath);
        } else {
            walPath = ctx.basePath() + "/wal";
            try {
                if (!ctx.storageManager().localDirectoryExists(walPath)) {
                    log.info(
                            "WAL directory does not exist, skipping txn-boundary recovery: {}",
                            walPath);
                    return;
                }
            } catch (IOException e) {
                log.info("WAL directory check failed, skipping txn-boundary recovery: {}", walPath);
                return;
            }
            log.info("Starting txn-boundary WAL recovery from local: {}", walPath);
        }

        try {
            NwlWALReader reader = createWALReader(walPath, ctx.hadoopConf());
            List<WALRecord> unflushedRecords = reader.readUnflushed();
            reader.close();

            if (unflushedRecords.isEmpty()) {
                log.info("No unflushed WAL records found for txn-boundary recovery");
                return;
            }

            recoverWithTxnBoundaries(
                    unflushedRecords, txnCommitChecker, walWriter, versionTrackerRestorer);

            if (!isHdfs) {
                ctx.storageManager().cleanupWALFiles("wal");
            }
            log.info(
                    "Txn-boundary WAL recovery completed"
                            + (isHdfs
                                    ? " (HDFS files retained for cross-node recovery)"
                                    : " and cleanup done"));

        } catch (Exception e) {
            log.error("Txn-boundary WAL recovery failed", e);
        }
    }

    void recoverWithTxnBoundaries(
            List<WALRecord> unflushedRecords,
            Function<Long, Boolean> txnCommitChecker,
            WALWriter walWriter,
            BiConsumer<String, Long> versionTrackerRestorer) {
        boolean hasTxnRecords = unflushedRecords.stream().anyMatch(r -> isTxnWALRecord(r.value()));

        if (!hasTxnRecords) {
            log.info("No TXN boundary records found, falling back to legacy WAL recovery");
            Map<String, List<WALRecord>> recordsByTable =
                    unflushedRecords.stream()
                            .filter(r -> r.value().length > 0)
                            .collect(
                                    Collectors.groupingBy(
                                            record -> {
                                                String tableId =
                                                        extractTableIdFromWALRecord(record.value());
                                                return tableId != null ? tableId : "unknown";
                                            }));
            replayWALRecords(recordsByTable);
            return;
        }

        Map<Long, List<TxnWALRecord>> txnGroups = groupTxnRecordsByTxnId(unflushedRecords);
        log.info("Found {} transaction groups in WAL", txnGroups.size());

        Map<Long, List<TxnWALRecord>> committedTxns = new LinkedHashMap<>();
        Map<Long, List<TxnWALRecord>> abortedTxns = new LinkedHashMap<>();
        Map<Long, List<TxnWALRecord>> preparedTxns = new LinkedHashMap<>();
        Map<Long, List<TxnWALRecord>> uncompletedTxns = new LinkedHashMap<>();

        for (Map.Entry<Long, List<TxnWALRecord>> entry : txnGroups.entrySet()) {
            Long txnId = entry.getKey();
            List<TxnWALRecord> records = entry.getValue();

            boolean hasCommit =
                    records.stream().anyMatch(r -> r.type() == TxnWALRecord.Type.TXN_COMMIT);
            boolean hasAbort =
                    records.stream().anyMatch(r -> r.type() == TxnWALRecord.Type.TXN_ABORT);
            boolean hasPrepared =
                    records.stream().anyMatch(r -> r.type() == TxnWALRecord.Type.TXN_PREPARED);

            if (hasCommit) {
                committedTxns.put(txnId, records);
            } else if (hasAbort) {
                abortedTxns.put(txnId, records);
            } else if (hasPrepared) {
                preparedTxns.put(txnId, records);
            } else {
                uncompletedTxns.put(txnId, records);
            }
        }

        for (Map.Entry<Long, List<TxnWALRecord>> entry : uncompletedTxns.entrySet()) {
            Long txnId = entry.getKey();
            List<TxnWALRecord> records = entry.getValue();
            String tableId = resolveTableIdFromTxnRecords(records);
            writeTxnAbortToWAL(txnId, tableId, walWriter);
            log.info("Marked uncompleted transaction {} as ABORTED", txnId);
        }

        for (Map.Entry<Long, List<TxnWALRecord>> entry : preparedTxns.entrySet()) {
            Long txnId = entry.getKey();
            List<TxnWALRecord> records = entry.getValue();
            handlePreparedTransaction(txnId, records, txnCommitChecker, walWriter);
        }

        for (Map.Entry<Long, List<TxnWALRecord>> entry : committedTxns.entrySet()) {
            replayTxnDataToLMDB(entry.getValue());
            log.info("Replayed committed transaction {}", entry.getKey());
        }

        if (versionTrackerRestorer != null) {
            restoreShardVersionTracker(committedTxns, preparedTxns, versionTrackerRestorer);
        }
    }

    static boolean isTxnWALRecord(byte[] data) {
        if (data == null || data.length == 0) {
            return false;
        }
        byte firstByte = data[0];
        // v1 format: type byte in range [TXN_BEGIN, TXN_ABORT]
        if (firstByte >= WALRecordCodec.RECORD_TYPE_TXN_BEGIN
                && firstByte <= WALRecordCodec.RECORD_TYPE_TXN_ABORT) {
            return true;
        }
        // v2 format: type byte has V2_MARKER (0x80) set, e.g. 0x80 | TXN_DATA = 0x84
        if (TxnWALRecordCodec.isV2Format(data)) {
            return true;
        }
        return false;
    }

    private Map<Long, List<TxnWALRecord>> groupTxnRecordsByTxnId(List<WALRecord> records) {
        Map<Long, List<TxnWALRecord>> txnGroups = new LinkedHashMap<>();
        for (WALRecord record : records) {
            if (record.value() == null || record.value().length == 0) {
                continue;
            }
            if (!isTxnWALRecord(record.value())) {
                continue;
            }
            try {
                TxnWALRecord txnRecord = TxnWALRecordCodec.decode(record.value());
                txnGroups.computeIfAbsent(txnRecord.txnId(), k -> new ArrayList<>()).add(txnRecord);
            } catch (Exception e) {
                log.warn("Failed to decode txn WAL record at seq={}", record.sequence(), e);
            }
        }
        return txnGroups;
    }

    private void handlePreparedTransaction(
            Long txnId,
            List<TxnWALRecord> records,
            Function<Long, Boolean> txnCommitChecker,
            WALWriter walWriter) {
        boolean metastoreCommitted = false;
        try {
            metastoreCommitted = txnCommitChecker != null && txnCommitChecker.apply(txnId);
        } catch (Exception e) {
            log.warn("Metastore query failed for txn {}, treating as not committed", txnId, e);
        }

        if (metastoreCommitted) {
            writeTxnCommitToWAL(txnId, records, walWriter);
            replayTxnDataToLMDB(records);
            log.info("Resolved PREPARED transaction {} as COMMITTED (Metastore confirmed)", txnId);
        } else {
            String tableId = resolveTableIdFromTxnRecords(records);
            writeTxnAbortToWAL(txnId, tableId, walWriter);
            deleteTxnKeysFromLMDB(records);
            log.info("Resolved PREPARED transaction {} as ABORTED (Metastore denied)", txnId);
        }
    }

    private void replayTxnDataToLMDB(List<TxnWALRecord> records) {
        Map<String, List<TxnWALRecord>> dataByTable = new LinkedHashMap<>();
        for (TxnWALRecord record : records) {
            if (record.type() == TxnWALRecord.Type.TXN_DATA) {
                String tableId = record.tableId() != null ? record.tableId() : "unknown";
                dataByTable.computeIfAbsent(tableId, k -> new ArrayList<>()).add(record);
            }
        }

        for (Map.Entry<String, List<TxnWALRecord>> entry : dataByTable.entrySet()) {
            String tableId = entry.getKey();
            List<TxnWALRecord> dataRecords = entry.getValue();
            LmdbInstance instance = ctx.currentWritableProvider().apply(tableId);
            if (instance == null) {
                log.error(
                        "Cannot replay txn data for table {}: no writable instance available",
                        tableId);
                continue;
            }

            for (TxnWALRecord dataRecord : dataRecords) {
                try {
                    byte[] key = dataRecord.key();
                    byte[] encodedValue = dataRecord.value();
                    LmdbInstance.PutStatus status = instance.put(key, encodedValue);
                    if (status != LmdbInstance.PutStatus.SUCCESS) {
                        log.warn(
                                "Txn data replay failed for key in table {}: status={}",
                                tableId,
                                status);
                    }
                } catch (Exception e) {
                    log.error(
                            "Failed to replay txn data record for table {}: {}",
                            tableId,
                            e.getMessage());
                }
            }
        }
    }

    private void deleteTxnKeysFromLMDB(List<TxnWALRecord> records) {
        Map<String, List<byte[]>> keysByTable = new LinkedHashMap<>();
        for (TxnWALRecord record : records) {
            if (record.type() == TxnWALRecord.Type.TXN_DATA) {
                String tableId = record.tableId() != null ? record.tableId() : "unknown";
                if (record.key() != null) {
                    keysByTable.computeIfAbsent(tableId, k -> new ArrayList<>()).add(record.key());
                }
            }
        }

        for (Map.Entry<String, List<byte[]>> entry : keysByTable.entrySet()) {
            String tableId = entry.getKey();
            List<byte[]> keys = entry.getValue();
            LmdbInstance instance = ctx.currentWritableProvider().apply(tableId);
            if (instance == null) {
                log.error(
                        "Cannot delete txn keys for table {}: no writable instance available",
                        tableId);
                continue;
            }
            try {
                LmdbInstance.BatchDeleteResult result = instance.batchDelete(keys);
                log.info(
                        "Deleted {} keys from LMDB for table {} (not found: {})",
                        result.deletedCount(),
                        tableId,
                        result.notFoundCount());
            } catch (Exception e) {
                log.error("Failed to delete txn keys from LMDB for table {}", tableId, e);
            }
        }
    }

    private void writeTxnAbortToWAL(Long txnId, String tableId, WALWriter walWriter) {
        if (walWriter == null) {
            log.warn("No WALWriter available, skipping TXN_ABORT write for txn {}", txnId);
            return;
        }
        try {
            TxnWALRecord abortRecord =
                    TxnWALRecord.abort(txnId, tableId != null ? tableId : "unknown");
            byte[] encoded = TxnWALRecordCodec.encode(abortRecord);
            WALRecord walRecord =
                    new DefaultWALRecord(
                            System.nanoTime(),
                            encoded,
                            System.currentTimeMillis(),
                            -1L,
                            tableId,
                            "DATA");
            walWriter.write(walRecord);
            walWriter.forceSync();
        } catch (Exception e) {
            log.error("Failed to write TXN_ABORT to WAL for txn {}", txnId, e);
        }
    }

    private void writeTxnCommitToWAL(Long txnId, List<TxnWALRecord> records, WALWriter walWriter) {
        if (walWriter == null) {
            log.warn("No WALWriter available, skipping TXN_COMMIT write for txn {}", txnId);
            return;
        }
        try {
            long commitTs = 0L;
            String tableId = "unknown";
            for (TxnWALRecord record : records) {
                if (record.type() == TxnWALRecord.Type.TXN_PREPARED) {
                    commitTs = record.commitTs();
                    if (record.tableId() != null) {
                        tableId = record.tableId();
                    }
                }
            }
            TxnWALRecord commitRecord = TxnWALRecord.commit(txnId, tableId, commitTs, 0L);
            byte[] encoded = TxnWALRecordCodec.encode(commitRecord);
            WALRecord walRecord =
                    new DefaultWALRecord(
                            System.nanoTime(),
                            encoded,
                            System.currentTimeMillis(),
                            -1L,
                            tableId,
                            "DATA");
            walWriter.write(walRecord);
            walWriter.forceSync();
        } catch (Exception e) {
            log.error("Failed to write TXN_COMMIT to WAL for txn {}", txnId, e);
        }
    }

    private String resolveTableIdFromTxnRecords(List<TxnWALRecord> records) {
        for (TxnWALRecord record : records) {
            if (record.tableId() != null) {
                return record.tableId();
            }
        }
        for (TxnWALRecord record : records) {
            if (record.tableIds() != null && !record.tableIds().isEmpty()) {
                return record.tableIds().get(0);
            }
        }
        return "unknown";
    }

    private void restoreShardVersionTracker(
            Map<Long, List<TxnWALRecord>> committedTxns,
            Map<Long, List<TxnWALRecord>> preparedTxns,
            BiConsumer<String, Long> versionTrackerRestorer) {
        Map<String, Long> maxCommitTsByTable = new HashMap<>();
        for (List<TxnWALRecord> records : committedTxns.values()) {
            for (TxnWALRecord record : records) {
                if (record.type() == TxnWALRecord.Type.TXN_COMMIT && record.commitTs() > 0) {
                    String tableId = record.tableId() != null ? record.tableId() : "unknown";
                    maxCommitTsByTable.merge(tableId, record.commitTs(), Long::max);
                }
            }
        }
        for (List<TxnWALRecord> records : preparedTxns.values()) {
            for (TxnWALRecord record : records) {
                if (record.type() == TxnWALRecord.Type.TXN_PREPARED && record.commitTs() > 0) {
                    String tableId = record.tableId() != null ? record.tableId() : "unknown";
                    maxCommitTsByTable.merge(tableId, record.commitTs(), Long::max);
                }
            }
        }
        for (Map.Entry<String, Long> entry : maxCommitTsByTable.entrySet()) {
            try {
                versionTrackerRestorer.accept(entry.getKey(), entry.getValue());
            } catch (Exception e) {
                log.error("Failed to restore ShardVersionTracker for table {}", entry.getKey(), e);
            }
        }
    }

    public void recoverFromLocalLMDB(
            String localBasePath,
            String tableIdentifier,
            Supplier<Boolean> memoryAvailableChecker,
            Runnable syncFlushTrigger) {
        try {
            if (!ctx.storageManager().localDirectoryExists(localBasePath)) {
                log.warn("Local LMDB path does not exist for recovery: {}", localBasePath);
                return;
            }
        } catch (IOException e) {
            log.warn("Failed to check local LMDB path for recovery: {}", localBasePath, e);
            return;
        }

        log.info(
                "Starting local LMDB recovery from {} for table {}",
                localBasePath,
                tableIdentifier);
        try {
            List<String> instanceDirs =
                    ctx.storageManager().listInstanceDirsForTable(localBasePath, tableIdentifier);
            for (String instanceId : instanceDirs) {
                try {
                    if (metastore != null) {
                        byte[] markBytes =
                                metastore
                                        .get(
                                                io.github.rawwise.common.catalog.Keys
                                                        .flushedInstanceKey(
                                                                tableIdentifier, instanceId))
                                        .orElse(null);
                        if (markBytes != null) {
                            FlushedInstanceMark mark = FlushedInstanceMark.fromBytes(markBytes);
                            if (mark.status() == FlushedInstanceMark.FlushStatus.FLUSHED
                                    || mark.status() == FlushedInstanceMark.FlushStatus.DELETING
                                    || mark.status() == FlushedInstanceMark.FlushStatus.DELETED) {
                                log.info(
                                        "Skipping flushed instance {} (status={}), scheduling dir cleanup",
                                        instanceId,
                                        mark.status());
                                scheduleDirCleanup(
                                        Paths.get(localBasePath, tableIdentifier, instanceId));
                                continue;
                            }
                        }
                    }

                    Path instancePath = Paths.get(localBasePath, tableIdentifier, instanceId);
                    if (!Files.exists(instancePath.resolve("lock.mdb"))) {
                        log.warn(
                                "Instance {} has no lock.mdb (incomplete destroy), scheduling dir cleanup",
                                instanceId);
                        scheduleDirCleanup(instancePath);
                        continue;
                    }

                    if (memoryAvailableChecker != null && !memoryAvailableChecker.get()) {
                        if (syncFlushTrigger != null) {
                            syncFlushTrigger.run();
                        }
                        if (!memoryAvailableChecker.get()) {
                            log.error(
                                    "Still no memory after flush during local LMDB recovery. Skip loading {}",
                                    instanceId);
                            continue;
                        }
                    }
                    LmdbInstance instance =
                            new LmdbInstance(
                                    instanceId,
                                    instancePath.getParent(),
                                    ctx.instanceMappedBytes());
                    ctx.memoryTracker().accept(instance.getTotalUsageSize(), 0L);
                    ctx.instanceRegistrar()
                            .accept(instanceId, CompletableFuture.completedFuture(instanceId));

                    int writableNum = ctx.writableNumSupplier().get();
                    TableStore tableStore = ctx.tableStoreFactory().apply(tableIdentifier);

                    if (instance.readOnly()) {
                        ctx.readOnlyInstanceRegistrar().accept(tableIdentifier, instance);
                    } else if (tableStore.remainingWritable() < writableNum) {
                        ctx.writableInstanceRegistrar().accept(tableIdentifier, instance);
                    } else {
                        instance.markAsReadOnly();
                        ctx.readOnlyInstanceRegistrar().accept(tableIdentifier, instance);
                    }
                    log.info("Recovered local LMDB instance: {}", instanceId);
                } catch (Exception e) {
                    log.error("Failed to recover local LMDB instance: {}", instanceId, e);
                }
            }
        } catch (IOException e) {
            log.error("Failed to scan local LMDB for recovery: {}", localBasePath, e);
        }
    }

    private void scheduleDirCleanup(Path dir) {
        try {
            if (Files.exists(dir)) {
                Files.walk(dir)
                        .sorted(java.util.Comparator.reverseOrder())
                        .forEach(
                                p -> {
                                    try {
                                        Files.deleteIfExists(p);
                                    } catch (Exception ignored) {
                                    }
                                });
                log.info("Cleaned up instance directory: {}", dir);
            }
        } catch (IOException e) {
            log.warn("Failed to clean up instance directory: {}", dir, e);
        }
    }

    public void recoverFromRemoteWAL(
            String walHdfsDir,
            String tableIdentifier,
            Supplier<Boolean> memoryAvailableChecker,
            Runnable syncFlushTrigger) {
        log.info("Starting remote WAL recovery from {} for table {}", walHdfsDir, tableIdentifier);

        try {
            if (!ctx.storageManager().hdfsAvailable()) {
                log.warn("HDFS storage not available for remote WAL recovery");
                return;
            }

            if (!ctx.storageManager().hdfsExists(walHdfsDir)) {
                log.warn("WAL HDFS directory does not exist: {}", walHdfsDir);
                return;
            }

            String dataHdfsDir = walHdfsDir.replace("/wal/", "/data/").replace("/wal", "/data");
            if (ctx.storageManager().hdfsExists(dataHdfsDir)) {
                log.info("Downloading Parquet cold data from HDFS: {}", dataHdfsDir);
                ctx.storageManager().createLocalDirectories("data_recovery/" + tableIdentifier);

                List<Storage.StorageStatus> dataFiles =
                        ctx.storageManager()
                                .listHdfsFilesWithFilter(
                                        dataHdfsDir, name -> name.endsWith(".parquet"));
                for (Storage.StorageStatus file : dataFiles) {
                    try {
                        String fileName = extractFileName(file.path().location());
                        ctx.storageManager()
                                .downloadFromHdfs(
                                        dataHdfsDir + "/" + fileName,
                                        "data_recovery/" + tableIdentifier + "/" + fileName);
                        log.info("Downloaded Parquet cold data from HDFS: {}", fileName);
                    } catch (Exception e) {
                        log.error("Failed to download Parquet cold data file: {}", file.path(), e);
                    }
                }
            } else {
                log.info("No Parquet cold data directory found at HDFS: {}", dataHdfsDir);
            }

            ctx.storageManager().createLocalDirectories("wal_recovery/" + tableIdentifier);

            List<Storage.StorageStatus> walFiles =
                    ctx.storageManager()
                            .listHdfsFilesWithFilter(
                                    walHdfsDir,
                                    name -> name.endsWith(".parquet") || name.endsWith(".nwl"));

            for (Storage.StorageStatus file : walFiles) {
                try {
                    String fileName = extractFileName(file.path().location());
                    ctx.storageManager()
                            .downloadFromHdfs(
                                    walHdfsDir + "/" + fileName,
                                    "wal_recovery/" + tableIdentifier + "/" + fileName);
                    log.info("Downloaded WAL segment from HDFS: {}", fileName);
                } catch (Exception e) {
                    log.error("Failed to download WAL segment: {}", file.path(), e);
                }
            }

            String localWalDir = ctx.basePath() + "/wal_recovery/" + tableIdentifier;
            NwlWALReader reader = createWALReader(localWalDir, ctx.hadoopConf());
            List<WALRecord> unflushedRecords = reader.readUnflushed();
            reader.close();

            if (unflushedRecords.isEmpty()) {
                log.info("No unflushed records found in remote WAL for table {}", tableIdentifier);
                return;
            }

            Map<String, List<WALRecord>> recordsByTable =
                    unflushedRecords.stream()
                            .filter(r -> r.value().length > 0)
                            .collect(
                                    Collectors.groupingBy(
                                            record -> {
                                                String tid =
                                                        extractTableIdFromWALRecord(record.value());
                                                return tid != null ? tid : "unknown";
                                            }));

            // Check memory availability before replaying WAL records
            if (memoryAvailableChecker != null && !memoryAvailableChecker.get()) {
                if (syncFlushTrigger != null) {
                    syncFlushTrigger.run();
                }
                if (!memoryAvailableChecker.get()) {
                    log.error(
                            "No memory available for remote WAL replay for table {}. "
                                    + "Skipping replay of {} records across {} tables.",
                            tableIdentifier,
                            unflushedRecords.size(),
                            recordsByTable.size());
                    ctx.storageManager().cleanupWALFiles("wal_recovery/" + tableIdentifier);
                    return;
                }
            }

            replayWALRecords(recordsByTable);

            ctx.storageManager().cleanupWALFiles("wal_recovery/" + tableIdentifier);
            log.info("Remote WAL recovery completed for table {}", tableIdentifier);

        } catch (Exception e) {
            log.error("Remote WAL recovery failed for table {}", tableIdentifier, e);
        }
    }

    public void recoverShard(
            ShardRecoveryRecord record,
            Supplier<Boolean> memoryAvailableChecker,
            Runnable syncFlushTrigger,
            Function<String, TableStore> tableStoreLookup) {
        log.info(
                "Starting shard recovery for shard {} from executor {}",
                record.shardId(),
                record.previousExecutorId());

        String currentNodeId = buildNodeId();
        String previousNodeId =
                record.previousNodeSlotId().contains(":")
                        ? record.previousNodeSlotId()
                                .substring(0, record.previousNodeSlotId().indexOf(':'))
                        : record.previousNodeSlotId();
        boolean sameNode = currentNodeId.equals(previousNodeId);

        if (sameNode && record.previousBasePath() != null && !record.previousBasePath().isBlank()) {
            Set<String> tables = record.tableIdentifiers();
            if (tables.isEmpty()) {
                tables = ctx.storageManager().discoverLocalTables(record.previousBasePath());
            }
            for (String tableIdentifier : tables) {
                recoverFromLocalLMDB(
                        record.previousBasePath(),
                        tableIdentifier,
                        memoryAvailableChecker,
                        syncFlushTrigger);
            }
        } else {
            if (record.walHdfsDir() != null && !record.walHdfsDir().isBlank()) {
                Set<String> tables = record.tableIdentifiers();
                if (tables.isEmpty()) {
                    tables = discoverRemoteWALTables(record.walHdfsDir());
                }
                for (String tableIdentifier : tables) {
                    recoverFromRemoteWAL(
                            record.walHdfsDir(),
                            tableIdentifier,
                            memoryAvailableChecker,
                            syncFlushTrigger);
                }
            } else {
                log.warn(
                        "No recovery path available for shard {} - neither same node nor HDFS WAL available",
                        record.shardId());
                if (ctx.durabilityLevel() == DurabilityLevel.PROCESS) {
                    log.warn(
                            "Durability level is PROCESS and cross-node recovery is needed for shard {}. "
                                    + "Data may be lost because WAL was not enabled. "
                                    + "Consider using NODE durability level with HDFS WAL for production deployments.",
                            record.shardId());
                }
            }
        }

        log.info("Shard recovery completed for shard {}", record.shardId());
    }

    public Set<String> discoverLocalTables(String localBasePath) {
        return ctx.storageManager().discoverLocalTables(localBasePath);
    }

    public Set<String> discoverRemoteWALTables(String walHdfsDir) {
        Set<String> tables = new java.util.HashSet<>();
        if (!ctx.storageManager().hdfsAvailable()) {
            return tables;
        }
        try {
            if (!ctx.storageManager().hdfsExists(walHdfsDir)) {
                return tables;
            }
            List<Storage.StorageStatus> files =
                    ctx.storageManager()
                            .listHdfsFilesWithFilter(
                                    walHdfsDir,
                                    name -> name.endsWith(".parquet") || name.endsWith(".nwl"));
            if (files.isEmpty()) {
                return tables;
            }
            NwlWALReader reader = createWALReader(walHdfsDir, ctx.hadoopConf());
            List<WALRecord> unflushedRecords = reader.readUnflushed();
            reader.close();
            for (WALRecord r : unflushedRecords) {
                if (r.value().length > 0) {
                    String tid = extractTableIdFromWALRecord(r.value());
                    if (tid != null && !"unknown".equals(tid)) {
                        tables.add(tid);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to discover remote WAL tables from {}", walHdfsDir, e);
        }
        return tables;
    }

    private NwlWALReader createWALReader(String basePath, Configuration hadoopConf)
            throws IOException {
        ReaderFactoryBootstrap.ensureInitialized();
        ReaderConfig config = ReaderConfig.builder(ReaderType.WAL).filePath(basePath).build();
        ReaderContext ctx = ReaderContext.ofHadoopConfig(hadoopConf);
        return ReaderFactoryRegistry.getInstance()
                .create(ReaderType.WAL, NwlWALReader.class, config, ctx);
    }

    private void replayWALRecords(Map<String, List<WALRecord>> recordsByTable) {
        int totalRecords = 0;
        int failedRecords = 0;
        int skippedRecords = 0;

        for (Map.Entry<String, List<WALRecord>> entry : recordsByTable.entrySet()) {
            String tableIdentifier = entry.getKey();
            List<WALRecord> records = entry.getValue();

            log.info("Replaying {} WAL records for table {}", records.size(), tableIdentifier);

            TableStore tableStore = ctx.tableStoreFactory().apply(tableIdentifier);
            LmdbInstance instance = ctx.currentWritableProvider().apply(tableIdentifier);
            if (instance == null) {
                // Try to create a new instance via newInstanceCreator before giving up
                if (ctx.newInstanceCreator() != null) {
                    // Check memory availability before creating a new instance
                    if (ctx.memoryAvailableChecker() != null
                            && !ctx.memoryAvailableChecker().get()) {
                        if (ctx.syncFlushTrigger() != null) {
                            ctx.syncFlushTrigger().run();
                        }
                        if (ctx.memoryAvailableChecker() != null
                                && !ctx.memoryAvailableChecker().get()) {
                            log.warn(
                                    "Cannot create new instance for table {}: memory unavailable",
                                    tableIdentifier);
                            skippedRecords += records.size();
                            continue;
                        }
                    }
                    try {
                        instance =
                                ctx.newInstanceCreator()
                                        .apply(tableIdentifier, ctx.instanceMappedBytes());
                        if (instance != null) {
                            ctx.memoryTracker().accept(instance.getTotalUsageSize(), 0L);
                            ctx.instanceRegistrar()
                                    .accept(
                                            instance.instanceId(),
                                            CompletableFuture.completedFuture(
                                                    instance.instanceId()));
                            ctx.writableInstanceRegistrar().accept(tableIdentifier, instance);
                            log.info(
                                    "Created new instance for table {} during WAL replay",
                                    tableIdentifier);
                        }
                    } catch (Exception e) {
                        log.error(
                                "Failed to create new instance for table {} during WAL replay: {}",
                                tableIdentifier,
                                e.getMessage());
                    }
                }
                if (instance == null) {
                    log.warn(
                            "Cannot replay WAL for table {}: no writable instance available and newInstanceCreator failed, skipping {} records",
                            tableIdentifier,
                            records.size());
                    skippedRecords += records.size();
                    continue;
                }
            }

            for (WALRecord record : records) {
                totalRecords++;
                try {
                    byte[] key = extractKeyFromWALRecord(record);
                    byte[] encodedValue = extractValueFromWALRecord(record.value());
                    LmdbInstance.PutStatus status = instance.put(key, encodedValue);
                    if (status != LmdbInstance.PutStatus.SUCCESS) {
                        log.warn(
                                "WAL replay put returned {} for key in table {} at seq={}",
                                status,
                                tableIdentifier,
                                record.sequence());
                    }
                } catch (Exception e) {
                    log.error(
                            "Failed to replay WAL record for table {} at seq={}: {}",
                            tableIdentifier,
                            record.sequence(),
                            e.getMessage());
                    failedRecords++;
                }
            }
        }

        if (totalRecords > 0 && (double) failedRecords / totalRecords > 0.1) {
            throw new RuntimeException(
                    "WAL replay failure rate exceeded 10%: "
                            + failedRecords
                            + "/"
                            + totalRecords
                            + " records failed. "
                            + "Aborting recovery to prevent inconsistent state.");
        }

        if (failedRecords > 0) {
            log.warn(
                    "WAL replay completed with {} failures out of {} records",
                    failedRecords,
                    totalRecords);
        }
    }

    static byte[] extractKeyFromWALRecord(WALRecord record) {
        return WALRecordCodec.extractKey(record.value());
    }

    static byte[] extractValueFromWALRecord(byte[] data) {
        return WALRecordCodec.extractValue(data);
    }

    static String extractTableIdFromWALRecord(byte[] data) {
        return WALRecordCodec.extractTableId(data);
    }

    static String parseTableIdentifier(String instanceId) {
        String rest = instanceId.substring("inst-".length());
        int last = rest.lastIndexOf('-');
        if (last <= 0) {
            return rest;
        }
        String withoutUuid = rest.substring(0, last);
        int secondLast = withoutUuid.lastIndexOf('-');
        if (secondLast <= 0) {
            return withoutUuid;
        }
        return withoutUuid.substring(0, secondLast);
    }

    private String buildNodeId() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }

    private String extractFileName(String path) {
        int lastSlash = path.lastIndexOf('/');
        return lastSlash >= 0 ? path.substring(lastSlash + 1) : path;
    }

    /**
     * Recover the BlockCatalog from Manifest files for a given table.
     *
     * <p>Creates a ManifestBackedBlockCatalog, reads the latest Snapshot's DataFiles, registers
     * them as ColdDataBlocks, and transfers the recovered blocks into the given TableStore. This
     * enables the read path to discover cold (Parquet/ORC) data that was flushed in previous
     * process lifetimes.
     *
     * @param snapshotManager the SnapshotManager to read manifests from
     * @param tableId the table identifier to recover blocks for
     * @param tableStore the TableStore to register recovered blocks into
     * @return the number of blocks successfully recovered
     */
    public int recoverBlockCatalogFromManifests(
            SnapshotManager snapshotManager, String tableId, TableStore tableStore) {
        log.info("Recovering block catalog from manifests for table {}", tableId);
        try {
            ManifestBackedBlockCatalog recoveryCatalog = new ManifestBackedBlockCatalog(tableId);
            int recoveredCount = recoveryCatalog.recoverFromManifests(snapshotManager, tableId);

            // Transfer recovered blocks to the TableStore's catalog
            for (DataBlock block : recoveryCatalog.currentBlocks()) {
                if (block instanceof ColdDataBlock coldBlock) {
                    tableStore.registerColdBlock(coldBlock);
                }
            }

            log.info(
                    "Block catalog recovery completed for table {}: {} blocks registered",
                    tableId,
                    recoveredCount);
            return recoveredCount;
        } catch (Exception e) {
            log.error("Failed to recover block catalog from manifests for table {}", tableId, e);
            return 0;
        }
    }

    public void cleanupPendingInstances(String tableIdentifier) {
        TableStore tableStore = ctx.tableStoreFactory().apply(tableIdentifier);
        if (tableStore == null) {
            log.warn(
                    "No TableStore found for table {}, skipping PENDING instance cleanup",
                    tableIdentifier);
            return;
        }

        Set<String> committedInstanceIds = new java.util.HashSet<>();
        CommitMarker commitMarker = tableStore.commitMarker();
        if (commitMarker != null) {
            List<CommitMarker.CommitRecord> commitRecords = commitMarker.scanCommitMarkers();
            for (CommitMarker.CommitRecord record : commitRecords) {
                committedInstanceIds.addAll(record.instanceIds());
                for (String instanceId : record.instanceIds()) {
                    log.info(
                            "Recovering committed instance {} from CommitMarker (txnId={})",
                            instanceId,
                            record.txnId());
                    try {
                        tableStore.recoverCommittedInstance(instanceId);
                    } catch (Exception e) {
                        log.error(
                                "Failed to recover committed instance {}: {}",
                                instanceId,
                                e.getMessage());
                    }
                }
            }
        }

        List<DataBlock> pendingBlocks = tableStore.blockCatalog().pendingBlocks();
        for (DataBlock pending : pendingBlocks) {
            if (!committedInstanceIds.contains(pending.blockId())) {
                log.info(
                        "Cleaning up PENDING instance {} from previous crash, treating as uncommitted",
                        pending.blockId());
                try {
                    tableStore.rollbackPendingInstance(pending.blockId());
                } catch (Exception e) {
                    log.error(
                            "Failed to rollback PENDING instance {}: {}",
                            pending.blockId(),
                            e.getMessage());
                }
            }
        }

        if (!pendingBlocks.isEmpty()) {
            log.info(
                    "PENDING instance cleanup completed for table {}: {} total, {} recovered, {} rolled back",
                    tableIdentifier,
                    pendingBlocks.size(),
                    committedInstanceIds.size(),
                    pendingBlocks.size() - committedInstanceIds.size());
        }
    }

    public void recoverVectorIndex(boolean vectorRecoveryOnStartup) {
        if (!vectorRecoveryOnStartup) {
            log.info("Vector index recovery on startup is disabled");
            return;
        }
        log.info("Starting vector index recovery...");
        log.info(
                "Vector index recovery completed (placeholder — full implementation requires VectorIndexManager integration)");
    }

    /**
     * Builder for {@link RecoveryContext}.
     *
     * <p>Provides a fluent API for constructing RecoveryContext instances with validation of
     * required fields. Required fields are: storageManager, basePath, instanceMappedBytes,
     * tableStoreFactory, currentWritableProvider. All other fields have sensible defaults (null or
     * no-op).
     */
    public static class RecoveryContextBuilder {

        private StorageManager storageManager;
        private String basePath;
        private long instanceMappedBytes;
        private Configuration hadoopConf;
        private WALConfig walConfig;
        private DurabilityLevel durabilityLevel = DurabilityLevel.PROCESS;
        private boolean failOnError = true;
        private Supplier<Integer> writableNumSupplier = () -> 3;
        private Function<String, TableStore> tableStoreFactory;
        private BiConsumer<String, LmdbInstance> writableInstanceRegistrar = (id, inst) -> {};
        private BiConsumer<String, LmdbInstance> readOnlyInstanceRegistrar = (id, inst) -> {};
        private BiConsumer<Long, Long> memoryTracker = (used, reserved) -> {};
        private Function<String, LmdbInstance> currentWritableProvider;
        private BiConsumer<String, CompletableFuture<String>> instanceRegistrar =
                (id, future) -> {};
        private BiFunction<String, Long, LmdbInstance> newInstanceCreator;
        private Supplier<Boolean> memoryAvailableChecker;
        private Runnable syncFlushTrigger;

        private RecoveryContextBuilder() {}

        public RecoveryContextBuilder storageManager(StorageManager storageManager) {
            this.storageManager = storageManager;
            return this;
        }

        public RecoveryContextBuilder basePath(String basePath) {
            this.basePath = basePath;
            return this;
        }

        public RecoveryContextBuilder instanceMappedBytes(long instanceMappedBytes) {
            this.instanceMappedBytes = instanceMappedBytes;
            return this;
        }

        public RecoveryContextBuilder hadoopConf(Configuration hadoopConf) {
            this.hadoopConf = hadoopConf;
            return this;
        }

        public RecoveryContextBuilder walConfig(WALConfig walConfig) {
            this.walConfig = walConfig;
            return this;
        }

        public RecoveryContextBuilder durabilityLevel(DurabilityLevel durabilityLevel) {
            this.durabilityLevel = durabilityLevel;
            return this;
        }

        public RecoveryContextBuilder failOnError(boolean failOnError) {
            this.failOnError = failOnError;
            return this;
        }

        public RecoveryContextBuilder writableNumSupplier(Supplier<Integer> writableNumSupplier) {
            this.writableNumSupplier = writableNumSupplier;
            return this;
        }

        public RecoveryContextBuilder tableStoreFactory(
                Function<String, TableStore> tableStoreFactory) {
            this.tableStoreFactory = tableStoreFactory;
            return this;
        }

        public RecoveryContextBuilder writableInstanceRegistrar(
                BiConsumer<String, LmdbInstance> writableInstanceRegistrar) {
            this.writableInstanceRegistrar = writableInstanceRegistrar;
            return this;
        }

        public RecoveryContextBuilder readOnlyInstanceRegistrar(
                BiConsumer<String, LmdbInstance> readOnlyInstanceRegistrar) {
            this.readOnlyInstanceRegistrar = readOnlyInstanceRegistrar;
            return this;
        }

        public RecoveryContextBuilder memoryTracker(BiConsumer<Long, Long> memoryTracker) {
            this.memoryTracker = memoryTracker;
            return this;
        }

        public RecoveryContextBuilder currentWritableProvider(
                Function<String, LmdbInstance> currentWritableProvider) {
            this.currentWritableProvider = currentWritableProvider;
            return this;
        }

        public RecoveryContextBuilder instanceRegistrar(
                BiConsumer<String, CompletableFuture<String>> instanceRegistrar) {
            this.instanceRegistrar = instanceRegistrar;
            return this;
        }

        public RecoveryContextBuilder newInstanceCreator(
                BiFunction<String, Long, LmdbInstance> newInstanceCreator) {
            this.newInstanceCreator = newInstanceCreator;
            return this;
        }

        public RecoveryContextBuilder memoryAvailableChecker(
                Supplier<Boolean> memoryAvailableChecker) {
            this.memoryAvailableChecker = memoryAvailableChecker;
            return this;
        }

        public RecoveryContextBuilder syncFlushTrigger(Runnable syncFlushTrigger) {
            this.syncFlushTrigger = syncFlushTrigger;
            return this;
        }

        /**
         * Builds and validates the RecoveryContext.
         *
         * @return a new RecoveryContext instance
         * @throws IllegalStateException if required fields are missing
         */
        public RecoveryContext build() {
            validate();
            return new RecoveryContext(
                    storageManager,
                    basePath,
                    instanceMappedBytes,
                    hadoopConf,
                    walConfig,
                    durabilityLevel,
                    failOnError,
                    writableNumSupplier,
                    tableStoreFactory,
                    writableInstanceRegistrar,
                    readOnlyInstanceRegistrar,
                    memoryTracker,
                    currentWritableProvider,
                    instanceRegistrar,
                    newInstanceCreator,
                    memoryAvailableChecker,
                    syncFlushTrigger);
        }

        private void validate() {
            if (storageManager == null) {
                throw new IllegalStateException("storageManager is required");
            }
            if (basePath == null || basePath.isBlank()) {
                throw new IllegalStateException("basePath is required and must not be blank");
            }
            if (instanceMappedBytes <= 0) {
                throw new IllegalStateException("instanceMappedBytes must be positive");
            }
            if (tableStoreFactory == null) {
                throw new IllegalStateException("tableStoreFactory is required");
            }
            if (currentWritableProvider == null) {
                throw new IllegalStateException("currentWritableProvider is required");
            }
        }
    }
}
