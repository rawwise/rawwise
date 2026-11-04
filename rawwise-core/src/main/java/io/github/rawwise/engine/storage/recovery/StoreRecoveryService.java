package io.github.rawwise.engine.storage.recovery;

import io.github.rawwise.backend.routing.ShardRecoveryRecord;
import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.MemoryBudget;
import io.github.rawwise.engine.storage.ParquetDataFile;
import io.github.rawwise.engine.storage.RecoveryManager;
import io.github.rawwise.engine.storage.block.ColdDataBlock;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.model.record.Snapshot;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

@Slf4j
public class StoreRecoveryService {

    private final RecoveryManager recoveryManager;
    private final Supplier<SnapshotManager> snapshotManagerProvider;
    private final Function<String, TableStore> tableStoreProvider;
    private final MemoryBudget memoryBudget;
    private final Supplier<Boolean> flushTrigger;

    public StoreRecoveryService(
            RecoveryManager recoveryManager,
            Supplier<SnapshotManager> snapshotManagerProvider,
            Function<String, TableStore> tableStoreProvider,
            MemoryBudget memoryBudget,
            Supplier<Boolean> flushTrigger) {
        this.recoveryManager = recoveryManager;
        this.snapshotManagerProvider = snapshotManagerProvider;
        this.tableStoreProvider = tableStoreProvider;
        this.memoryBudget = memoryBudget;
        this.flushTrigger = flushTrigger;
    }

    public void recoverFromLocalLMDB(String localBasePath, String tableIdentifier) {
        recoveryManager.recoverFromLocalLMDB(
                localBasePath,
                tableIdentifier,
                memoryBudget::canAllocateOneInstance,
                flushTrigger::get);
    }

    public void recoverFromRemoteWAL(String walHdfsDir, String tableIdentifier) {
        recoveryManager.recoverFromRemoteWAL(
                walHdfsDir,
                tableIdentifier,
                memoryBudget::canAllocateOneInstance,
                flushTrigger::get);
    }

    public void recoverShard(ShardRecoveryRecord record) {
        recoveryManager.recoverShard(
                record,
                memoryBudget::canAllocateOneInstance,
                flushTrigger::get,
                tableStoreProvider);
    }

    public int recoverColdBlocksForTable(String tableIdentifier) {
        SnapshotManager sm = snapshotManagerProvider.get();
        if (sm == null) {
            log.warn(
                    "SnapshotManager not configured, skipping cold block recovery for table {}",
                    tableIdentifier);
            return 0;
        }
        try {
            Optional<Snapshot> snapshotOpt = sm.latestSnapshot(tableIdentifier);
            if (snapshotOpt.isEmpty()) {
                log.info(
                        "No snapshot found for table {}, skipping cold block recovery",
                        tableIdentifier);
                return 0;
            }
            Snapshot snapshot = snapshotOpt.get();
            List<DataFile> dataFiles = sm.listDataFiles(tableIdentifier, snapshot.snapshotId());
            if (dataFiles.isEmpty()) {
                return 0;
            }
            TableStore ts = tableStoreProvider.apply(tableIdentifier);
            int recovered = 0;
            for (DataFile dataFile : dataFiles) {
                if (dataFile instanceof ParquetDataFile pdf) {
                    ColdDataBlock block = buildColdBlockFromDataFile(pdf, tableIdentifier);
                    ts.registerColdBlock(block);
                    recovered++;
                }
            }
            log.info(
                    "Recovered {} cold blocks from snapshot {} for table {}",
                    recovered,
                    snapshot.snapshotId(),
                    tableIdentifier);
            return recovered;
        } catch (IOException e) {
            log.error(
                    "Failed to recover cold blocks for table {}: {}",
                    tableIdentifier,
                    e.getMessage(),
                    e);
            return 0;
        }
    }

    private ColdDataBlock buildColdBlockFromDataFile(
            ParquetDataFile dataFile, String tableIdentifier) {
        String filePath = dataFile.filePath();
        Storage.StoragePath storagePath = toStoragePath(filePath);

        byte[] rowGroupIndex = null;
        try {
            org.apache.hadoop.fs.Path parquetPath = new org.apache.hadoop.fs.Path(filePath);
            org.apache.parquet.hadoop.metadata.ParquetMetadata meta =
                    org.apache.parquet.hadoop.ParquetFileReader.readFooter(
                            new Configuration(), parquetPath);
            String encoded =
                    meta.getFileMetaData().getKeyValueMetaData().get("rawwise.sparseIndex");
            if (encoded != null) {
                rowGroupIndex = Base64.getDecoder().decode(encoded);
            }
        } catch (Exception e) {
            log.warn("Failed to recover sparse index from {}, will use full scan", filePath, e);
        }

        return new ColdDataBlock(
                dataFile.filePath(),
                dataFile.tableId(),
                DataBlock.Kind.IMMUTABLE_COLD,
                DataBlock.Format.PARQUET,
                DataBlock.Format.PARQUET,
                new DataBlock.Location("remote", storagePath, 0L, dataFile.fileSizeBytes()),
                dataFile.keyRange(),
                dataFile.rowCount(),
                dataFile.fileSizeBytes(),
                dataFile.schemaVersion(),
                dataFile.snapshotId(),
                dataFile.partitionValues(),
                new ColdDataBlock.Statistics(
                        dataFile.rowCount(),
                        dataFile.fileSizeBytes(),
                        dataFile.minSequenceNumber(),
                        dataFile.maxSequenceNumber()),
                DataBlock.Visibility.ARCHIVED,
                dataFile.minSequenceNumber(),
                dataFile.maxSequenceNumber(),
                dataFile.compactionLevel(),
                null,
                rowGroupIndex);
    }

    private static Storage.StoragePath toStoragePath(String filePath) {
        if (filePath == null) {
            return new Storage.StoragePath("file", "");
        }
        if (filePath.startsWith("hdfs:")) {
            return new Storage.StoragePath("hdfs", filePath.substring("hdfs:".length()));
        }
        if (filePath.startsWith("file:")) {
            return new Storage.StoragePath("file", filePath.substring("file:".length()));
        }
        return new Storage.StoragePath("file", filePath);
    }
}
