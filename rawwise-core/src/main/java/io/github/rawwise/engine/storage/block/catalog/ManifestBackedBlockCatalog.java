package io.github.rawwise.engine.storage.block.catalog;

import io.github.rawwise.common.collection.iterator.CloseableIterator;
import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.LogFile;
import io.github.rawwise.engine.storage.RawWiseFile;
import io.github.rawwise.engine.storage.block.ColdDataBlock;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.block.index.RowGroupIndex;
import io.github.rawwise.engine.storage.manifest.ManifestFile;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.engine.txn.record.TxnCommitRecord;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.model.record.Snapshot;
import io.github.rawwise.model.record.TableId;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

/**
 * BlockCatalog that can recover cold blocks from Manifest files on startup.
 *
 * <p>Extends InMemoryBlockCatalog to inherit all in-memory operations, and adds a {@link
 * #recoverFromManifests} method that reads the latest Snapshot via SnapshotManager, lists all
 * DataFiles, and registers them as ColdDataBlocks.
 *
 * <p>This enables the BlockCatalog to be reconstructed after a process restart, closing the
 * metadata gap where cold (Parquet/ORC) data was previously invisible to the read path until a
 * flush happened in the current process lifetime.
 */
@Slf4j
public class ManifestBackedBlockCatalog extends InMemoryBlockCatalog {

    private volatile List<LogFile> recoveredLogFiles = List.of();

    /**
     * Optional Metastore reference for reading {@link TxnCommitRecord}s. Nullable for backward
     * compatibility — if not set, TxnCommitRecord-related methods return empty/false.
     */
    private volatile Metastore metastore;

    /**
     * Optional catalog name for building TxnCommitRecord Metastore keys. Must be set together with
     * {@link #metastore} for TxnCommitRecord methods to function.
     */
    private volatile String catalogName;

    public ManifestBackedBlockCatalog(String tableId) {
        super(tableId);
    }

    /**
     * Sets the Metastore and catalog name for TxnCommitRecord lookups.
     *
     * <p>Both must be set together for {@link #getTxnCommitRecords(long)} and {@link
     * #isTxnCommitted(long)} to function. If either is null, those methods return empty/false.
     *
     * @param metastore the Metastore instance
     * @param catalogName the catalog name (e.g., "metastore")
     */
    public void setMetastoreAndCatalog(Metastore metastore, String catalogName) {
        this.metastore = metastore;
        this.catalogName = catalogName;
    }

    /**
     * Recover ColdDataBlocks from the latest Snapshot's Manifest files.
     *
     * <p>For each DataFile in the latest Snapshot:
     *
     * <ul>
     *   <li>PARQUET format: creates a ColdDataBlock with Format.PARQUET
     *   <li>ORC format: creates a ColdDataBlock with Format.ORC (placeholder)
     *   <li>Other formats: skipped with a warning log
     * </ul>
     *
     * <p>Also recovers LogFile entries (UpdateLog/DeleteVector) from manifests, which can be
     * queried via {@link #getRecoveredLogFiles()}.
     *
     * @param snapshotManager the SnapshotManager to read manifests from
     * @param tableId the table identifier to recover blocks for
     * @return the number of blocks successfully recovered
     */
    public int recoverFromManifests(SnapshotManager snapshotManager, String tableId)
            throws IOException {
        Optional<Snapshot> latestSnapshot = snapshotManager.latestSnapshot(tableId);
        if (latestSnapshot.isEmpty()) {
            log.info("No snapshots found for table {}, skipping block catalog recovery", tableId);
            return 0;
        }

        Snapshot snapshot = latestSnapshot.get();

        List<DataFile> dataFiles;
        List<LogFile> logFiles;
        Map<String, byte[]> fileBloomMap;
        try {
            dataFiles = snapshotManager.listDataFiles(tableId, snapshot.snapshotId());
            logFiles = snapshotManager.listLogFiles(tableId, snapshot.snapshotId());
            List<ManifestFile> manifestFiles =
                    snapshotManager.listManifestFiles(tableId, snapshot.snapshotId());
            fileBloomMap = new HashMap<>();
            for (ManifestFile mf : manifestFiles) {
                if (mf.bloomFilter() != null) {
                    for (DataFile df : mf.dataFiles()) {
                        fileBloomMap.put(df.filePath(), mf.bloomFilter());
                    }
                }
            }
        } catch (IOException e) {
            log.warn(
                    "Failed to read manifests for table {} snapshot {}, skipping block catalog recovery",
                    tableId,
                    snapshot.snapshotId(),
                    e);
            return 0;
        }

        // Recover LogFiles
        if (!logFiles.isEmpty()) {
            this.recoveredLogFiles = Collections.unmodifiableList(new ArrayList<>(logFiles));
            log.info(
                    "Recovered {} log files from manifests for table {} (snapshot {})",
                    logFiles.size(),
                    tableId,
                    snapshot.snapshotId());
        }

        if (dataFiles.isEmpty()) {
            log.info(
                    "No data files found in snapshot {} for table {}",
                    snapshot.snapshotId(),
                    tableId);
            return 0;
        }

        int recoveredCount = 0;
        for (DataFile dataFile : dataFiles) {
            try {
                DataBlock.Format blockFormat = toBlockFormat(dataFile.format());
                if (blockFormat == DataBlock.Format.UNKNOWN) {
                    log.warn(
                            "Skipping data file with unsupported format {}: {}",
                            dataFile.format(),
                            dataFile.filePath());
                    continue;
                }

                byte[] rowGroupIndex = null;
                if (blockFormat == DataBlock.Format.PARQUET) {
                    rowGroupIndex = buildRowGroupIndex(dataFile.filePath());
                }

                ColdDataBlock block =
                        new ColdDataBlock(
                                dataFile.filePath(),
                                dataFile.tableId(),
                                DataBlock.Kind.IMMUTABLE_COLD,
                                blockFormat,
                                blockFormat,
                                toLocation(dataFile),
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
                                fileBloomMap.get(dataFile.filePath()),
                                rowGroupIndex);
                upsert(block);
                recoveredCount++;
                log.debug(
                        "Recovered cold block from manifest: {} format={}",
                        dataFile.filePath(),
                        dataFile.format());
            } catch (Exception e) {
                log.warn("Failed to recover block from data file: {}", dataFile.filePath(), e);
            }
        }

        log.info(
                "Recovered {} blocks from manifests for table {} (snapshot {})",
                recoveredCount,
                tableId,
                snapshot.snapshotId());
        return recoveredCount;
    }

    private byte[] buildRowGroupIndex(String filePath) {
        try {
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(filePath);
            try (ParquetFileReader reader =
                    ParquetFileReader.open(
                            org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                                    path, new Configuration()))) {
                List<RowGroupIndex.RowGroupEntry> entries = new ArrayList<>();
                for (BlockMetaData blockMeta : reader.getFooter().getBlocks()) {
                    RowGroupIndex.RowGroupEntry entry = extractRowGroupEntry(blockMeta);
                    if (entry != null) {
                        entries.add(entry);
                    }
                }
                return new RowGroupIndex(entries).serialize();
            }
        } catch (IOException e) {
            log.warn(
                    "Failed to build RowGroupIndex from footer for recovered block {}: {}",
                    filePath,
                    e.getMessage());
            return null;
        }
    }

    private static RowGroupIndex.RowGroupEntry extractRowGroupEntry(BlockMetaData blockMeta) {
        for (ColumnChunkMetaData colMeta : blockMeta.getColumns()) {
            if (!"key".equals(colMeta.getPath().toDotString())) {
                continue;
            }
            byte[] minBytes = colMeta.getStatistics().getMinBytes();
            byte[] maxBytes = colMeta.getStatistics().getMaxBytes();
            return new RowGroupIndex.RowGroupEntry(
                    minBytes != null ? minBytes : new byte[0],
                    maxBytes != null ? maxBytes : new byte[0],
                    blockMeta.getRowCount(),
                    blockMeta.getStartingPos());
        }
        return null;
    }

    /**
     * Returns the list of LogFiles recovered from the latest snapshot's manifests. This includes
     * UpdateLog and DeleteVector entries.
     *
     * @return unmodifiable list of recovered LogFiles, empty if none recovered
     */
    public List<LogFile> getRecoveredLogFiles() {
        return recoveredLogFiles;
    }

    /**
     * Reads all {@link TxnCommitRecord}s associated with the given snapshot ID from the Metastore.
     *
     * <p>This method scans the TxnCommitRecord prefix for this table and filters by {@code
     * snapshotId}. Each record's JSON payload is deserialized into a {@link TxnCommitRecord}
     * object.
     *
     * <p>If the Metastore or catalog name is not configured (via {@link #setMetastoreAndCatalog}),
     * this method returns an empty list.
     *
     * @param snapshotId the snapshot ID to filter by
     * @return list of TxnCommitRecords matching the snapshot ID; empty if none found or Metastore
     *     not configured
     */
    public List<TxnCommitRecord> getTxnCommitRecords(long snapshotId) {
        if (metastore == null || catalogName == null) {
            log.debug(
                    "getTxnCommitRecords: Metastore or catalogName not configured, "
                            + "returning empty list for snapshotId={}",
                    snapshotId);
            return List.of();
        }

        TableId id = TableId.parseLenient(tableId());
        byte[] prefix =
                TxnCommitRecord.buildMetastorePrefix(catalogName, id.database(), id.tableName());

        List<TxnCommitRecord> results = new ArrayList<>();
        try (CloseableIterator<KeyValue> it = metastore.scan(prefix)) {
            while (it.hasNext()) {
                KeyValue kv = it.next();
                try {
                    TxnCommitRecord record = TxnCommitRecord.fromJson(kv.value());
                    if (record.snapshotId() == snapshotId) {
                        results.add(record);
                    }
                } catch (IOException e) {
                    log.warn(
                            "getTxnCommitRecords: Failed to deserialize TxnCommitRecord "
                                    + "for snapshotId={}, skipping",
                            snapshotId,
                            e);
                }
            }
        } catch (IOException e) {
            log.warn(
                    "getTxnCommitRecords: Failed to scan TxnCommitRecords for table={} snapshotId={}",
                    tableId(),
                    snapshotId,
                    e);
            return List.of();
        }

        log.debug(
                "getTxnCommitRecords: found {} record(s) for snapshotId={} table={}",
                results.size(),
                snapshotId,
                tableId());
        return Collections.unmodifiableList(results);
    }

    /**
     * Checks whether a transaction with the given {@code txnId} has been committed by looking up
     * its {@link TxnCommitRecord} in the Metastore.
     *
     * <p>This is the visibility check used by read paths to determine if a transaction's writes are
     * visible (CCI-010: No partial commit).
     *
     * <p>If the Metastore or catalog name is not configured (via {@link #setMetastoreAndCatalog}),
     * this method returns {@code false}.
     *
     * @param txnId the transaction ID to check
     * @return {@code true} if a TxnCommitRecord exists for the txnId; {@code false} otherwise or if
     *     Metastore is not configured
     */
    public boolean isTxnCommitted(long txnId) {
        if (metastore == null || catalogName == null) {
            log.debug(
                    "isTxnCommitted: Metastore or catalogName not configured, "
                            + "returning false for txnId={}",
                    txnId);
            return false;
        }

        TableId id = TableId.parseLenient(tableId());
        byte[] key =
                TxnCommitRecord.buildMetastoreKey(
                        catalogName, id.database(), id.tableName(), txnId);

        try {
            Optional<byte[]> value = metastore.get(key);
            boolean committed = value.isPresent();
            log.debug(
                    "isTxnCommitted: txnId={} committed={} table={}", txnId, committed, tableId());
            return committed;
        } catch (IOException e) {
            log.warn(
                    "isTxnCommitted: Failed to check TxnCommitRecord for txnId={} table={}",
                    txnId,
                    tableId(),
                    e);
            return false;
        }
    }

    /** Convert RawWiseFile.FileFormat to DataBlock.Format. */
    private static DataBlock.Format toBlockFormat(RawWiseFile.FileFormat fileFormat) {
        return switch (fileFormat) {
            case PARQUET -> DataBlock.Format.PARQUET;
            case ORC -> DataBlock.Format.ORC;
            default -> DataBlock.Format.UNKNOWN;
        };
    }

    /**
     * Construct a DataBlock.Location from a DataFile's filePath.
     *
     * <p>Parses the filePath (which may be a URI like "file:/path" or "hdfs:/path") into scheme and
     * location components for the StoragePath.
     */
    private static DataBlock.Location toLocation(DataFile dataFile) {
        String filePath = dataFile.filePath();
        String scheme = "file";
        String location = filePath;

        if (filePath.startsWith("hdfs:")) {
            scheme = "hdfs";
            location = filePath.substring("hdfs:".length());
        } else if (filePath.startsWith("file:")) {
            scheme = "file";
            location = filePath.substring("file:".length());
        }

        return new DataBlock.Location(
                "recovered",
                new Storage.StoragePath(scheme, location),
                0L,
                dataFile.fileSizeBytes());
    }
}
