package io.github.rawwise.engine.storage.manifest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.rawwise.common.catalog.Catalog;
import io.github.rawwise.common.catalog.CommitTransaction;
import io.github.rawwise.common.catalog.Keys;
import io.github.rawwise.common.collection.iterator.CloseableIterator;
import io.github.rawwise.common.exception.CatalogException;
import io.github.rawwise.common.exception.CatalogIOException;
import io.github.rawwise.common.exception.CommitConflictException;
import io.github.rawwise.engine.storage.ColumnStatistics;
import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.LogFile;
import io.github.rawwise.engine.storage.RawWiseFile;
import io.github.rawwise.engine.txn.record.CommitType;
import io.github.rawwise.engine.txn.record.TxnCommitRecord;
import io.github.rawwise.frontend.ir.SnapshotStatistics;
import io.github.rawwise.meta.PbManifestContent;
import io.github.rawwise.meta.PbSnapshotUpdate;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import io.github.rawwise.model.op.OpType;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.Snapshot;
import io.github.rawwise.model.record.TableId;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SnapshotManager implements SnapshotOperations {

    /** Maximum age for a cached SnapshotStatistics before it is considered stale (60 seconds). */
    private static final long STATS_MAX_AGE_MS = 60_000;

    private final Catalog catalog;
    private final Metastore store;
    private final ObjectMapper manifestMapper;

    public SnapshotManager(Catalog catalog, Metastore store) {
        this.catalog = catalog;
        this.store = store;
        this.manifestMapper = ManifestJsonCodecs.sharedObjectMapper();
    }

    @Override
    public Snapshot commitSnapshot(
            String tableId,
            List<DataFile> newDataFiles,
            List<LogFile> newLogFiles,
            long minSeq,
            long maxSeq)
            throws IOException {
        return commitSnapshotViaCatalog(tableId, newDataFiles, newLogFiles, minSeq, maxSeq, null);
    }

    public Snapshot commitSnapshot(
            String tableId,
            List<DataFile> newDataFiles,
            List<LogFile> newLogFiles,
            long minSeq,
            long maxSeq,
            byte[] bloomFilter)
            throws IOException {
        return commitSnapshotViaCatalog(
                tableId, newDataFiles, newLogFiles, minSeq, maxSeq, bloomFilter);
    }

    public Snapshot commitCompactionSnapshot(
            String tableId,
            List<DataFile> newDataFiles,
            List<DataFile> replacedDataFiles,
            long minSeq,
            long maxSeq)
            throws IOException {
        return commitCompactionSnapshotViaCatalog(
                tableId, newDataFiles, replacedDataFiles, minSeq, maxSeq);
    }

    public Snapshot commitCleanSnapshot(String tableId, List<String> filesToRemove)
            throws IOException {
        return commitCleanSnapshotViaCatalog(tableId, filesToRemove);
    }

    private Snapshot commitSnapshotViaCatalog(
            String tableId,
            List<DataFile> newDataFiles,
            List<LogFile> newLogFiles,
            long minSeq,
            long maxSeq,
            byte[] bloomFilter)
            throws IOException {
        TableId id = parseTableId(tableId);

        CommitTransaction txn;
        try {
            txn = catalog.newCommit(id);
        } catch (CatalogException e) {
            throw new IOException("Failed to start commit", e);
        }
        long baseSnapshotId = txn.baseSnapshotId();

        String manifestId = UUID.randomUUID().toString();
        ManifestFile manifestFile =
                new ManifestFile(
                        manifestId,
                        RawWiseFile.FileFormat.JSON,
                        0,
                        System.currentTimeMillis(),
                        -1L,
                        newDataFiles != null ? newDataFiles : List.of(),
                        newLogFiles != null ? newLogFiles : List.of(),
                        minSeq,
                        maxSeq,
                        bloomFilter);
        byte[] manifestBytes = manifestMapper.writeValueAsBytes(manifestFile);

        List<CommitTransaction.TableOperation> tableOps =
                buildDataCommitOperations(id, baseSnapshotId, manifestId, manifestBytes);

        CommitTransaction populated =
                new CommitTransaction(txn.commitId(), id, baseSnapshotId, tableOps);
        try {
            catalog.commitWithRetry(
                    populated,
                    3,
                    2.0,
                    freshBase ->
                            buildDataCommitOperations(id, freshBase, manifestId, manifestBytes));
        } catch (CommitConflictException e) {
            try {
                catalog.abort(txn);
            } catch (Exception ae) {
                log.warn("abort failed", ae);
            }
            throw new SnapshotCommitConflictException("Concurrent commit", baseSnapshotId, -1L);
        } catch (CatalogException e) {
            try {
                catalog.abort(txn);
            } catch (Exception ae) {
                log.warn("abort failed", ae);
            }
            throw new IOException("Snapshot commit failed", e);
        }

        // Return latest snapshot after successful commit
        List<Snapshot> latestSnapshots;
        try {
            latestSnapshots = catalog.listSnapshots(id);
        } catch (CatalogException e) {
            throw new IOException("Failed to list snapshots after commit", e);
        }
        if (latestSnapshots.isEmpty()) {
            throw new IOException("No snapshot found after successful commit");
        }
        Snapshot result = latestSnapshots.get(0);
        invalidateStatistics(tableId, result.snapshotId());

        // Write TxnCommitRecord after snapshot metadata CAS succeeds.
        // This is the cross-domain atomic commit contract — once this record exists,
        // the transaction is irrevocably committed (CCI-010).
        // Uses CAS (NotExists) for idempotency: if the txnId already has a record
        // (e.g., PublishStage wrote it first, or crash-recovery replay), the CAS
        // fails and we treat it as idempotent success.
        writeTxnCommitRecordIfAbsent(tableId, id, txn.commitId(), baseSnapshotId, result);

        return result;
    }

    /**
     * Builds the list of TableOperations for a data commit, including manifest payload, snapshot
     * update, and merged manifestIds from the latest snapshot. This method is called during initial
     * commit AND during retry to ensure the manifest list incorporates any concurrent commits.
     */
    private List<CommitTransaction.TableOperation> buildDataCommitOperations(
            TableId id, long baseSnapshotId, String manifestId, byte[] manifestBytes)
            throws IOException {

        Snapshot latestSnapshot = null;
        try {
            if (baseSnapshotId >= 0) {
                latestSnapshot = catalog.getSnapshot(id, baseSnapshotId).orElse(null);
            }
        } catch (CatalogException e) {
            throw new IOException("Failed to get snapshot", e);
        }

        List<String> manifestIds = new ArrayList<>();
        if (latestSnapshot != null
                && latestSnapshot.manifestList() != null
                && !latestSnapshot.manifestList().isEmpty()) {
            manifestIds.addAll(latestSnapshot.manifestList());
        }
        manifestIds.add(manifestId);

        List<CommitTransaction.TableOperation> tableOps = new ArrayList<>();

        byte[] manifestPayload =
                PbManifestContent.newBuilder()
                        .setManifestId(manifestId)
                        .setContent(ByteString.copyFrom(manifestBytes))
                        .setContentSize(manifestBytes.length)
                        .setCreatedAtMillis(System.currentTimeMillis())
                        .build()
                        .toByteArray();
        tableOps.add(
                new CommitTransaction.TableOperation(
                        OpType.MANIFEST_WRITE.wireName(),
                        "manifest:" + manifestId,
                        manifestPayload));

        byte[] snapshotPayload =
                PbSnapshotUpdate.newBuilder()
                        .setBaseSnapshotId(baseSnapshotId)
                        .addAllManifestIds(manifestIds)
                        .setCreatedAtMillis(System.currentTimeMillis())
                        .setSummary("commit: " + manifestIds.size() + " manifests")
                        .build()
                        .toByteArray();
        tableOps.add(
                new CommitTransaction.TableOperation(
                        OpType.SNAPSHOT_UPDATE.wireName(), "snapshot-update", snapshotPayload));

        return tableOps;
    }

    private Snapshot commitCompactionSnapshotViaCatalog(
            String tableId,
            List<DataFile> newDataFiles,
            List<DataFile> replacedDataFiles,
            long minSeq,
            long maxSeq)
            throws IOException {
        TableId id = parseTableId(tableId);
        CommitTransaction txn;
        try {
            txn = catalog.newCommit(id);
        } catch (CatalogException e) {
            throw new IOException("Failed to start commit", e);
        }
        long baseSnapshotId = txn.baseSnapshotId();

        Set<String> replacedPaths = new HashSet<>();
        if (replacedDataFiles != null)
            for (DataFile df : replacedDataFiles) replacedPaths.add(df.filePath());

        String newManifestId = UUID.randomUUID().toString();
        ManifestFile newManifest =
                new ManifestFile(
                        newManifestId,
                        RawWiseFile.FileFormat.JSON,
                        0,
                        System.currentTimeMillis(),
                        -1L,
                        newDataFiles != null ? newDataFiles : List.of(),
                        List.of(),
                        minSeq,
                        maxSeq,
                        null);
        byte[] newManifestBytes = manifestMapper.writeValueAsBytes(newManifest);

        List<CommitTransaction.TableOperation> tableOps =
                buildCompactionOperations(
                        tableId,
                        id,
                        baseSnapshotId,
                        newManifestId,
                        newManifestBytes,
                        replacedPaths);

        CommitTransaction populated =
                new CommitTransaction(txn.commitId(), id, baseSnapshotId, tableOps);
        try {
            catalog.commitWithRetry(
                    populated,
                    3,
                    2.0,
                    freshBase ->
                            buildCompactionOperations(
                                    tableId,
                                    id,
                                    freshBase,
                                    newManifestId,
                                    newManifestBytes,
                                    replacedPaths));
        } catch (CommitConflictException e) {
            try {
                catalog.abort(txn);
            } catch (Exception ae) {
                log.warn("abort failed", ae);
            }
            throw new SnapshotCommitConflictException("Concurrent compaction", baseSnapshotId, -1L);
        } catch (CatalogException e) {
            try {
                catalog.abort(txn);
            } catch (Exception ae) {
                log.warn("abort failed", ae);
            }
            throw new IOException("Compaction commit failed", e);
        }

        long newSnapshotId = resolveLatestSnapshotId(id);
        invalidateStatistics(tableId, newSnapshotId);
        List<Snapshot> latestSnapshots;
        try {
            latestSnapshots = catalog.listSnapshots(id);
        } catch (CatalogException e) {
            throw new IOException("Failed to list snapshots after commit", e);
        }
        int manifestCount =
                latestSnapshots.isEmpty() || latestSnapshots.get(0).manifestList() == null
                        ? 0
                        : latestSnapshots.get(0).manifestList().size();
        return Snapshot.of(
                newSnapshotId,
                List.of("metastore:" + newManifestId),
                System.currentTimeMillis(),
                "compaction: " + manifestCount + " manifests",
                "compaction",
                resolveSchema(id));
    }

    /**
     * Builds the list of TableOperations for a compaction commit, including rewriting existing
     * manifests to remove replaced files and adding the new compacted manifest. This method is
     * called during initial commit AND during retry to ensure the manifest list incorporates any
     * concurrent commits.
     */
    private List<CommitTransaction.TableOperation> buildCompactionOperations(
            String tableId,
            TableId id,
            long baseSnapshotId,
            String newManifestId,
            byte[] newManifestBytes,
            Set<String> replacedPaths)
            throws IOException {
        Snapshot latestSnapshot = null;
        try {
            if (baseSnapshotId >= 0) {
                latestSnapshot = catalog.getSnapshot(id, baseSnapshotId).orElse(null);
            }
        } catch (CatalogException e) {
            throw new IOException("Failed to get snapshot", e);
        }

        List<CommitTransaction.TableOperation> tableOps = new ArrayList<>();
        Map<String, String> oldToRewritten = new HashMap<>();

        if (latestSnapshot != null
                && !replacedPaths.isEmpty()
                && latestSnapshot.manifestList() != null
                && !latestSnapshot.manifestList().isEmpty()) {
            for (String oldMid : latestSnapshot.manifestList()) {
                Optional<PbManifestContent> co = readManifestContent(tableId, oldMid);
                if (co.isEmpty()) continue;
                ManifestFile om =
                        manifestMapper.readValue(
                                co.get().getContent().toByteArray(), ManifestFile.class);
                List<DataFile> filtered = new ArrayList<>();
                for (DataFile df : om.dataFiles())
                    if (!replacedPaths.contains(df.filePath())) filtered.add(df);
                if (filtered.size() != om.dataFiles().size()) {
                    String rid = oldMid + "_v" + System.currentTimeMillis();
                    ManifestFile rw =
                            new ManifestFile(
                                    rid,
                                    RawWiseFile.FileFormat.JSON,
                                    0,
                                    System.currentTimeMillis(),
                                    -1L,
                                    filtered,
                                    om.logFiles(),
                                    om.minSequenceNumber(),
                                    om.maxSequenceNumber(),
                                    om.bloomFilter());
                    byte[] rb = manifestMapper.writeValueAsBytes(rw);
                    byte[] rp =
                            PbManifestContent.newBuilder()
                                    .setManifestId(rid)
                                    .setContent(ByteString.copyFrom(rb))
                                    .setContentSize(rb.length)
                                    .setCreatedAtMillis(System.currentTimeMillis())
                                    .build()
                                    .toByteArray();
                    tableOps.add(
                            new CommitTransaction.TableOperation(
                                    OpType.MANIFEST_WRITE.wireName(), "manifest:" + rid, rp));
                    oldToRewritten.put(oldMid, rid);
                }
            }
        }

        byte[] nmp =
                PbManifestContent.newBuilder()
                        .setManifestId(newManifestId)
                        .setContent(ByteString.copyFrom(newManifestBytes))
                        .setContentSize(newManifestBytes.length)
                        .setCreatedAtMillis(System.currentTimeMillis())
                        .build()
                        .toByteArray();
        tableOps.add(
                new CommitTransaction.TableOperation(
                        OpType.MANIFEST_WRITE.wireName(), "manifest:" + newManifestId, nmp));

        List<String> manifestIds = new ArrayList<>();
        if (latestSnapshot != null
                && latestSnapshot.manifestList() != null
                && !latestSnapshot.manifestList().isEmpty()) {
            for (String mid : latestSnapshot.manifestList())
                manifestIds.add(oldToRewritten.getOrDefault(mid, mid));
        }
        manifestIds.add(newManifestId);

        byte[] sp =
                PbSnapshotUpdate.newBuilder()
                        .setBaseSnapshotId(baseSnapshotId)
                        .addAllManifestIds(manifestIds)
                        .setCreatedAtMillis(System.currentTimeMillis())
                        .setSummary("compaction: " + manifestIds.size() + " manifests")
                        .build()
                        .toByteArray();
        tableOps.add(
                new CommitTransaction.TableOperation(
                        OpType.SNAPSHOT_UPDATE.wireName(), "compaction-snapshot", sp));

        return tableOps;
    }

    private Snapshot commitCleanSnapshotViaCatalog(String tableId, List<String> filesToRemove)
            throws IOException {
        if (filesToRemove == null || filesToRemove.isEmpty())
            throw new IllegalArgumentException("filesToRemove must not be empty");
        TableId id = parseTableId(tableId);
        CommitTransaction txn;
        try {
            txn = catalog.newCommit(id);
        } catch (CatalogException e) {
            throw new IOException("Failed to start commit", e);
        }
        long cbsId = txn.baseSnapshotId();

        List<CommitTransaction.TableOperation> to =
                buildCleanOperations(tableId, id, cbsId, filesToRemove);

        CommitTransaction populated = new CommitTransaction(txn.commitId(), id, cbsId, to);
        try {
            catalog.commitWithRetry(
                    populated,
                    3,
                    2.0,
                    freshBase -> buildCleanOperations(tableId, id, freshBase, filesToRemove));
        } catch (CommitConflictException e) {
            try {
                catalog.abort(txn);
            } catch (Exception ae) {
                log.warn("abort failed", ae);
            }
            throw new SnapshotCommitConflictException("Concurrent clean", cbsId, -1L);
        } catch (CatalogException e) {
            try {
                catalog.abort(txn);
            } catch (Exception ae) {
                log.warn("abort failed", ae);
            }
            throw new IOException("Clean commit failed", e);
        }

        long newId = resolveLatestSnapshotId(id);
        invalidateStatistics(tableId, newId);
        List<Snapshot> latestSnapshots;
        try {
            latestSnapshots = catalog.listSnapshots(id);
        } catch (CatalogException e) {
            throw new IOException("Failed to list snapshots after commit", e);
        }
        int manifestCount =
                latestSnapshots.isEmpty() || latestSnapshots.get(0).manifestList() == null
                        ? 0
                        : latestSnapshots.get(0).manifestList().size();
        return Snapshot.of(
                newId,
                List.of("metastore:clean"),
                System.currentTimeMillis(),
                "clean: " + manifestCount + " manifests",
                "clean",
                resolveSchema(id));
    }

    /**
     * Builds the list of TableOperations for a clean commit, including rewriting manifests to
     * remove deleted files, unregistering files, and deleting empty manifests. This method is
     * called during initial commit AND during retry to ensure operations incorporate any concurrent
     * commits.
     */
    private List<CommitTransaction.TableOperation> buildCleanOperations(
            String tableId, TableId id, long baseSnapshotId, List<String> filesToRemove)
            throws IOException {
        Snapshot latestSnapshot = null;
        try {
            if (baseSnapshotId >= 0) {
                latestSnapshot = catalog.getSnapshot(id, baseSnapshotId).orElse(null);
            }
        } catch (CatalogException e) {
            throw new IOException("Failed to get snapshot", e);
        }

        Set<String> rp = new HashSet<>(filesToRemove);
        List<CommitTransaction.TableOperation> to = new ArrayList<>();
        Map<String, String> o2r = new HashMap<>();
        Set<String> mtr = new HashSet<>();
        Set<String> arp = new HashSet<>();

        if (latestSnapshot != null
                && latestSnapshot.manifestList() != null
                && !latestSnapshot.manifestList().isEmpty()) {
            for (String omid : latestSnapshot.manifestList()) {
                Optional<PbManifestContent> co = readManifestContent(tableId, omid);
                if (co.isEmpty()) continue;
                ManifestFile om =
                        manifestMapper.readValue(
                                co.get().getContent().toByteArray(), ManifestFile.class);
                List<DataFile> fd = new ArrayList<>();
                for (DataFile df : om.dataFiles()) {
                    if (!rp.contains(df.filePath())) fd.add(df);
                    else arp.add(df.filePath());
                }
                List<LogFile> fl = new ArrayList<>();
                for (LogFile lf : om.logFiles()) {
                    if (!rp.contains(lf.filePath())) fl.add(lf);
                    else arp.add(lf.filePath());
                }
                if (fd.size() != om.dataFiles().size() || fl.size() != om.logFiles().size()) {
                    if (fd.isEmpty() && fl.isEmpty()) mtr.add(omid);
                    else {
                        String rid = omid + "_clean_v" + System.currentTimeMillis();
                        ManifestFile rw =
                                new ManifestFile(
                                        rid,
                                        RawWiseFile.FileFormat.JSON,
                                        0,
                                        System.currentTimeMillis(),
                                        -1L,
                                        fd,
                                        fl,
                                        om.minSequenceNumber(),
                                        om.maxSequenceNumber(),
                                        om.bloomFilter());
                        byte[] rb = manifestMapper.writeValueAsBytes(rw);
                        byte[] rpb =
                                PbManifestContent.newBuilder()
                                        .setManifestId(rid)
                                        .setContent(ByteString.copyFrom(rb))
                                        .setContentSize(rb.length)
                                        .setCreatedAtMillis(System.currentTimeMillis())
                                        .build()
                                        .toByteArray();
                        to.add(
                                new CommitTransaction.TableOperation(
                                        OpType.MANIFEST_WRITE.wireName(), "manifest:" + rid, rpb));
                        o2r.put(omid, rid);
                    }
                }
            }
        }

        for (String p : arp)
            to.add(
                    new CommitTransaction.TableOperation(
                            OpType.FILE_UNREGISTER.wireName(),
                            "file-unregister:" + p,
                            new byte[0]));
        for (String m : mtr)
            to.add(
                    new CommitTransaction.TableOperation(
                            OpType.MANIFEST_DELETE.wireName(),
                            "manifest-delete:" + m,
                            new byte[0]));

        List<String> mids = new ArrayList<>();
        if (latestSnapshot != null
                && latestSnapshot.manifestList() != null
                && !latestSnapshot.manifestList().isEmpty()) {
            for (String m : latestSnapshot.manifestList())
                if (!mtr.contains(m)) mids.add(o2r.getOrDefault(m, m));
        }

        byte[] sp =
                PbSnapshotUpdate.newBuilder()
                        .setBaseSnapshotId(baseSnapshotId)
                        .addAllManifestIds(mids)
                        .setCreatedAtMillis(System.currentTimeMillis())
                        .setSummary("clean: " + mids.size() + " manifests")
                        .build()
                        .toByteArray();
        to.add(
                new CommitTransaction.TableOperation(
                        OpType.SNAPSHOT_UPDATE.wireName(), "clean-snapshot", sp));

        return to;
    }

    public record SnapshotExpiration(
            long snapshotId, long timestampMillis, long orphanFileBytes, int orphanFileCount) {
        public SnapshotExpiration {
            if (snapshotId < 0)
                throw new IllegalArgumentException("snapshotId must not be negative");
            if (orphanFileBytes < 0)
                throw new IllegalArgumentException("orphanFileBytes must not be negative");
            if (orphanFileCount < 0)
                throw new IllegalArgumentException("orphanFileCount must not be negative");
        }
    }

    public SnapshotExpiration expireSnapshot(
            String tableId, long snapshotId, Set<Long> retainedSnapshotIds) throws IOException {
        TableId id = parseTableId(tableId);
        try {
            List<String> candidatePaths = new ArrayList<>();
            Optional<Snapshot> toExpireOpt = catalog.getSnapshot(id, snapshotId);
            if (toExpireOpt.isEmpty()) {
                return new SnapshotExpiration(snapshotId, 0, 0, 0);
            }
            Snapshot toExpire = toExpireOpt.get();

            if (toExpire.manifestList() != null && !toExpire.manifestList().isEmpty()) {
                for (String mid : toExpire.manifestList()) {
                    String trimmed = mid.trim();
                    if (trimmed.isEmpty()) continue;
                    Optional<PbManifestContent> contentOpt = readManifestContent(tableId, trimmed);
                    if (contentOpt.isPresent()) {
                        ManifestFile mf =
                                manifestMapper.readValue(
                                        contentOpt.get().getContent().toByteArray(),
                                        ManifestFile.class);
                        mf.dataFiles().forEach(df -> candidatePaths.add(df.filePath()));
                        mf.logFiles().forEach(lf -> candidatePaths.add(lf.filePath()));
                    }
                }
            }

            Set<String> retainedPaths = new HashSet<>();
            boolean anyRetainedRead = false;
            boolean hasRetainedIds = false;
            for (long rid : retainedSnapshotIds) {
                if (rid == snapshotId) continue;
                hasRetainedIds = true;
                try {
                    Optional<Snapshot> rsOpt = catalog.getSnapshot(id, rid);
                    if (rsOpt.isEmpty()) continue;
                    Snapshot rs = rsOpt.get();
                    if (rs.manifestList() != null && !rs.manifestList().isEmpty()) {
                        for (String mid : rs.manifestList()) {
                            String trimmed = mid.trim();
                            if (trimmed.isEmpty()) continue;
                            Optional<PbManifestContent> contentOpt =
                                    readManifestContent(tableId, trimmed);
                            if (contentOpt.isPresent()) {
                                try {
                                    ManifestFile mf =
                                            manifestMapper.readValue(
                                                    contentOpt.get().getContent().toByteArray(),
                                                    ManifestFile.class);
                                    mf.dataFiles().forEach(df -> retainedPaths.add(df.filePath()));
                                    mf.logFiles().forEach(lf -> retainedPaths.add(lf.filePath()));
                                    anyRetainedRead = true;
                                } catch (IOException e) {
                                    log.warn(
                                            "Failed to parse manifest {} for retained snapshot {}",
                                            mid,
                                            rid,
                                            e);
                                }
                            }
                        }
                    }
                } catch (CatalogException e) {
                    log.warn("Failed to read retained snapshot {} for expireSnapshot", rid, e);
                }
            }
            if (hasRetainedIds && !anyRetainedRead) {
                throw new RuntimeException(
                        "All retained snapshot reads failed for table "
                                + tableId
                                + " while expiring snapshot "
                                + snapshotId
                                + ": refusing to proceed with empty retainedPaths");
            }

            List<String> filesToDelete = new ArrayList<>();
            for (String path : candidatePaths) {
                if (!retainedPaths.contains(path)) filesToDelete.add(path);
            }

            long byteEstimate = 0L;
            if (!filesToDelete.isEmpty()) {
                commitCleanSnapshot(tableId, filesToDelete);
                byteEstimate = (long) filesToDelete.size() * 4096L;
            }
            byte[] snapshotKey = Keys.snapshotKey(id, snapshotId);
            store.delete(snapshotKey);

            if (toExpire.hasManifestList()) {
                for (String mid : toExpire.manifestList()) {
                    byte[] idxKey = Keys.manifestIndexKey(id, mid);
                    try {
                        Optional<byte[]> commitIdBytes = store.get(idxKey);
                        if (commitIdBytes.isPresent()) {
                            long commitId = ByteBuffer.wrap(commitIdBytes.get()).getLong();
                            byte[] opKey =
                                    Keys.operationPayloadKey(
                                            id, commitId, OpType.MANIFEST_WRITE.wireName(), mid);
                            store.delete(opKey);
                        }
                    } catch (Exception e) {
                        log.warn(
                                "Failed to clean manifest {} during expireSnapshot for {}",
                                mid,
                                tableId,
                                e);
                    }
                    store.delete(idxKey);
                }
            }

            return new SnapshotExpiration(
                    snapshotId, toExpire.timestampMillis(), byteEstimate, filesToDelete.size());
        } catch (CatalogException e) {
            throw new IOException("Failed to expire snapshot " + snapshotId + " for " + tableId, e);
        }
    }

    @Override
    public Optional<Snapshot> latestSnapshot(String tableId) throws IOException {
        try {
            TableId id = parseTableId(tableId);
            List<Snapshot> ss = catalog.listSnapshots(id);
            return ss.isEmpty() ? Optional.empty() : Optional.of(ss.get(0));
        } catch (CatalogException e) {
            throw new IOException("Failed to get latest snapshot for table: " + tableId, e);
        }
    }

    @Override
    public List<Snapshot> getSnapshots(String tableId) throws IOException {
        try {
            return catalog.listSnapshots(parseTableId(tableId));
        } catch (CatalogException e) {
            throw new IOException("Failed to get snapshots for table: " + tableId, e);
        }
    }

    @Override
    public List<Snapshot> getRecentSnapshots(String tableId, int count) throws IOException {
        if (count <= 0) {
            throw new IllegalArgumentException("count must be positive, got: " + count);
        }
        List<Snapshot> all = getSnapshots(tableId);
        if (all.size() <= count) return new ArrayList<>(all);
        return new ArrayList<>(all.subList(0, count));
    }

    public Map<Integer, List<DataFile>> getLevelLayout(String tableId, long snapshotId)
            throws IOException {
        List<DataFile> allFiles = listDataFiles(tableId, snapshotId);
        Map<Integer, List<DataFile>> layout = new HashMap<>();
        for (DataFile df : allFiles) {
            layout.computeIfAbsent(df.compactionLevel(), k -> new ArrayList<>()).add(df);
        }
        return layout;
    }

    public Optional<Snapshot> getSnapshot(String tableId, long snapshotId) throws IOException {
        try {
            return catalog.getSnapshot(parseTableId(tableId), snapshotId);
        } catch (CatalogIOException e) {
            throw new IOException("Failed to get snapshot", e);
        } catch (CatalogException e) {
            log.warn("Failed to get snapshot {} for table {}", snapshotId, tableId, e);
            return Optional.empty();
        }
    }

    @Override
    public List<DataFile> listDataFiles(String tableId, long snapshotId) throws IOException {
        Optional<Snapshot> so = getSnapshot(tableId, snapshotId);
        if (so.isEmpty() || !so.get().hasManifestList()) return List.of();
        List<DataFile> dfs = new ArrayList<>();
        for (String m : so.get().manifestList()) {
            Optional<PbManifestContent> c = readManifestContent(tableId, m);
            if (c.isPresent())
                dfs.addAll(
                        manifestMapper
                                .readValue(c.get().getContent().toByteArray(), ManifestFile.class)
                                .dataFiles());
        }
        return dfs;
    }

    public List<LogFile> listLogFiles(String tableId, long snapshotId) throws IOException {
        Optional<Snapshot> so = getSnapshot(tableId, snapshotId);
        if (so.isEmpty() || !so.get().hasManifestList()) return List.of();
        List<LogFile> lfs = new ArrayList<>();
        for (String m : so.get().manifestList()) {
            Optional<PbManifestContent> c = readManifestContent(tableId, m);
            if (c.isPresent())
                lfs.addAll(
                        manifestMapper
                                .readValue(c.get().getContent().toByteArray(), ManifestFile.class)
                                .logFiles());
        }
        return lfs;
    }

    public List<ManifestFile> listManifestFiles(String tableId, long snapshotId)
            throws IOException {
        Optional<Snapshot> so = getSnapshot(tableId, snapshotId);
        if (so.isEmpty() || !so.get().hasManifestList()) return List.of();
        List<ManifestFile> mfs = new ArrayList<>();
        for (String m : so.get().manifestList()) {
            Optional<PbManifestContent> c = readManifestContent(tableId, m);
            if (c.isPresent())
                mfs.add(
                        manifestMapper.readValue(
                                c.get().getContent().toByteArray(), ManifestFile.class));
        }
        return mfs;
    }

    public long countTombstones(String tableId, long snapshotId) throws IOException {
        List<LogFile> logFiles = listLogFiles(tableId, snapshotId);
        return logFiles.stream()
                .filter(lf -> lf.logType() == LogFile.LogType.DELETE_VECTOR)
                .mapToLong(LogFile::rowCount)
                .sum();
    }

    public void persistStatistics(String tableId, long snapshotId, SnapshotStatistics stats)
            throws IOException {
        TableId id = parseTableId(tableId);
        byte[] key = Keys.snapshotStatsKey(id, snapshotId);
        byte[] value = manifestMapper.writeValueAsBytes(stats);
        // Use CAS (NotExists) to avoid overwriting already-persisted statistics
        store.compareAndSwap(
                List.of(new Precondition.NotExists(key)), List.of(new Mutation.Put(key, value)));
    }

    public Optional<SnapshotStatistics> getStatistics(String tableId, long snapshotId)
            throws IOException {
        TableId id = parseTableId(tableId);
        byte[] key = Keys.snapshotStatsKey(id, snapshotId);
        Optional<byte[]> raw = store.get(key);
        if (raw.isEmpty()) return Optional.empty();
        return Optional.of(manifestMapper.readValue(raw.get(), SnapshotStatistics.class));
    }

    public SnapshotStatistics getOrComputeStatistics(String tableId, long snapshotId)
            throws IOException {
        Optional<SnapshotStatistics> cached = getStatistics(tableId, snapshotId);
        if (cached.isPresent() && !cached.get().isStale(STATS_MAX_AGE_MS)) {
            return cached.get();
        }
        if (cached.isPresent()) {
            log.debug(
                    "SnapshotStatistics stale for table={} snapshot={}, recomputing",
                    tableId,
                    snapshotId);
        }

        List<DataFile> dataFiles = listDataFiles(tableId, snapshotId);
        long totalRows = dataFiles.stream().mapToLong(DataFile::rowCount).sum();
        long totalBytes = dataFiles.stream().mapToLong(DataFile::fileSizeBytes).sum();
        int fileCount = dataFiles.size();
        long tombstoneCount = countTombstones(tableId, snapshotId);

        ColumnStatistics merged = ColumnStatistics.EMPTY;
        for (DataFile df : dataFiles) {
            if (df.columnStatistics() != null) {
                merged = merged.merge(df.columnStatistics());
            }
        }

        SnapshotStatistics stats =
                new SnapshotStatistics(totalRows, totalBytes, fileCount, tombstoneCount, merged);
        persistStatistics(tableId, snapshotId, stats);
        return stats;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    /**
     * Invalidates cached SnapshotStatistics for the given table and snapshot. Should be called
     * after flush, compaction, or clean operations that modify the snapshot's data file
     * composition.
     */
    public void invalidateStatistics(String tableId, long snapshotId) throws IOException {
        TableId id = parseTableId(tableId);
        byte[] key = Keys.snapshotStatsKey(id, snapshotId);
        store.delete(key);
        log.debug("Invalidated SnapshotStatistics for table={} snapshot={}", tableId, snapshotId);
    }

    /**
     * Invalidates all cached SnapshotStatistics for a table by scanning the stats key prefix and
     * deleting each entry. Useful after major compaction or bulk clean operations that affect
     * multiple snapshots.
     */
    public void invalidateAllStatistics(String tableId) throws IOException {
        TableId id = parseTableId(tableId);
        byte[] statsPrefix = Keys.snapshotStatsPrefix(id);
        int count = 0;
        try (CloseableIterator<KeyValue> it = store.scan(statsPrefix)) {
            while (it.hasNext()) {
                store.delete(it.next().key());
                count++;
            }
        } catch (IOException e) {
            log.warn("Failed to scan keys for statistics invalidation: {}", e.getMessage());
            throw e;
        }
        log.debug("Invalidated {} SnapshotStatistics entries for table={}", count, tableId);
    }

    private long resolveLatestSnapshotId(TableId id) throws IOException {
        try {
            List<Snapshot> ss = catalog.listSnapshots(id);
            return ss.isEmpty() ? -1L : ss.get(0).snapshotId();
        } catch (CatalogException e) {
            throw new IOException("Failed to resolve latest snapshot", e);
        }
    }

    private Schema resolveSchema(TableId id) {
        try {
            return catalog.loadTable(id).meta().schema();
        } catch (Exception e) {
            log.warn("Failed to resolve schema", e);
            return null;
        }
    }

    private Optional<PbManifestContent> readManifestContent(String tableId, String manifestId)
            throws IOException {
        TableId id = parseTableId(tableId);
        byte[] idxKey = Keys.manifestIndexKey(id, manifestId);
        Optional<byte[]> commitIdBytes = store.get(idxKey);
        if (commitIdBytes.isPresent()) {
            long commitId = ByteBuffer.wrap(commitIdBytes.get()).getLong();
            byte[] opKey =
                    Keys.operationPayloadKey(
                            id, commitId, OpType.MANIFEST_WRITE.wireName(), manifestId);
            Optional<byte[]> payload = store.get(opKey);
            if (payload.isPresent()) {
                try {
                    return Optional.of(PbManifestContent.parseFrom(payload.get()));
                } catch (InvalidProtocolBufferException e) {
                    log.error(
                            "Corrupt manifest content at key: {}",
                            java.util.HexFormat.of().formatHex(opKey));
                    return Optional.empty();
                }
            }
            log.warn(
                    "Manifest index found but operation payload missing for manifestId={}, commitId={}",
                    manifestId,
                    commitId);
            return Optional.empty();
        }

        log.debug("No manifest index for manifestId={}, falling back to scan", manifestId);
        byte[] prefix = Keys.operationPayloadPrefix(id, OpType.MANIFEST_WRITE.wireName());
        try (CloseableIterator<KeyValue> it = store.scan(prefix)) {
            while (it.hasNext()) {
                KeyValue kv = it.next();
                try {
                    PbManifestContent content = PbManifestContent.parseFrom(kv.value());
                    if (content.getManifestId().equals(manifestId)) {
                        return Optional.of(content);
                    }
                } catch (InvalidProtocolBufferException e) {
                    log.error(
                            "Corrupt manifest content at key: {}",
                            java.util.HexFormat.of().formatHex(kv.key()));
                }
            }
        }
        return Optional.empty();
    }

    private static TableId parseTableId(String rawId) {
        return TableId.parseLenient(rawId);
    }

    /**
     * Writes a {@link TxnCommitRecord} to the Metastore using CAS (NotExists) for idempotency. If a
     * record already exists for the given txnId (e.g., PublishStage wrote it first, or
     * crash-recovery replay), the CAS fails and the write is treated as an idempotent success.
     *
     * <p>This method is internal to {@code commitSnapshotViaCatalog()} and does not affect
     * Compaction/Clean/LazyUpdate paths (they don't read or write TxnCommitRecord).
     *
     * <p>Key format: {@code /txn_commit_record/{catalog}/{db}/{table}/{txnId}}
     *
     * @param tableId raw table identifier string
     * @param id parsed TableId
     * @param commitId the commit ID (≡ txnId ≡ commitTs per unified strategy)
     * @param baseSnapshotId the base snapshot ID (previousSnapshotId)
     * @param result the newly committed Snapshot
     */
    private void writeTxnCommitRecordIfAbsent(
            String tableId, TableId id, long commitId, long baseSnapshotId, Snapshot result) {
        try {
            int schemaVersion = (int) result.schemaVersion();
            TxnCommitRecord record =
                    new TxnCommitRecord(
                            commitId,
                            commitId,
                            tableId,
                            java.util.Set.of(tableId),
                            result.snapshotId(),
                            baseSnapshotId,
                            schemaVersion,
                            System.currentTimeMillis(),
                            CommitType.OLTP);
            byte[] key =
                    TxnCommitRecord.buildMetastoreKey(
                            catalog.name(), id.database(), id.tableName(), commitId);
            byte[] value = record.toJson();

            boolean casSuccess =
                    store.compareAndSwap(
                            List.of(new Precondition.NotExists(key)),
                            List.of(new Mutation.Put(key, value)));

            if (casSuccess) {
                log.info(
                        "SnapshotManager: TxnCommitRecord written for txn={} table={} snapshot={}",
                        commitId,
                        tableId,
                        result.snapshotId());
            } else {
                log.info(
                        "SnapshotManager: TxnCommitRecord already exists for txn={} (idempotent), table={}",
                        commitId,
                        tableId);
            }
        } catch (IOException e) {
            log.error(
                    "SnapshotManager: Failed to write TxnCommitRecord for txn={} table={}. "
                            + "Snapshot is committed but TxnCommitRecord is missing. "
                            + "Visibility checks may be affected.",
                    commitId,
                    tableId,
                    e);
        } catch (RuntimeException e) {
            log.error(
                    "SnapshotManager: Unexpected error writing TxnCommitRecord for txn={} table={}",
                    commitId,
                    tableId,
                    e);
        }
    }
}
