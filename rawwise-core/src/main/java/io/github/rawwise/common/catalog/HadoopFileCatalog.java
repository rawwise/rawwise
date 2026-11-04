package io.github.rawwise.common.catalog;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.rawwise.common.exception.*;
import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.engine.storage.spi.Storage.StoragePath;
import io.github.rawwise.meta.PbTableMetadata;
import io.github.rawwise.model.record.*;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.impl.RawWiseTableImpl;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HadoopFileCatalog implements Catalog {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().registerModule(new JavaTimeModule());
    private static final int FORMAT_VERSION = 1;

    private final String catalogName;
    private final Storage storage;
    private final String scheme;
    private final String rootPath;
    private final AtomicLong txnIdCounter;
    private final AtomicLong snapshotIdCounter;
    private final Map<String, TableMetadataFile> tableCache;
    private final Map<String, Object> tableLocks;

    public HadoopFileCatalog(String catalogName, Storage storage, String scheme, String rootPath) {
        this.catalogName = catalogName;
        this.storage = storage;
        this.scheme = scheme;
        this.rootPath = rootPath.endsWith("/") ? rootPath : rootPath + "/";
        this.txnIdCounter = new AtomicLong(0);
        this.snapshotIdCounter = new AtomicLong(0);
        this.tableCache = new ConcurrentHashMap<>();
        this.tableLocks = new ConcurrentHashMap<>();
        initCounters();
    }

    private StoragePath sp(String location) {
        return new StoragePath(scheme, location);
    }

    private void initCounters() {
        StoragePath counterPath = sp(rootPath + "counters.json");
        try {
            if (storage.exists(counterPath)) {
                byte[] data = readFully(counterPath);
                CounterFile counters = MAPPER.readValue(data, CounterFile.class);
                txnIdCounter.set(counters.lastTxnId);
                snapshotIdCounter.set(counters.lastSnapshotId);
            }
        } catch (Exception e) {
            log.warn("Failed to load counters, starting from 0", e);
        }

        long maxExistingSnapshotId = scanMaxSnapshotId();
        if (maxExistingSnapshotId > snapshotIdCounter.get()) {
            log.warn(
                    "Counter file lagged behind snapshots ({}/{}), correcting",
                    snapshotIdCounter.get(),
                    maxExistingSnapshotId);
            snapshotIdCounter.set(maxExistingSnapshotId);
        }
    }

    private long scanMaxSnapshotId() {
        long maxId = 0;
        try {
            StoragePath tablesDir = sp(rootPath + "tables/");
            if (!storage.exists(tablesDir)) {
                return 0;
            }
            List<StoragePath> nsEntries = storage.list(tablesDir);
            for (StoragePath nsEntry : nsEntries) {
                String nsName =
                        nsEntry.location().replace(tablesDir.location(), "").replace("/", "");
                if (nsName.isEmpty()) continue;
                StoragePath nsDir = sp(tablesDir.location() + nsName + "/");
                List<StoragePath> tableEntries = storage.list(nsDir);
                for (StoragePath tableEntry : tableEntries) {
                    String tableName = tableEntry.location().replace(nsDir.location(), "");
                    if (tableName.endsWith("/")) {
                        tableName = tableName.substring(0, tableName.length() - 1);
                    }
                    if (tableName.isEmpty() || tableName.equals(".namespace")) continue;
                    StoragePath snapPath = sp(nsDir.location() + tableName + "/snapshots.json");
                    if (storage.exists(snapPath)) {
                        try {
                            TableMetadataFile mdf =
                                    readSnapshotsFile(new TableId(nsName, tableName));
                            if (mdf.lastSnapshotId > maxId) {
                                maxId = mdf.lastSnapshotId;
                            }
                        } catch (Exception e) {
                            log.debug(
                                    "Skipping corrupt snapshots file for {}.{}", nsName, tableName);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to scan max snapshot id, assuming 0", e);
        }
        return maxId;
    }

    private void saveCounters() {
        StoragePath counterPath = sp(rootPath + "counters.json");
        try {
            CounterFile counters = new CounterFile();
            counters.lastTxnId = txnIdCounter.get();
            counters.lastSnapshotId = snapshotIdCounter.get();
            byte[] data = MAPPER.writeValueAsBytes(counters);
            writeFully(counterPath, data);
        } catch (Exception e) {
            log.error("Failed to save counters", e);
        }
    }

    private String tableMetaPath(TableId id) {
        return rootPath + "tables/" + id.database() + "/" + id.tableName() + "/table.meta.pb";
    }

    private String snapshotsPath(TableId id) {
        return rootPath + "tables/" + id.database() + "/" + id.tableName() + "/snapshots.json";
    }

    private String namespaceDir(String namespace) {
        return rootPath + "tables/" + namespace + "/";
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public void createNamespace(String namespace) throws CatalogException {
        if (namespace == null || namespace.isBlank()) {
            throw new IllegalArgumentException("Namespace name cannot be empty");
        }
        StoragePath dir = sp(namespaceDir(namespace));
        try {
            if (storage.exists(dir)) {
                throw new NamespaceNotEmptyException(
                        "Namespace '" + namespace + "' already exists");
            }
            storage.createDirectories(dir);
            writeFully(sp(namespaceDir(namespace) + ".namespace"), new byte[0]);
        } catch (CatalogException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogIOException("Failed to create namespace: " + namespace, e);
        }
    }

    @Override
    public boolean dropNamespace(String namespace) throws CatalogException {
        StoragePath dir = sp(namespaceDir(namespace));
        try {
            if (!storage.exists(dir)) {
                throw new CatalogIOException("Namespace '" + namespace + "' does not exist");
            }
            List<String> tables = listTableNames(namespace);
            if (!tables.isEmpty()) {
                throw new NamespaceNotEmptyException("Namespace '" + namespace + "' is not empty");
            }
            storage.delete(sp(namespaceDir(namespace) + ".namespace"));
            return true;
        } catch (CatalogException e) {
            throw e;
        } catch (IOException e) {
            throw new CatalogIOException("Failed to drop namespace: " + namespace, e);
        }
    }

    @Override
    public boolean namespaceExists(String namespace) throws CatalogException {
        try {
            return storage.exists(sp(namespaceDir(namespace) + ".namespace"));
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public List<String> listNamespaces() throws CatalogException {
        List<String> result = new ArrayList<>();
        StoragePath tablesDir = sp(rootPath + "tables/");
        try {
            if (!storage.exists(tablesDir)) {
                return result;
            }
            List<StoragePath> entries = storage.list(tablesDir);
            for (StoragePath entry : entries) {
                String name = entry.location().replace(tablesDir.location(), "").replace("/", "");
                if (!name.isEmpty()
                        && storage.exists(sp(tablesDir.location() + name + "/.namespace"))) {
                    result.add(name);
                }
            }
        } catch (IOException e) {
            throw new CatalogIOException("Failed to list namespaces", e);
        }
        return result;
    }

    @Override
    public RawWiseTable createTable(
            TableId id, Schema schema, PartitionSpec partitionSpec, TableProperties properties)
            throws CatalogException {
        if (id == null || id.tableName() == null || id.tableName().isBlank()) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }
        if (schema == null) {
            throw new SchemaIncompatibleException("Schema is required");
        }
        StoragePath metaPath = sp(tableMetaPath(id));
        try {
            if (storage.exists(metaPath)) {
                throw new TableAlreadyExistsException(
                        "Table already exists: " + id.database() + "." + id.tableName());
            }
            String namespace = id.database();
            if (!namespaceExists(namespace)) {
                createNamespace(namespace);
            }

            long tableIdNum = Math.abs(UUID.randomUUID().getLeastSignificantBits());
            TableMeta meta =
                    TableMeta.builder()
                            .withTableId(tableIdNum)
                            .withCatalogName(catalogName)
                            .withDatabaseName(namespace)
                            .withTableName(id.tableName())
                            .withTableType(TableMeta.TableType.PRIMARY_KEY)
                            .withSchema(schema)
                            .withPartitionSpec(partitionSpec)
                            .build();

            PbTableMetadata pbMeta = ProtoTableMetaConverter.toPb(meta);
            ensureParentDir(metaPath);
            writeFully(metaPath, pbMeta.toByteArray());

            TableMetadataFile mdf = new TableMetadataFile();
            mdf.lastSnapshotId = 0;
            mdf.lastTxnId = 0;
            mdf.snapshots = new ArrayList<>();
            writeSnapshotsFile(id, mdf);

            tableCache.put(id.database() + "." + id.tableName(), mdf);
            return new RawWiseTableImpl(
                    id.database() + "." + id.tableName(), meta, DynamicTableInfo.builder().build());
        } catch (CatalogException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogIOException("Failed to create table: " + id.tableName(), e);
        }
    }

    @Override
    public boolean dropTable(TableId id, boolean purge) throws CatalogException {
        StoragePath metaPath = sp(tableMetaPath(id));
        try {
            if (!storage.exists(metaPath)) {
                throw new TableNotFoundException(
                        "Table not found: " + id.database() + "." + id.tableName());
            }
            storage.delete(metaPath);
            storage.delete(sp(snapshotsPath(id)));
            tableCache.remove(id.database() + "." + id.tableName());
            return true;
        } catch (IOException e) {
            throw new CatalogIOException("Failed to drop table: " + id.tableName(), e);
        }
    }

    @Override
    public boolean renameTable(TableId from, TableId to) throws CatalogException {
        throw new CatalogIOException("Rename not yet implemented for HadoopFileCatalog");
    }

    @Override
    public boolean tableExists(TableId id) throws CatalogException {
        try {
            return storage.exists(sp(tableMetaPath(id)));
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public RawWiseTable loadTable(TableId id) throws CatalogException {
        try {
            PbTableMetadata pbMeta = readTableMeta(id);
            TableMeta meta = ProtoTableMetaConverter.fromPb(pbMeta);
            TableMetadataFile mdf = readSnapshotsFile(id);
            tableCache.put(id.database() + "." + id.tableName(), mdf);
            return new RawWiseTableImpl(
                    id.database() + "." + id.tableName(), meta, DynamicTableInfo.builder().build());
        } catch (CatalogException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogIOException("Failed to load table: " + id.tableName(), e);
        }
    }

    @Override
    public RawWiseTable loadTable(TableId id, long snapshotId) throws CatalogException {
        throw new UnsupportedOperationException(
                "Time travel (loadTable with snapshotId) is not yet supported in HadoopFileCatalog");
    }

    @Override
    public List<io.github.rawwise.model.record.IndexMeta> indexesOf(TableId id)
            throws CatalogException {
        return List.of();
    }

    private PbTableMetadata readTableMeta(TableId id) throws IOException {
        StoragePath path = sp(tableMetaPath(id));
        if (!storage.exists(path)) {
            throw new TableNotFoundException(
                    "Table not found: " + id.database() + "." + id.tableName());
        }
        byte[] data = readFully(path);
        return PbTableMetadata.parseFrom(data);
    }

    @Override
    public void alterSchema(TableId id, Schema newSchema) throws CatalogException {
        try {
            PbTableMetadata pbMeta = readTableMeta(id);
            TableMeta meta = ProtoTableMetaConverter.fromPb(pbMeta);

            SchemaValidator.validate(
                    meta.schema(), newSchema, SchemaValidator.Compatibility.BACKWARD);

            long newVersion = meta.schema().version() + 1;
            Schema versionedSchema =
                    new Schema(
                            newSchema.structType(),
                            newSchema.properties(),
                            newVersion,
                            newSchema.constraints(),
                            newSchema.columnGroups());

            TableMeta updated = TableMeta.builder(meta).withSchema(versionedSchema).build();

            PbTableMetadata updatedPb = ProtoTableMetaConverter.toPb(updated);
            writeFully(sp(tableMetaPath(id)), updatedPb.toByteArray());
        } catch (CatalogException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogIOException("Failed to alter schema for table: " + id.tableName(), e);
        }
    }

    @Override
    public void updateProperties(TableId id, TableProperties changes) throws CatalogException {
        throw new CatalogIOException("updateProperties not yet implemented for HadoopFileCatalog");
    }

    @Override
    public Optional<Schema> loadSchemaVersion(TableId id, long version) throws CatalogException {
        return Optional.empty();
    }

    @Override
    public List<TableId> listTables(String namespace) throws CatalogException {
        List<TableId> result = new ArrayList<>();
        StoragePath dir = sp(namespaceDir(namespace));
        try {
            if (!storage.exists(dir)) {
                return result;
            }
            List<StoragePath> entries = storage.list(dir);
            for (StoragePath entry : entries) {
                String name = entry.location().replace(dir.location(), "");
                if (name.endsWith("/")) {
                    name = name.substring(0, name.length() - 1);
                }
                if (!name.isEmpty()
                        && !name.equals(".namespace")
                        && storage.exists(sp(dir.location() + name + "/table.meta.pb"))) {
                    result.add(new TableId(namespace, name));
                }
            }
        } catch (IOException e) {
            throw new CatalogIOException("Failed to list tables for namespace: " + namespace, e);
        }
        return result;
    }

    private List<String> listTableNames(String namespace) throws CatalogException {
        List<String> result = new ArrayList<>();
        StoragePath dir = sp(namespaceDir(namespace));
        try {
            if (!storage.exists(dir)) {
                return result;
            }
            List<StoragePath> entries = storage.list(dir);
            for (StoragePath entry : entries) {
                String name = entry.location().replace(dir.location(), "");
                if (name.endsWith("/")) {
                    name = name.substring(0, name.length() - 1);
                }
                if (!name.isEmpty()
                        && !name.equals(".namespace")
                        && storage.exists(sp(dir.location() + name + "/table.meta.pb"))) {
                    result.add(name);
                }
            }
        } catch (IOException e) {
            throw new CatalogIOException(
                    "Failed to list table names for namespace: " + namespace, e);
        }
        return result;
    }

    @Override
    public CommitTransaction newCommit(TableId id) throws CatalogException {
        try {
            TableMetadataFile mdf = readSnapshotsFile(id);
            long txnId = txnIdCounter.incrementAndGet();
            return new CommitTransaction(txnId, id, mdf.lastSnapshotId, new ArrayList<>());
        } catch (CatalogException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogIOException("Failed to start commit for table: " + id.tableName(), e);
        }
    }

    @Override
    public void commit(CommitTransaction txn) throws CatalogException {
        String lockKey = txn.tableId().database() + "." + txn.tableId().tableName();
        Object tableLock = tableLocks.computeIfAbsent(lockKey, k -> new Object());
        synchronized (tableLock) {
            try {
                TableMetadataFile mdf = readSnapshotsFile(txn.tableId());
                if (mdf.lastSnapshotId != txn.baseSnapshotId()) {
                    throw new CommitConflictException(
                            "Concurrent commit detected for table "
                                    + txn.tableId().tableName()
                                    + ": expected baseSnapshotId="
                                    + txn.baseSnapshotId()
                                    + ", actual="
                                    + mdf.lastSnapshotId);
                }

                long newSnapshotId = snapshotIdCounter.incrementAndGet();

                long maxSnapshotId = newSnapshotId;
                String manifestId = UUID.randomUUID().toString();
                Snapshot snapshot =
                        new Snapshot(
                                newSnapshotId,
                                List.of(manifestId),
                                System.currentTimeMillis(),
                                "commit " + txn.commitId(),
                                "commit",
                                null,
                                0,
                                0);

                mdf.snapshots.add(0, toSnapshotEntry(snapshot));
                mdf.lastSnapshotId = maxSnapshotId;
                mdf.lastTxnId = Math.max(mdf.lastTxnId, txn.commitId());
                writeSnapshotsFile(txn.tableId(), mdf);

                saveCounters();

                tableCache.put(txn.tableId().database() + "." + txn.tableId().tableName(), mdf);
            } catch (CatalogException e) {
                throw e;
            } catch (Exception e) {
                throw new CatalogIOException(
                        "Failed to commit for table: " + txn.tableId().tableName(), e);
            }
        }
    }

    @Override
    public void commitWithRetry(CommitTransaction txn, int maxRetries, double backoffMultiplier)
            throws CatalogException {
        int attempt = 0;
        CommitTransaction current = txn;
        while (true) {
            try {
                commit(current);
                return;
            } catch (CommitConflictException e) {
                attempt++;
                if (attempt >= maxRetries) {
                    throw e;
                }
                try {
                    Thread.sleep((long) (100 * Math.pow(backoffMultiplier, attempt)));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new CatalogIOException("Commit retry interrupted", ie);
                }
                try {
                    TableMetadataFile fresh = readSnapshotsFile(current.tableId());
                    current =
                            new CommitTransaction(
                                    current.commitId(),
                                    current.tableId(),
                                    fresh.lastSnapshotId,
                                    current.operations());
                } catch (IOException ioe) {
                    throw new CatalogIOException("Failed to refresh snapshot state for retry", ioe);
                }
            }
        }
    }

    @Override
    public void abort(CommitTransaction txn) throws CatalogException {
        throw new UnsupportedOperationException(
                "abort is not yet supported in HadoopFileCatalog — transactions are fire-and-forget");
    }

    @Override
    public Optional<Snapshot> getSnapshot(TableId id, long snapshotId) throws CatalogException {
        try {
            TableMetadataFile mdf = readSnapshotsFile(id);
            for (SnapshotEntry entry : mdf.snapshots) {
                if (entry.snapshotId == snapshotId) {
                    return Optional.of(fromSnapshotEntry(entry));
                }
            }
        } catch (IOException e) {
            throw new CatalogIOException("Failed to get snapshot for table: " + id.tableName(), e);
        }
        return Optional.empty();
    }

    @Override
    public List<Snapshot> listSnapshots(TableId id) throws CatalogException {
        try {
            TableMetadataFile mdf = readSnapshotsFile(id);
            List<Snapshot> result = new ArrayList<>();
            for (SnapshotEntry entry : mdf.snapshots) {
                result.add(fromSnapshotEntry(entry));
            }
            result.sort(Comparator.comparingLong(Snapshot::snapshotId).reversed());
            return result;
        } catch (IOException e) {
            throw new CatalogIOException(
                    "Failed to list snapshots for table: " + id.tableName(), e);
        }
    }

    @Override
    public void expireSnapshots(TableId id, SnapshotRetention retention) throws CatalogException {
        log.warn("expireSnapshots not yet implemented");
    }

    @Override
    public void invalidateCache(TableId id) {
        tableCache.remove(id.database() + "." + id.tableName());
    }

    @Override
    public void close() {
        tableCache.clear();
    }

    private SnapshotEntry toSnapshotEntry(Snapshot s) {
        SnapshotEntry e = new SnapshotEntry();
        e.snapshotId = s.snapshotId();
        e.manifestList = s.manifestList();
        e.timestampMillis = s.timestampMillis();
        e.summary = s.summary();
        e.operation = s.operation();
        return e;
    }

    private Snapshot fromSnapshotEntry(SnapshotEntry e) {
        return new Snapshot(
                e.snapshotId,
                e.manifestList,
                e.timestampMillis,
                e.summary,
                e.operation,
                null,
                0,
                0);
    }

    private TableMetadataFile readSnapshotsFile(TableId id) throws IOException {
        StoragePath path = sp(snapshotsPath(id));
        if (!storage.exists(path)) {
            TableMetadataFile mdf = new TableMetadataFile();
            mdf.snapshots = new ArrayList<>();
            return mdf;
        }
        byte[] data = readFully(path);
        return MAPPER.readValue(data, TableMetadataFile.class);
    }

    private void writeSnapshotsFile(TableId id, TableMetadataFile mdf) throws IOException {
        StoragePath path = sp(snapshotsPath(id));
        ensureParentDir(path);
        byte[] data = MAPPER.writeValueAsBytes(mdf);
        writeFully(path, data);
    }

    private void ensureParentDir(StoragePath path) throws IOException {
        String location = path.location();
        int lastSlash = location.lastIndexOf('/');
        if (lastSlash > 0) {
            String parent = location.substring(0, lastSlash);
            StoragePath parentPath = sp(parent);
            if (!storage.exists(parentPath)) {
                storage.createDirectories(parentPath);
            }
        }
    }

    private byte[] readFully(StoragePath path) throws IOException {
        Storage.InputHandle handle = storage.openRead(path);
        try (InputStream is = handle.stream()) {
            return is.readAllBytes();
        } finally {
            try {
                handle.close();
            } catch (Exception e) {
                log.warn("Failed to close input handle", e);
            }
        }
    }

    private void writeFully(StoragePath path, byte[] data) throws IOException {
        String tmpLocation = path.location() + ".tmp." + UUID.randomUUID();
        StoragePath tmpPath = sp(tmpLocation);
        Storage.OutputHandle handle = storage.openWrite(tmpPath, Storage.WriteMode.CREATE);
        try (OutputStream os = handle.stream()) {
            os.write(data);
            os.flush();
            handle.sync();
        } finally {
            try {
                handle.close();
            } catch (Exception e) {
                log.warn("Failed to close output handle", e);
            }
        }
        try {
            storage.rename(tmpPath, path);
        } catch (Exception e) {
            try {
                storage.delete(tmpPath);
            } catch (Exception ignored) {
            }
            throw e;
        }
    }

    static class CounterFile {
        @JsonProperty long lastTxnId;
        @JsonProperty long lastSnapshotId;
    }

    static class TableMetadataFile {
        @JsonProperty int formatVersion = FORMAT_VERSION;
        @JsonProperty long lastSnapshotId;
        @JsonProperty long lastTxnId;
        @JsonProperty List<SnapshotEntry> snapshots;
    }

    static class SnapshotEntry {
        @JsonProperty long snapshotId;
        @JsonProperty List<String> manifestList;
        @JsonProperty long timestampMillis;
        @JsonProperty String summary;
        @JsonProperty String operation;
    }
}
