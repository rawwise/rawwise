package io.github.rawwise.common.catalog;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.rawwise.common.collection.iterator.CloseableIterator;
import io.github.rawwise.common.exception.*;
import io.github.rawwise.engine.storage.codec.BodyCodecRegistry;
import io.github.rawwise.meta.PbManifestContent;
import io.github.rawwise.meta.PbSchema;
import io.github.rawwise.meta.PbSchemaHistoryEntry;
import io.github.rawwise.meta.PbSnapshot;
import io.github.rawwise.meta.PbSnapshotUpdate;
import io.github.rawwise.meta.PbTableMetadata;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import io.github.rawwise.model.op.OpType;
import io.github.rawwise.model.record.*;
import io.github.rawwise.model.record.PrimaryKeyConstraint;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.StructField;
import io.github.rawwise.model.record.impl.RawWiseTableImpl;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link Catalog} implementation backed by a {@link Metastore}.
 *
 * <h3>Key Responsibilities</h3>
 *
 * <ul>
 *   <li>Namespace management via simple KV entries
 *   <li>Table DDL with {@link PbTableMetadata} serialization
 *   <li>Table loading with Caffeine caching (TTL 5 min, max 1000 entries)
 *   <li>Commit lifecycle: newCommit / commit (OCC+CAS) / abort
 *   <li>Orphan commit-intent GC via background {@link ScheduledExecutorService}
 *   <li>Snapshot listing and expiration
 * </ul>
 *
 * <h3>Thread Safety</h3>
 *
 * <p>{@code createTable} and {@code alterSchema} use CAS-based atomicity via {@link
 * Metastore#compareAndSwap} instead of JVM-level synchronization. Other operations are safe via the
 * linearizable guarantees of {@link Metastore}.
 */
@Slf4j
public final class MetastoreCatalog implements Catalog {

    // ============ Constants ============

    private static final byte[] NAMESPACE_PREFIX = "namespace:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] EMPTY_VALUE = new byte[0];
    private static final long INTENT_TIMEOUT_MS = Duration.ofMinutes(10).toMillis();
    private static final long INTENT_GC_INTERVAL_MS = Duration.ofMinutes(5).toMillis();
    private static final int SUBKEY_BATCH_SIZE = 1000;

    // ============ Fields ============

    private final MetastoreCatalogConfig config;
    private final Metastore store;
    private final String name;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean gcRunning = new AtomicBoolean(false);
    private final Cache<TableId, RawWiseTable> tableCache;
    private final ScheduledExecutorService gcExecutor;

    // ============ Constructor ============

    /**
     * Creates a new {@code MetastoreCatalog}.
     *
     * @param config the catalog configuration (must contain a valid {@link Metastore})
     */
    public MetastoreCatalog(MetastoreCatalogConfig config) {
        this.config = Objects.requireNonNull(config, "config cannot be null");
        this.store = config.store();
        this.name = config.catalogName();

        // Initialize Caffeine cache: max 1000 entries, TTL 5 minutes
        this.tableCache =
                Caffeine.newBuilder()
                        .maximumSize(1000)
                        .expireAfterWrite(5, TimeUnit.MINUTES)
                        .build();

        // Initialize orphan intent GC executor
        this.gcExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "catalog-intent-gc-" + name);
                            t.setDaemon(true);
                            return t;
                        });
        this.gcExecutor.scheduleWithFixedDelay(
                this::gcOrphanIntents,
                INTENT_GC_INTERVAL_MS,
                INTENT_GC_INTERVAL_MS,
                TimeUnit.MILLISECONDS);

        // Add shutdown hook for graceful cleanup
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    try {
                                        close();
                                    } catch (Exception e) {
                                        log.warn(
                                                "Error during shutdown hook close: {}",
                                                e.getMessage());
                                    }
                                },
                                "catalog-shutdown-" + name));

        log.info("MetastoreCatalog initialized: name={}, basePath={}", name, config.basePath());
    }

    // ============ Basic info ============

    @Override
    public String name() {
        return name;
    }

    // ============ Namespace operations ============

    @Override
    public void createNamespace(String namespace) throws CatalogException {
        ensureOpen();
        if (namespace == null || namespace.isBlank()) {
            throw new IllegalArgumentException("Namespace cannot be null or blank");
        }
        try {
            byte[] key = namespaceKey(namespace);
            store.put(key, EMPTY_VALUE);
            log.info("Created namespace: {}", namespace);
        } catch (IOException e) {
            throw new CatalogIOException(
                    "Failed to create namespace: " + namespace, "Metastore", e);
        }
    }

    @Override
    public boolean dropNamespace(String namespace) throws CatalogException {
        ensureOpen();
        if (namespace == null || namespace.isBlank()) {
            throw new IllegalArgumentException("Namespace cannot be null or blank");
        }
        try {
            // Check that no tables exist in this namespace
            List<TableId> tables = listTables(namespace);
            if (!tables.isEmpty()) {
                throw new NamespaceNotEmptyException(
                        "Namespace '"
                                + namespace
                                + "' is not empty ("
                                + tables.size()
                                + " tables)");
            }
            byte[] key = namespaceKey(namespace);
            store.delete(key);
            log.info("Dropped namespace: {}", namespace);
            return true;
        } catch (CatalogException e) {
            throw e;
        } catch (IOException e) {
            throw new CatalogIOException("Failed to drop namespace: " + namespace, "Metastore", e);
        }
    }

    @Override
    public boolean namespaceExists(String namespace) throws CatalogException {
        ensureOpen();
        if (namespace == null || namespace.isBlank()) {
            return false;
        }
        try {
            return store.exists(namespaceKey(namespace));
        } catch (IOException e) {
            throw new CatalogIOException(
                    "Failed to check namespace existence: " + namespace, "Metastore", e);
        }
    }

    @Override
    public List<String> listNamespaces() throws CatalogException {
        ensureOpen();
        try {
            List<String> namespaces = new ArrayList<>();
            try (CloseableIterator<KeyValue> it = store.scan(NAMESPACE_PREFIX)) {
                while (it.hasNext()) {
                    byte[] key = it.next().key();
                    String name =
                            new String(
                                    key,
                                    NAMESPACE_PREFIX.length,
                                    key.length - NAMESPACE_PREFIX.length,
                                    StandardCharsets.UTF_8);
                    namespaces.add(name);
                }
            }
            return namespaces;
        } catch (IOException e) {
            throw new CatalogIOException("Failed to list namespaces", "Metastore", e);
        }
    }

    // ============ Table DDL ============

    @Override
    public RawWiseTable createTable(
            TableId id, Schema schema, PartitionSpec partitionSpec, TableProperties properties)
            throws CatalogException {
        ensureOpen();
        validateTableId(id);
        validateBodyCodecProperty(properties);
        validateColumnAndCgNames(schema);

        try {
            // 1. Allocate numeric table ID (may waste one on conflict, but conflicts are rare)
            long numericTableId = allocateTableId();

            // 2. Determine table type from properties
            TableMeta.TableType tableType = determineTableType(properties);

            // 3. Build location path
            String location =
                    config.basePath() + "/" + name + "/" + id.database() + "/" + id.tableName();

            // 4. Build TableMeta
            TableMeta meta =
                    TableMeta.builder()
                            .withTableId(numericTableId)
                            .withCatalogName(name)
                            .withDatabaseName(id.database())
                            .withTableName(id.tableName())
                            .withTableType(tableType)
                            .withSchema(schema)
                            .withPrimaryKey(resolvePrimaryKey(schema, tableType, properties))
                            .withPartitionSpec(
                                    partitionSpec != null
                                            ? partitionSpec
                                            : PartitionSpec.unpartitioned())
                            .withProperties(new HashMap<>(properties.props()))
                            .withLocation(location)
                            .build();

            // 5. Serialize to protobuf
            PbTableMetadata pbMeta = ProtoTableMetaConverter.toPb(meta);
            byte[] pbBytes = pbMeta.toByteArray();

            // 6. Build table list entry key for namespace indexing
            byte[] listKey = Keys.namespaceTableListKey(id.database());
            byte[] tableEntryKey = new byte[listKey.length + id.tableName().length()];
            System.arraycopy(listKey, 0, tableEntryKey, 0, listKey.length);
            System.arraycopy(
                    id.tableName().getBytes(StandardCharsets.UTF_8),
                    0,
                    tableEntryKey,
                    listKey.length,
                    id.tableName().length());

            // 7. Scan for residual keys from previous lifecycle before CAS
            // (safe: metaKey doesn't exist yet, so no concurrent access to this prefix)
            byte[] metaKey = tableMetaKey(id);
            byte[] epochKey = Keys.tableEpochKey(id);
            byte[] tableIdKey = Keys.tableIdentityKey(id);
            byte[] prefix = Keys.tablePrefix(id);
            List<Mutation> residueDeletes = new ArrayList<>();
            try (CloseableIterator<KeyValue> it = store.scan(prefix)) {
                while (it.hasNext()) {
                    byte[] key = it.next().key();
                    if (!java.util.Arrays.equals(key, epochKey)
                            && !java.util.Arrays.equals(key, tableIdKey)) {
                        residueDeletes.add(new Mutation.Delete(key));
                    }
                }
            }

            // 8. CAS: atomically check table doesn't exist, create metadata + namespace index +
            // epoch + identity
            boolean created =
                    store.compareAndSwap(
                            List.of(new Precondition.NotExists(metaKey)),
                            List.of(
                                    new Mutation.Put(metaKey, pbBytes),
                                    new Mutation.Put(tableEntryKey, EMPTY_VALUE),
                                    new Mutation.Put(epochKey, Keys.encodeEpoch(0)),
                                    new Mutation.Put(tableIdKey, longToBytes(numericTableId))));

            if (!created) {
                throw new TableAlreadyExistsException(
                        "Table already exists: " + id.database() + "." + id.tableName());
            }

            // 9. Delete residual keys from previous lifecycle (paginated, best-effort)
            if (!residueDeletes.isEmpty()) {
                try {
                    int residueCount = residueDeletes.size();
                    int offset = 0;
                    while (offset < residueCount) {
                        int end = Math.min(offset + SUBKEY_BATCH_SIZE, residueCount);
                        store.batchWrite(residueDeletes.subList(offset, end));
                        offset = end;
                    }
                    log.info(
                            "Cleaned {} residual keys from previous lifecycle for {}.{}",
                            residueCount,
                            id.database(),
                            id.tableName());
                } catch (Exception e) {
                    log.warn(
                            "Failed to clean residual keys for {}.{} "
                                    + "(table created successfully, cleanup will retry on next access)",
                            id.database(),
                            id.tableName(),
                            e);
                }
            }

            log.info(
                    "Created table: {}.{} (tableId={}, type={}, location={})",
                    id.database(),
                    id.tableName(),
                    numericTableId,
                    tableType,
                    location);

            // 10. Load and return the table (also populates cache)
            return loadTable(id);

        } catch (TableAlreadyExistsException e) {
            throw e;
        } catch (IOException e) {
            throw new CatalogIOException(
                    "Failed to create table: " + id.database() + "." + id.tableName(),
                    "Metastore",
                    e);
        }
    }

    @Override
    public boolean dropTable(TableId id, boolean purge) throws CatalogException {
        ensureOpen();
        validateTableId(id);

        if (purge) {
            throw new UnsupportedOperationException(
                    "purge=true (data file deletion) is not yet implemented");
        }

        try {
            byte[] metaKey = tableMetaKey(id);
            Optional<byte[]> currentMeta = store.get(metaKey);
            if (currentMeta.isEmpty()) {
                log.warn("Table not found for drop: {}.{}", id.database(), id.tableName());
                invalidateCache(id);
                return false;
            }
            byte[] originalMetaBytes = currentMeta.get();

            byte[] epochKey = Keys.tableEpochKey(id);
            byte[] tableIdKey = Keys.tableIdentityKey(id);
            long currentEpoch = store.get(epochKey).map(Keys::decodeEpoch).orElse(0L);

            byte[] listKey = Keys.namespaceTableListKey(id.database());
            byte[] tableEntryKey = new byte[listKey.length + id.tableName().length()];
            System.arraycopy(listKey, 0, tableEntryKey, 0, listKey.length);
            System.arraycopy(
                    id.tableName().getBytes(StandardCharsets.UTF_8),
                    0,
                    tableEntryKey,
                    listKey.length,
                    id.tableName().length());

            boolean success =
                    store.compareAndSwap(
                            List.of(
                                    new Precondition.Exists(metaKey),
                                    new Precondition.ValueEquals(metaKey, originalMetaBytes)),
                            List.of(
                                    new Mutation.Delete(metaKey),
                                    new Mutation.Delete(tableEntryKey),
                                    new Mutation.Delete(tableIdKey),
                                    new Mutation.Put(
                                            epochKey, Keys.encodeEpoch(currentEpoch + 1))));

            if (!success) {
                log.warn(
                        "Drop CAS failed for {}.{} — table may have been concurrently dropped or recreated",
                        id.database(),
                        id.tableName());
                invalidateCache(id);
                return false;
            }

            long postDropEpoch = currentEpoch + 1;
            int cleaned = 0;
            try {
                byte[] prefix = Keys.tablePrefix(id);
                List<Mutation> batch = new ArrayList<>(SUBKEY_BATCH_SIZE);
                try (CloseableIterator<KeyValue> it = store.scan(prefix)) {
                    while (it.hasNext()) {
                        byte[] key = it.next().key();
                        if (java.util.Arrays.equals(key, metaKey)) {
                            log.warn(
                                    "Table {}.{} recreated during drop orphan scan — "
                                            + "skipping orphan cleanup to preserve new lifecycle",
                                    id.database(),
                                    id.tableName());
                            break;
                        }
                        if (!java.util.Arrays.equals(key, epochKey)
                                && !java.util.Arrays.equals(key, tableIdKey)) {
                            batch.add(new Mutation.Delete(key));
                            if (batch.size() >= SUBKEY_BATCH_SIZE) {
                                long recheckEpoch =
                                        store.get(epochKey).map(Keys::decodeEpoch).orElse(-1L);
                                if (recheckEpoch != postDropEpoch) {
                                    log.warn(
                                            "Table {}.{} recreated during drop orphan scan "
                                                    + "(epoch {} → {}), aborting cleanup",
                                            id.database(),
                                            id.tableName(),
                                            postDropEpoch,
                                            recheckEpoch);
                                    batch.clear();
                                    break;
                                }
                                store.batchWrite(batch);
                                cleaned += batch.size();
                                batch.clear();
                            }
                        }
                    }
                    if (!batch.isEmpty()) {
                        long recheckEpoch = store.get(epochKey).map(Keys::decodeEpoch).orElse(-1L);
                        if (recheckEpoch != postDropEpoch) {
                            log.warn(
                                    "Table {}.{} recreated during drop orphan scan "
                                            + "(epoch {} → {}), discarding final batch",
                                    id.database(),
                                    id.tableName(),
                                    postDropEpoch,
                                    recheckEpoch);
                            batch.clear();
                        } else {
                            store.batchWrite(batch);
                            cleaned += batch.size();
                        }
                    }
                }
            } catch (IOException e) {
                log.warn(
                        "Orphan cleanup partially failed for {}.{} "
                                + "(table dropped successfully, {} keys cleaned before error): {}",
                        id.database(),
                        id.tableName(),
                        cleaned,
                        e.getMessage());
            }

            invalidateCache(id);

            log.info(
                    "Dropped table (metadata only): {}.{} ({} orphan keys cleaned)",
                    id.database(),
                    id.tableName(),
                    cleaned);
            return true;

        } catch (IOException e) {
            throw new CatalogIOException(
                    "Failed to drop table: " + id.database() + "." + id.tableName(),
                    "Metastore",
                    e);
        }
    }

    @Override
    public boolean renameTable(TableId from, TableId to) throws CatalogException {
        ensureOpen();
        validateTableId(from);
        validateTableId(to);

        try {
            byte[] fromKey = tableMetaKey(from);
            byte[] toKey = tableMetaKey(to);

            if (!store.exists(fromKey)) {
                throw new TableNotFoundException(
                        "Source table not found: " + from.database() + "." + from.tableName(),
                        from.database() + "." + from.tableName());
            }
            if (store.exists(toKey)) {
                log.warn("Cannot rename to existing table: {}.{}", to.database(), to.tableName());
                return false;
            }

            Optional<byte[]> metaBytes = store.get(fromKey);
            if (metaBytes.isEmpty()) {
                throw new TableNotFoundException(
                        "Table disappeared during rename: " + from.database());
            }

            byte[] originalBytes = metaBytes.get();
            PbTableMetadata pbMeta = PbTableMetadata.parseFrom(originalBytes);
            PbTableMetadata.Builder builder = pbMeta.toBuilder();
            builder.getIdentifierBuilder().setDatabase(to.database()).setTable(to.tableName());
            byte[] updatedBytes = builder.build().toByteArray();

            byte[] oldNsKey = Keys.namespaceTableListKey(from.database());
            byte[] oldEntryKey = new byte[oldNsKey.length + from.tableName().length()];
            System.arraycopy(oldNsKey, 0, oldEntryKey, 0, oldNsKey.length);
            System.arraycopy(
                    from.tableName().getBytes(StandardCharsets.UTF_8),
                    0,
                    oldEntryKey,
                    oldNsKey.length,
                    from.tableName().length());

            byte[] newNsKey = Keys.namespaceTableListKey(to.database());
            byte[] newEntryKey = new byte[newNsKey.length + to.tableName().length()];
            System.arraycopy(newNsKey, 0, newEntryKey, 0, newNsKey.length);
            System.arraycopy(
                    to.tableName().getBytes(StandardCharsets.UTF_8),
                    0,
                    newEntryKey,
                    newNsKey.length,
                    to.tableName().length());

            byte[] intentKey = Keys.renameIntentKey(to);
            byte[] intentValue = buildRenameIntentValue(from);

            boolean success =
                    store.compareAndSwap(
                            List.of(
                                    new Precondition.ValueEquals(fromKey, originalBytes),
                                    new Precondition.NotExists(toKey)),
                            List.of(
                                    new Mutation.Delete(fromKey),
                                    new Mutation.Put(toKey, updatedBytes),
                                    new Mutation.Delete(oldEntryKey),
                                    new Mutation.Put(newEntryKey, EMPTY_VALUE),
                                    new Mutation.Put(intentKey, intentValue)));

            if (!success) {
                log.warn("Rename CAS failed: {} -> {}", from, to);
                invalidateCache(from);
                invalidateCache(to);
                return false;
            }

            invalidateCache(from);
            invalidateCache(to);

            boolean relocated = false;
            try {
                relocateSubkeys(from, to);
                store.delete(intentKey);
                relocated = true;
            } catch (IOException e) {
                log.error(
                        "Subkey relocation failed for rename {} -> {} (recovery will run on next loadTable): {}",
                        from,
                        to,
                        e.getMessage(),
                        e);
                try {
                    List<byte[]> toDelete = new ArrayList<>();
                    byte[] oldPrefix = Keys.tablePrefix(from);
                    byte[] newPrefix = Keys.tablePrefix(to);
                    int oldPrefixLen = oldPrefix.length;
                    int newPrefixLen = newPrefix.length;
                    try (CloseableIterator<KeyValue> it = store.scan(oldPrefix)) {
                        while (it.hasNext()) {
                            byte[] oldKey = it.next().key();
                            int suffixLen = oldKey.length - oldPrefixLen;
                            if (suffixLen >= 0) {
                                byte[] newKey = new byte[newPrefixLen + suffixLen];
                                System.arraycopy(newPrefix, 0, newKey, 0, newPrefixLen);
                                System.arraycopy(
                                        oldKey, oldPrefixLen, newKey, newPrefixLen, suffixLen);
                                if (store.exists(newKey)) {
                                    toDelete.add(oldKey);
                                }
                            }
                        }
                    }
                    if (!toDelete.isEmpty()) {
                        List<Mutation> dels = new ArrayList<>(toDelete.size());
                        for (byte[] k : toDelete) {
                            dels.add(new Mutation.Delete(k));
                        }
                        store.batchWrite(dels);
                        log.info(
                                "Cleaned {} already-relocated keys from old prefix {} -> {}",
                                toDelete.size(),
                                from,
                                to);
                    }
                } catch (IOException e2) {
                    log.warn(
                            "Best-effort cleanup after relocateSubkeys failure also failed: {}",
                            e2.getMessage());
                }
            }

            if (relocated) {
                log.info("Renamed table: {} -> {}", from, to);
            } else {
                log.warn(
                        "Table metadata renamed but subkey relocation deferred: {} -> {}",
                        from,
                        to);
            }
            return true;

        } catch (TableNotFoundException e) {
            throw e;
        } catch (IOException e) {
            throw new CatalogIOException(
                    "Failed to rename table: " + from + " -> " + to, "Metastore", e);
        }
    }

    /**
     * The {@code relocateSubkeys} uses a paginated prefix scan (batch size {@value
     * #SUBKEY_BATCH_SIZE}) to avoid OOM on tables with millions of subkeys. Each batch is written
     * via best-effort {@link Metastore#batchWrite} — first puts then deletes — so crash
     * mid-relocation is recoverable via {@link #recoverRenameIntent}.
     *
     * <p><b>Crash safety:</b> a rename-intent marker ({@code :rename-intent}) is written at the
     * target prefix before subkey relocation and deleted after. On next {@code loadTable}, if the
     * marker exists, {@link #doLoadTable} calls {@code recoverRenameIntent} to resume.
     *
     * <p><b>Atomicity semantics:</b> {@code batchWrite} is not atomic — InMemoryMetastore applies
     * mutations sequentially while RocksDB wraps them in a single {@code WriteBatch}. This is
     * acceptable because orphaned keys at the old prefix are harmless (no correctness impact) and
     * the rename-intent marker enables eventual recovery.
     */
    private void relocateSubkeys(TableId from, TableId to) throws IOException {
        byte[] oldPrefix = Keys.tablePrefix(from);
        byte[] newPrefix = Keys.tablePrefix(to);
        int oldPrefixLen = oldPrefix.length;
        int newPrefixLen = newPrefix.length;

        long totalRelocated = 0;

        try (CloseableIterator<KeyValue> it = store.scan(oldPrefix)) {
            List<KeyValue> batch = new ArrayList<>(SUBKEY_BATCH_SIZE);

            while (it.hasNext()) {
                batch.add(it.next());

                if (batch.size() >= SUBKEY_BATCH_SIZE) {
                    totalRelocated +=
                            flushSubkeyBatch(
                                    batch, oldPrefix, oldPrefixLen, newPrefix, newPrefixLen);
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                totalRelocated +=
                        flushSubkeyBatch(batch, oldPrefix, oldPrefixLen, newPrefix, newPrefixLen);
            }
        }

        log.info("Relocated {} subkeys: {} -> {}", totalRelocated, from, to);
    }

    private long flushSubkeyBatch(
            List<KeyValue> batch,
            byte[] oldPrefix,
            int oldPrefixLen,
            byte[] newPrefix,
            int newPrefixLen)
            throws IOException {
        List<Mutation> putMutations = new ArrayList<>(batch.size());
        List<Mutation> deleteMutations = new ArrayList<>(batch.size());

        for (KeyValue kv : batch) {
            byte[] oldKey = kv.key();
            int suffixLen = oldKey.length - oldPrefixLen;
            if (suffixLen < 0) {
                throw new IOException(
                        "Scanned key does not start with old prefix: "
                                + HexFormat.of().formatHex(oldKey));
            }
            byte[] newKey = new byte[newPrefixLen + suffixLen];
            System.arraycopy(newPrefix, 0, newKey, 0, newPrefixLen);
            System.arraycopy(oldKey, oldPrefixLen, newKey, newPrefixLen, suffixLen);
            if (store.exists(newKey)) {
                deleteMutations.add(new Mutation.Delete(oldKey));
                continue;
            }
            putMutations.add(new Mutation.Put(newKey, kv.value()));
            deleteMutations.add(new Mutation.Delete(oldKey));
        }

        store.batchWrite(putMutations);
        store.batchWrite(deleteMutations);
        return putMutations.size();
    }

    static byte[] buildRenameIntentValue(TableId from) {
        byte[] dbBytes = from.database().getBytes(StandardCharsets.UTF_8);
        byte[] tblBytes = from.tableName().getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[dbBytes.length + 1 + tblBytes.length];
        System.arraycopy(dbBytes, 0, value, 0, dbBytes.length);
        value[dbBytes.length] = ':';
        System.arraycopy(tblBytes, 0, value, dbBytes.length + 1, tblBytes.length);
        return value;
    }

    private static TableId parseRenameIntentValue(byte[] value) {
        String s = new String(value, StandardCharsets.UTF_8);
        int colon = s.indexOf(':');
        if (colon < 0) {
            throw new IllegalArgumentException("Invalid rename-intent value: " + s);
        }
        return new TableId(s.substring(0, colon), s.substring(colon + 1));
    }

    private void recoverRenameIntent(TableId to) throws IOException {
        byte[] intentKey = Keys.renameIntentKey(to);
        Optional<byte[]> intentValue = store.get(intentKey);
        if (intentValue.isEmpty()) {
            return;
        }

        TableId from;
        try {
            from = parseRenameIntentValue(intentValue.get());
        } catch (IllegalArgumentException e) {
            log.warn("Corrupt rename-intent for {}, deleting marker", to);
            store.delete(intentKey);
            return;
        }

        log.info("Recovering incomplete rename relocation: {} -> {}", from, to);
        relocateSubkeys(from, to);
        store.delete(intentKey);
    }

    @Override
    public boolean tableExists(TableId id) throws CatalogException {
        ensureOpen();
        validateTableId(id);
        try {
            return store.exists(tableMetaKey(id));
        } catch (IOException e) {
            throw new CatalogIOException(
                    "Failed to check table existence: " + id.database() + "." + id.tableName(),
                    "Metastore",
                    e);
        }
    }

    @Override
    public List<TableId> listTables(String namespace) throws CatalogException {
        ensureOpen();
        if (namespace == null || namespace.isBlank()) {
            return List.of();
        }
        try {
            Set<String> tableNames = new LinkedHashSet<>();
            byte[] prefix = Keys.namespaceTableListKey(namespace);

            try (CloseableIterator<KeyValue> it = store.scan(prefix)) {
                while (it.hasNext()) {
                    byte[] key = it.next().key();
                    String tn =
                            new String(
                                    key,
                                    prefix.length,
                                    key.length - prefix.length,
                                    StandardCharsets.UTF_8);
                    tableNames.add(tn);
                }
            }

            return tableNames.stream().map(tn -> new TableId(namespace, tn)).toList();

        } catch (IOException e) {
            throw new CatalogIOException(
                    "Failed to list tables in namespace: " + namespace, "Metastore", e);
        }
    }

    // ============ Table loading (read path) ============

    @Override
    public RawWiseTable loadTable(TableId id) throws CatalogException {
        ensureOpen();
        validateTableId(id);

        // Check cache first
        RawWiseTable cached = tableCache.getIfPresent(id);
        if (cached != null) {
            log.debug("Cache hit for table: {}.{}", id.database(), id.tableName());
            return cached;
        }

        // Load from store
        RawWiseTable table = doLoadTable(id);
        tableCache.put(id, table);
        return table;
    }

    @Override
    public RawWiseTable loadTable(TableId id, long snapshotId) throws CatalogException {
        ensureOpen();
        validateTableId(id);

        try {
            TableMeta tableMeta = loadTableMeta(id);

            recoverRenameIntent(id);

            List<Snapshot> snapshots = listSnapshotsInternal(id);
            Snapshot targetSnapshot =
                    snapshots.stream()
                            .filter(s -> s.snapshotId() == snapshotId)
                            .findFirst()
                            .orElse(null);

            DynamicTableInfo dynamicInfo;
            if (targetSnapshot != null) {
                dynamicInfo =
                        DynamicTableInfo.builder().withSnapshots(List.of(targetSnapshot)).build();
            } else {
                dynamicInfo = DynamicTableInfo.builder().build();
            }

            String tableIdentifier = buildTableIdentifier(id);
            return new RawWiseTableImpl(tableIdentifier, tableMeta, dynamicInfo);

        } catch (TableNotFoundException e) {
            throw e;
        } catch (IOException e) {
            throw new CatalogIOException(
                    "Failed to load table: " + id.database() + "." + id.tableName(),
                    "Metastore",
                    e);
        }
    }

    /** Performs the actual table load from Metastore (no cache check). */
    private RawWiseTable doLoadTable(TableId id) throws CatalogException {
        try {
            TableMeta tableMeta = loadTableMeta(id);

            recoverRenameIntent(id);

            // 2. Load snapshots and build dynamic info
            List<Snapshot> snapshots = listSnapshotsInternal(id);
            DynamicTableInfo dynamicInfo =
                    DynamicTableInfo.builder().withSnapshots(snapshots).build();

            // 3. Build table identifier
            String tableIdentifier = buildTableIdentifier(id);

            // 4. Create RawWiseTableImpl (serializable, no Metastore dependency)
            RawWiseTableImpl table = new RawWiseTableImpl(tableIdentifier, tableMeta, dynamicInfo);

            log.debug(
                    "Loaded table: {} (tableId={}, snapshots={})",
                    tableIdentifier,
                    tableMeta.tableId(),
                    snapshots.size());

            return table;

        } catch (TableNotFoundException e) {
            throw e;
        } catch (IOException e) {
            throw new CatalogIOException(
                    "Failed to load table: " + id.database() + "." + id.tableName(),
                    "Metastore",
                    e);
        } catch (Exception e) {
            throw new InvalidMetadataException(
                    "Failed to parse metadata for " + id.database() + "." + id.tableName(),
                    id.database() + "." + id.tableName(),
                    e);
        }
    }

    /** Loads {@link TableMeta} from store, throwing {@link TableNotFoundException} if absent. */
    private TableMeta loadTableMeta(TableId id) throws IOException, CatalogException {
        byte[] metaKey = tableMetaKey(id);
        Optional<byte[]> metaBytes = store.get(metaKey);
        if (metaBytes.isEmpty()) {
            throw new TableNotFoundException(
                    "Table not found: " + id.database() + "." + id.tableName(),
                    id.database() + "." + id.tableName());
        }
        try {
            PbTableMetadata pbMeta = PbTableMetadata.parseFrom(metaBytes.get());
            return ProtoTableMetaConverter.fromPb(pbMeta);
        } catch (Exception e) {
            throw new InvalidMetadataException(
                    "Failed to parse table metadata for " + id.database() + "." + id.tableName(),
                    id.database() + "." + id.tableName(),
                    e);
        }
    }

    /** Internal snapshot listing (IO method, not caching). */
    private List<Snapshot> listSnapshotsInternal(TableId id) throws IOException {
        byte[] snapPrefix = Keys.snapshotPrefix(id);
        List<KeyValue> kvs = store.scanAll(snapPrefix);
        return kvs.stream()
                .map(
                        kv -> {
                            try {
                                PbSnapshot pb = PbSnapshot.parseFrom(kv.value());
                                return ProtoTableMetaConverter.fromPbSnapshot(pb);
                            } catch (Exception e) {
                                log.warn(
                                        "Failed to parse snapshot for key: {}",
                                        HexFormat.of().formatHex(kv.key()),
                                        e);
                                return null;
                            }
                        })
                .filter(Objects::nonNull)
                .sorted(Comparator.comparingLong(Snapshot::snapshotId).reversed())
                .toList();
    }

    // ============ Metadata mutations ============

    @Override
    public void alterSchema(TableId id, Schema newSchema) throws CatalogException {
        ensureOpen();
        validateTableId(id);
        validateColumnAndCgNames(newSchema);

        try {
            byte[] metaKey = tableMetaKey(id);
            Optional<byte[]> currentBytes = store.get(metaKey);
            if (currentBytes.isEmpty()) {
                throw new TableNotFoundException("Table not found: " + id.database());
            }

            TableMeta current =
                    ProtoTableMetaConverter.fromPb(PbTableMetadata.parseFrom(currentBytes.get()));

            if (newSchema.version() <= current.schema().version()) {
                throw new SchemaIncompatibleException(
                        "Schema version must increase: current="
                                + current.schema().version()
                                + ", proposed="
                                + newSchema.version());
            }

            String strategyStr =
                    current.properties() != null
                            ? current.properties().getOrDefault("schema.compatibility", "BACKWARD")
                            : "BACKWARD";
            SchemaValidator.Compatibility strategy;
            try {
                strategy = SchemaValidator.Compatibility.valueOf(strategyStr.toUpperCase());
            } catch (IllegalArgumentException e) {
                log.warn(
                        "Unknown schema.compatibility value '{}', defaulting to BACKWARD",
                        strategyStr);
                strategy = SchemaValidator.Compatibility.BACKWARD;
            }

            SchemaValidator.validate(current.schema(), newSchema, strategy);

            TableMeta.Builder builder =
                    TableMeta.builder(current).withSchema(newSchema).withUpdatedAt(Instant.now());
            newSchema
                    .primaryKeyConstraint()
                    .ifPresent(
                            pkConstraint ->
                                    builder.withPrimaryKey(
                                            new PrimaryKey(
                                                    new ArrayList<>(pkConstraint.fieldNames()))));
            TableMeta updated = builder.build();

            PbTableMetadata pbMeta = ProtoTableMetaConverter.toPb(updated);

            PbSchema pbSchema = ProtoTableMetaConverter.toPbSchema(newSchema);
            byte[] schemaBytes = pbSchema.toByteArray();

            byte[] latestKey = Keys.snapshotLatestKey(id);
            long currentSnapshotId =
                    store.get(latestKey).map(MetastoreCatalog::bytesToLong).orElse(0L);

            String schemaDigest = computeDigest(schemaBytes);
            String operationType = classifySchemaChange(current.schema(), newSchema);
            String fieldIdSetDigest = computeFieldIdSetDigest(current.schema(), newSchema);

            PbSchemaHistoryEntry historyEntry =
                    PbSchemaHistoryEntry.newBuilder()
                            .setSchemaBytes(com.google.protobuf.ByteString.copyFrom(schemaBytes))
                            .setVersion(newSchema.version())
                            .setParentVersion(current.schema().version())
                            .setSchemaDigest(schemaDigest)
                            .setSnapshotId(currentSnapshotId)
                            .setCreatedAtMillis(System.currentTimeMillis())
                            .setOperationType(operationType)
                            .setFieldIdSetDigest(fieldIdSetDigest)
                            .build();

            byte[] schemaKey = Keys.schemaVersionKey(id, newSchema.version());

            boolean success =
                    store.compareAndSwap(
                            List.of(
                                    new Precondition.ValueEquals(metaKey, currentBytes.get()),
                                    new Precondition.NotExists(schemaKey)),
                            List.of(
                                    new Mutation.Put(schemaKey, historyEntry.toByteArray()),
                                    new Mutation.Put(metaKey, pbMeta.toByteArray())));

            if (!success) {
                throw new CommitConflictException(
                        "Concurrent modification detected during alterSchema for "
                                + id.database()
                                + "."
                                + id.tableName());
            }

            invalidateCache(id);
            log.info(
                    "Altered schema for table: {}.{} (compatibility={}, operation={})",
                    id.database(),
                    id.tableName(),
                    strategy,
                    operationType);

        } catch (TableNotFoundException | CommitConflictException | SchemaIncompatibleException e) {
            throw e;
        } catch (IOException e) {
            throw new CatalogIOException(
                    "Failed to alter schema: " + id.database() + "." + id.tableName(),
                    "Metastore",
                    e);
        }
    }

    @Override
    public Optional<Schema> loadSchemaVersion(TableId id, long version) throws CatalogException {
        ensureOpen();
        validateTableId(id);
        try {
            byte[] key = Keys.schemaVersionKey(id, version);
            Optional<byte[]> bytes = store.get(key);
            if (bytes.isEmpty()) return Optional.empty();
            return Optional.of(parseSchemaVersionBytes(bytes.get(), id, version));
        } catch (IOException e) {
            throw new CatalogIOException(
                    "Failed to load schema version " + version, "Metastore", e);
        }
    }

    private Schema parseSchemaVersionBytes(byte[] bytes, TableId id, long version) {
        String location = id.database() + "." + id.tableName() + ":v" + version;
        try {
            PbSchemaHistoryEntry entry = PbSchemaHistoryEntry.parseFrom(bytes);
            if (!entry.hasVersion()) {
                throw new InvalidMetadataException(
                        "Schema history entry missing version field: key version=" + version,
                        location);
            }
            if (entry.getVersion() != version) {
                throw new InvalidMetadataException(
                        "Schema history entry version mismatch: key version="
                                + version
                                + ", entry version="
                                + entry.getVersion(),
                        location);
            }
            PbSchema pbSchema = PbSchema.parseFrom(entry.getSchemaBytes());
            return ProtoTableMetaConverter.fromPbSchema(pbSchema);
        } catch (InvalidMetadataException e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidMetadataException(
                    "Failed to parse schema history for " + location, location, e);
        }
    }

    @Override
    public void updateProperties(TableId id, TableProperties changes) throws CatalogException {
        ensureOpen();
        validateTableId(id);

        try {
            byte[] metaKey = tableMetaKey(id);
            Optional<byte[]> currentBytes = store.get(metaKey);
            if (currentBytes.isEmpty()) {
                throw new TableNotFoundException("Table not found: " + id.database());
            }

            TableMeta current =
                    ProtoTableMetaConverter.fromPb(PbTableMetadata.parseFrom(currentBytes.get()));

            Map<String, String> merged = new HashMap<>(current.properties());
            merged.putAll(changes.props());

            TableMeta updated =
                    TableMeta.builder(current)
                            .withProperties(merged)
                            .withUpdatedAt(Instant.now())
                            .build();

            PbTableMetadata pbMeta = ProtoTableMetaConverter.toPb(updated);

            boolean success =
                    store.compareAndSwap(
                            List.of(new Precondition.ValueEquals(metaKey, currentBytes.get())),
                            List.of(new Mutation.Put(metaKey, pbMeta.toByteArray())));

            if (!success) {
                throw new CommitConflictException(
                        "Concurrent modification detected during updateProperties for "
                                + id.database()
                                + "."
                                + id.tableName());
            }

            invalidateCache(id);
            log.info("Updated properties for table: {}.{}", id.database(), id.tableName());

        } catch (TableNotFoundException | CommitConflictException e) {
            throw e;
        } catch (IOException e) {
            throw new CatalogIOException(
                    "Failed to update properties: " + id.database() + "." + id.tableName(),
                    "Metastore",
                    e);
        }
    }

    // ============ Commit lifecycle ============

    @Override
    public CommitTransaction newCommit(TableId id) throws CatalogException {
        ensureOpen();
        validateTableId(id);

        try {
            // 1. Persistently allocate commit ID
            byte[] counterKey = Keys.commitIdCounterKey(id);
            long commitId = allocatePersistentId(counterKey);

            // 2. Read current latest snapshot (base for OCC)
            byte[] latestKey = Keys.snapshotLatestKey(id);
            Optional<byte[]> latestBytes = store.get(latestKey);
            long baseSnapshotId = latestBytes.map(MetastoreCatalog::bytesToLong).orElse(-1L);

            // 3. Write intent marker (idempotency guard + tableId binding for cross-lifecycle
            // safety)
            byte[] intentKey = Keys.commitIntentKey(id, commitId);
            byte[] tableIdKey = Keys.tableIdentityKey(id);
            long recordedTableId =
                    store.get(tableIdKey).map(MetastoreCatalog::bytesToLong).orElse(0L);
            CommitIntentRecord intent =
                    new CommitIntentRecord(
                            baseSnapshotId, System.currentTimeMillis(), recordedTableId);
            byte[] intentBytes = serializeIntent(intent);

            boolean intentWritten =
                    store.compareAndSwap(
                            List.of(new Precondition.NotExists(intentKey)),
                            List.of(new Mutation.Put(intentKey, intentBytes)));

            if (!intentWritten) {
                throw new CommitConflictException(
                        "Duplicate commit intent: commitId="
                                + commitId
                                + " for table "
                                + id.database()
                                + "."
                                + id.tableName());
            }

            log.debug(
                    "New commit started: table={}.{}, commitId={}, baseSnapshotId={}",
                    id.database(),
                    id.tableName(),
                    commitId,
                    baseSnapshotId);

            // 4. Return transaction handle
            return new CommitTransaction(commitId, id, baseSnapshotId, List.of());

        } catch (CatalogException e) {
            throw e;
        } catch (IOException e) {
            throw new CatalogIOException(
                    "Failed to start commit for " + id.database() + "." + id.tableName(),
                    "Metastore",
                    e);
        }
    }

    @Override
    public void commit(CommitTransaction txn) throws CatalogException {
        ensureOpen();
        Objects.requireNonNull(txn, "txn cannot be null");
        TableId id = txn.tableId();

        try {
            byte[] intentKey = Keys.commitIntentKey(id, txn.commitId());
            byte[] latestKey = Keys.snapshotLatestKey(id);
            byte[] baseBytes = longToBytes(txn.baseSnapshotId());

            // Allocate snapshot ID before CAS.
            // Trade-off: wastes one ID on CAS conflict, but conflicts are rare.
            long newSnapshotId = allocatePersistentId(Keys.snapshotIdCounterKey(id));

            // 2. Build snapshot from SNAPSHOT_UPDATE payload (if present), or fallback
            Optional<CommitTransaction.TableOperation> snapshotUpdateOp =
                    txn.operations().stream()
                            .filter(
                                    op ->
                                            OpType.SNAPSHOT_UPDATE
                                                    .wireName()
                                                    .equals(op.operationType()))
                            .findFirst();

            Snapshot snapshot;
            if (snapshotUpdateOp.isPresent() && snapshotUpdateOp.get().payload() != null) {
                try {
                    PbSnapshotUpdate update =
                            PbSnapshotUpdate.parseFrom(snapshotUpdateOp.get().payload());
                    String summary = update.getSummary().isEmpty() ? "commit" : update.getSummary();
                    List<String> manifestIds =
                            update.getManifestIdsList().isEmpty()
                                    ? null
                                    : update.getManifestIdsList();
                    snapshot =
                            Snapshot.of(
                                    newSnapshotId,
                                    manifestIds,
                                    System.currentTimeMillis(),
                                    summary,
                                    "commit",
                                    null);
                } catch (InvalidProtocolBufferException e) {
                    log.error(
                            "Failed to parse SNAPSHOT_UPDATE payload, falling back to basic snapshot",
                            e);
                    String operation =
                            txn.operations().isEmpty()
                                    ? "commit"
                                    : txn.operations().get(0).operationType();
                    snapshot = Snapshot.of(newSnapshotId, operation);
                }
            } else {
                String operation =
                        txn.operations().isEmpty()
                                ? "commit"
                                : txn.operations().get(0).operationType();
                snapshot = Snapshot.of(newSnapshotId, operation);
            }
            PbSnapshot pbSnapshot = ProtoTableMetaConverter.toPbSnapshot(snapshot);
            byte[] snapshotBytes = pbSnapshot.toByteArray();

            // 3. Build mutation list
            List<Mutation> mutations = new ArrayList<>();
            mutations.add(new Mutation.Put(latestKey, longToBytes(newSnapshotId)));
            mutations.add(new Mutation.Put(Keys.snapshotKey(id, newSnapshotId), snapshotBytes));

            // Process per-operation mutations
            // Parse MANIFEST_WRITE payload once — used for both key disambiguation AND index write
            int opIndex = 0;
            for (CommitTransaction.TableOperation op : txn.operations()) {
                if (op.payload() != null && op.payload().length > 0) {
                    String manifestId;
                    if (OpType.MANIFEST_WRITE.wireName().equals(op.operationType())) {
                        PbManifestContent content;
                        try {
                            content = PbManifestContent.parseFrom(op.payload());
                        } catch (IOException e) {
                            throw new CatalogIOException(
                                    "Failed to parse MANIFEST_WRITE payload for indexing",
                                    "Metastore",
                                    e);
                        }
                        manifestId = content.getManifestId();
                        byte[] idxKey = Keys.manifestIndexKey(id, manifestId);
                        byte[] commitIdBytes = longToBytes(txn.commitId());
                        mutations.add(new Mutation.Put(idxKey, commitIdBytes));
                    } else {
                        manifestId = "op-" + opIndex;
                    }

                    byte[] opKey =
                            Keys.operationPayloadKey(
                                    id, txn.commitId(), op.operationType(), manifestId);
                    mutations.add(new Mutation.Put(opKey, op.payload()));
                }
                opIndex++;
            }

            mutations.add(new Mutation.Delete(intentKey));

            // Write commit-result record for idempotent replay detection
            byte[] resultKey = Keys.commitResultKey(id, txn.commitId());
            CommitResultRecord resultRecord =
                    new CommitResultRecord(
                            newSnapshotId, txn.commitId(), System.currentTimeMillis());
            mutations.add(new Mutation.Put(resultKey, resultRecord.toBytes()));

            // 4. Execute OCC CAS
            // For first commit (baseSnapshotId == -1, no latestKey exists), use NotExists.
            // For subsequent commits, use ValueEquals to detect concurrent conflicts.
            // Always verify metaKey exists to prevent commits on dropped tables.
            // TableId-guard prevents cross-lifecycle ABA: numericTableId is globally unique
            // per table creation and never reused, so drop+recreate produces a different ID.
            byte[] metaKey = tableMetaKey(id);
            byte[] tableIdKey = Keys.tableIdentityKey(id);
            Optional<byte[]> intentBytes = store.get(intentKey);
            long recordedTableId =
                    intentBytes
                            .map(MetastoreCatalog::deserializeIntent)
                            .map(CommitIntentRecord::tableId)
                            .orElse(0L);
            byte[] tableIdBytes = longToBytes(recordedTableId);
            List<Precondition> preconditions;
            if (txn.baseSnapshotId() == -1L) {
                preconditions =
                        List.of(
                                new Precondition.NotExists(latestKey),
                                new Precondition.Exists(intentKey),
                                new Precondition.Exists(metaKey),
                                new Precondition.ValueEquals(tableIdKey, tableIdBytes));
            } else {
                preconditions =
                        List.of(
                                new Precondition.ValueEquals(latestKey, baseBytes),
                                new Precondition.Exists(intentKey),
                                new Precondition.Exists(metaKey),
                                new Precondition.ValueEquals(tableIdKey, tableIdBytes));
            }

            boolean success = store.compareAndSwap(preconditions, mutations);

            if (!success) {
                // Determine failure reason
                if (store.exists(intentKey)) {
                    // intent exists but precondition failed -> OCC conflict
                    throw new CommitConflictException(
                            "Commit conflict: base snapshot "
                                    + txn.baseSnapshotId()
                                    + " is stale for table "
                                    + id.database()
                                    + "."
                                    + id.tableName());
                } else {
                    // intent gone -> check commit-result for idempotent replay
                    Optional<byte[]> resultBytes = store.get(resultKey);
                    if (resultBytes.isPresent()) {
                        try {
                            CommitResultRecord result =
                                    CommitResultRecord.fromBytes(resultBytes.get());
                            if (result.commitId() == txn.commitId()) {
                                log.info(
                                        "Commit {} is idempotent: commit-result found for {}.{}",
                                        txn.commitId(),
                                        id.database(),
                                        id.tableName());
                                return;
                            }
                        } catch (IllegalArgumentException e) {
                            log.error(
                                    "Corrupt commit-result for commitId={}, treating as missing",
                                    txn.commitId(),
                                    e);
                        }
                    }
                    throw new CommitConflictException(
                            "Commit intent lost for commitId="
                                    + txn.commitId()
                                    + " on table "
                                    + id.database()
                                    + "."
                                    + id.tableName());
                }
            }

            invalidateCache(id);
            log.info(
                    "Committed: table={}.{}, commitId={}, snapshotId={}, baseSnapshotId={}",
                    id.database(),
                    id.tableName(),
                    txn.commitId(),
                    newSnapshotId,
                    txn.baseSnapshotId());

        } catch (CommitConflictException e) {
            throw e;
        } catch (IOException e) {
            throw new CatalogIOException(
                    "Commit failed for " + id.database() + "." + id.tableName(), "Metastore", e);
        }
    }

    @Override
    public void abort(CommitTransaction txn) throws CatalogException {
        ensureOpen();
        Objects.requireNonNull(txn, "txn cannot be null");
        TableId id = txn.tableId();

        try {
            byte[] intentKey = Keys.commitIntentKey(id, txn.commitId());
            store.delete(intentKey);
            log.info(
                    "Aborted commit: table={}.{}, commitId={}",
                    id.database(),
                    id.tableName(),
                    txn.commitId());
        } catch (IOException e) {
            throw new CatalogIOException(
                    "Failed to abort commit "
                            + txn.commitId()
                            + " for "
                            + id.database()
                            + "."
                            + id.tableName(),
                    "Metastore",
                    e);
        }
    }

    @Override
    public void commitWithRetry(CommitTransaction txn, int maxRetries, double backoffMultiplier)
            throws CatalogException {
        long delayMs = 100;
        for (int i = 0; i <= maxRetries; i++) {
            try {
                commit(txn);
                return;
            } catch (CommitConflictException e) {
                if (i == maxRetries) {
                    throw e;
                }
                log.warn(
                        "Commit conflict (attempt {}/{}): table={}.{}, commitId={}, retrying in {}ms",
                        i + 1,
                        maxRetries + 1,
                        txn.tableId().database(),
                        txn.tableId().tableName(),
                        txn.commitId(),
                        delayMs);
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new CommitConflictException("Commit retry interrupted", ie);
                }
                delayMs = (long) (delayMs * backoffMultiplier);

                // Refresh baseSnapshotId: CommitTransaction is immutable, so we must
                // re-read latest snapshot and construct a fresh transaction for retry
                try {
                    byte[] latestKey = Keys.snapshotLatestKey(txn.tableId());
                    Optional<byte[]> latestBytes = store.get(latestKey);
                    long freshBase = latestBytes.map(MetastoreCatalog::bytesToLong).orElse(-1L);
                    txn =
                            new CommitTransaction(
                                    txn.commitId(), txn.tableId(), freshBase, txn.operations());

                    // Also update the intent record's baseSnapshotId (preserve original tableId)
                    byte[] intentKey = Keys.commitIntentKey(txn.tableId(), txn.commitId());
                    long originalTableId =
                            store.get(intentKey)
                                    .map(MetastoreCatalog::deserializeIntent)
                                    .map(CommitIntentRecord::tableId)
                                    .orElse(0L);
                    CommitIntentRecord newIntent =
                            new CommitIntentRecord(
                                    freshBase, System.currentTimeMillis(), originalTableId);
                    store.put(intentKey, serializeIntent(newIntent));
                } catch (IOException ioException) {
                    throw new CatalogIOException(
                            "Failed to refresh base snapshot for retry: "
                                    + txn.tableId().database()
                                    + "."
                                    + txn.tableId().tableName(),
                            "Metastore",
                            ioException);
                }
            }
        }
    }

    @Override
    public void commitWithRetry(
            CommitTransaction txn,
            int maxRetries,
            double backoffMultiplier,
            OperationRebuilder onConflict)
            throws CatalogException {
        long delayMs = 100;
        for (int i = 0; i <= maxRetries; i++) {
            try {
                commit(txn);
                return;
            } catch (CommitConflictException e) {
                if (i == maxRetries) {
                    throw e;
                }
                log.warn(
                        "Commit conflict (attempt {}/{}): table={}.{}, commitId={}, retrying in {}ms",
                        i + 1,
                        maxRetries + 1,
                        txn.tableId().database(),
                        txn.tableId().tableName(),
                        txn.commitId(),
                        delayMs);
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new CommitConflictException("Commit retry interrupted", ie);
                }
                delayMs = (long) (delayMs * backoffMultiplier);

                try {
                    byte[] latestKey = Keys.snapshotLatestKey(txn.tableId());
                    Optional<byte[]> latestBytes = store.get(latestKey);
                    long freshBase = latestBytes.map(MetastoreCatalog::bytesToLong).orElse(-1L);

                    // Rebuild operations from fresh state via callback (IOException declared)
                    List<CommitTransaction.TableOperation> freshOps = onConflict.rebuild(freshBase);

                    txn = new CommitTransaction(txn.commitId(), txn.tableId(), freshBase, freshOps);

                    // Update the intent record's baseSnapshotId (preserve original tableId)
                    byte[] intentKey = Keys.commitIntentKey(txn.tableId(), txn.commitId());
                    long originalTableId =
                            store.get(intentKey)
                                    .map(MetastoreCatalog::deserializeIntent)
                                    .map(CommitIntentRecord::tableId)
                                    .orElse(0L);
                    CommitIntentRecord newIntent =
                            new CommitIntentRecord(
                                    freshBase, System.currentTimeMillis(), originalTableId);
                    store.put(intentKey, serializeIntent(newIntent));
                } catch (IOException ioException) {
                    throw new CatalogIOException(
                            "Failed to refresh base snapshot for retry: "
                                    + txn.tableId().database()
                                    + "."
                                    + txn.tableId().tableName(),
                            "Metastore",
                            ioException);
                }
            }
        }
    }

    // ============ Snapshot management ============

    @Override
    public List<Snapshot> listSnapshots(TableId id) throws CatalogException {
        ensureOpen();
        validateTableId(id);
        try {
            return listSnapshotsInternal(id);
        } catch (IOException e) {
            throw new CatalogIOException(
                    "Failed to list snapshots for " + id.database() + "." + id.tableName(),
                    "Metastore",
                    e);
        }
    }

    @Override
    public Optional<Snapshot> getSnapshot(TableId id, long snapshotId) throws CatalogException {
        ensureOpen();
        validateTableId(id);

        try {
            byte[] snapKey = Keys.snapshotKey(id, snapshotId);
            Optional<byte[]> snapBytes = store.get(snapKey);
            if (snapBytes.isEmpty()) {
                return Optional.empty();
            }
            PbSnapshot pb = PbSnapshot.parseFrom(snapBytes.get());
            return Optional.of(ProtoTableMetaConverter.fromPbSnapshot(pb));
        } catch (IOException e) {
            throw new CatalogIOException(
                    "Failed to get snapshot "
                            + snapshotId
                            + " for "
                            + id.database()
                            + "."
                            + id.tableName(),
                    "Metastore",
                    e);
        } catch (Exception e) {
            throw new InvalidMetadataException(
                    "Failed to parse snapshot "
                            + snapshotId
                            + " for "
                            + id.database()
                            + "."
                            + id.tableName(),
                    e);
        }
    }

    @Override
    public void expireSnapshots(TableId id, SnapshotRetention retention) throws CatalogException {
        ensureOpen();
        validateTableId(id);

        try {
            List<Snapshot> snapshots = listSnapshotsInternal(id);
            if (snapshots.size() <= retention.minSnapshotsToKeep()) {
                log.debug(
                        "No snapshots to expire for {}.{}: {} snapshots, minToKeep={}",
                        id.database(),
                        id.tableName(),
                        snapshots.size(),
                        retention.minSnapshotsToKeep());
                return;
            }

            // Snapshots are sorted by ID descending (newest first)
            // Keep the newest minSnapshotsToKeep
            Instant cutoff = Instant.now().minus(retention.maxAge());

            List<Snapshot> toExpire = new ArrayList<>();
            for (int i = retention.minSnapshotsToKeep(); i < snapshots.size(); i++) {
                Snapshot s = snapshots.get(i);
                if (s.timestampInstant().isBefore(cutoff)) {
                    toExpire.add(s);
                }
            }

            for (Snapshot s : toExpire) {
                byte[] snapKey = Keys.snapshotKey(id, s.snapshotId());
                store.delete(snapKey);

                if (s.hasManifestList()) {
                    for (String mid : s.manifestList()) {
                        byte[] idxKey = Keys.manifestIndexKey(id, mid);
                        try {
                            Optional<byte[]> commitIdBytes = store.get(idxKey);
                            if (commitIdBytes.isPresent()) {
                                long commitId = ByteBuffer.wrap(commitIdBytes.get()).getLong();
                                byte[] opKey =
                                        Keys.operationPayloadKey(
                                                id,
                                                commitId,
                                                OpType.MANIFEST_WRITE.wireName(),
                                                mid);
                                store.delete(opKey);
                            }
                        } catch (IOException e) {
                            log.warn(
                                    "Failed to clean manifest index for {} in {}.{}",
                                    mid,
                                    id.database(),
                                    id.tableName(),
                                    e);
                        }
                        store.delete(idxKey);
                    }
                }

                log.debug(
                        "Expired snapshot {} for {}.{}",
                        s.snapshotId(),
                        id.database(),
                        id.tableName());
            }

            if (!toExpire.isEmpty()) {
                log.info(
                        "Expired {} snapshots for {}.{} (kept {})",
                        toExpire.size(),
                        id.database(),
                        id.tableName(),
                        snapshots.size() - toExpire.size());
            }

        } catch (IOException e) {
            throw new CatalogIOException(
                    "Failed to expire snapshots for " + id.database() + "." + id.tableName(),
                    "Metastore",
                    e);
        }
    }

    // ============ Cache management ============

    @Override
    public void invalidateCache(TableId id) {
        tableCache.invalidate(id);
        log.debug("Invalidated cache for table: {}.{}", id.database(), id.tableName());
    }

    // ============ Secondary Index ============

    private static final String INDEXES_PROP_KEY = "rawwise.indexes";

    private static final com.fasterxml.jackson.databind.ObjectMapper INDEX_JSON =
            new com.fasterxml.jackson.databind.ObjectMapper();

    @Override
    public List<io.github.rawwise.model.record.IndexMeta> indexesOf(TableId id)
            throws CatalogException {
        ensureOpen();
        RawWiseTable table = loadTable(id);
        String jsonStr = table.meta().properties().get(INDEXES_PROP_KEY);
        if (jsonStr == null || jsonStr.isBlank()) return List.of();
        try {
            return INDEX_JSON.readValue(
                    jsonStr,
                    INDEX_JSON
                            .getTypeFactory()
                            .constructCollectionType(
                                    List.class, io.github.rawwise.model.record.IndexMeta.class));
        } catch (Exception e) {
            log.warn(
                    "Failed to deserialize indexes for table {}.{}: {}",
                    id.database(),
                    id.tableName(),
                    e.getMessage());
            return List.of();
        }
    }

    public static void storeIndexes(
            Map<String, String> properties, List<io.github.rawwise.model.record.IndexMeta> indexes)
            throws com.fasterxml.jackson.core.JsonProcessingException {
        if (indexes == null || indexes.isEmpty()) {
            properties.remove(INDEXES_PROP_KEY);
        } else {
            properties.put(INDEXES_PROP_KEY, INDEX_JSON.writeValueAsString(indexes));
        }
    }

    // ============ Lifecycle ============

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        log.info("Closing MetastoreCatalog: {}", name);

        // Stop GC executor
        gcExecutor.shutdown();
        try {
            if (!gcExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                gcExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            gcExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Invalidate cache
        tableCache.invalidateAll();

        // Close store
        try {
            store.close();
        } catch (IOException e) {
            log.error("Error closing Metastore for catalog {}", name, e);
        }

        log.info("MetastoreCatalog closed: {}", name);
    }

    // ============ Internal: Persistent ID allocation ============

    /**
     * Allocates a monotonically increasing ID via CAS spin. Prefers {@link
     * Metastore#incrementAndGet} when supported.
     */
    private long allocatePersistentId(byte[] counterKey) throws IOException {
        // Try native increment first
        try {
            return store.incrementAndGet(counterKey, 1);
        } catch (UnsupportedOperationException e) {
            // Fall through to CAS spin
        }

        // CAS spin fallback
        while (true) {
            Optional<byte[]> current = store.get(counterKey);
            long currentVal = current.map(MetastoreCatalog::bytesToLong).orElse(0L);
            long nextVal = currentVal + 1;
            byte[] nextBytes = longToBytes(nextVal);

            List<Precondition> preconditions;
            if (current.isPresent()) {
                preconditions = List.of(new Precondition.ValueEquals(counterKey, current.get()));
            } else {
                preconditions = List.of(new Precondition.NotExists(counterKey));
            }

            List<Mutation> mutations = List.of(new Mutation.Put(counterKey, nextBytes));

            if (store.compareAndSwap(preconditions, mutations)) {
                return nextVal;
            }
            // Spin on conflict
        }
    }

    /** Allocates a global table numeric ID. */
    private long allocateTableId() throws IOException {
        byte[] counterKey = ("global:counter:table-id:").getBytes(StandardCharsets.UTF_8);
        return allocatePersistentId(counterKey);
    }

    // ============ Internal: Orphan intent GC ============

    /**
     * Scans all tables and deletes commit-intent entries older than 10 minutes. Runs periodically
     * via {@link #gcExecutor}.
     */
    private void gcOrphanIntents() {
        if (closed.get()) {
            return;
        }
        if (!gcRunning.compareAndSet(false, true)) {
            log.warn("Previous GC still running, skipping this round");
            return;
        }
        try {
            long cutoffTime = System.currentTimeMillis() - INTENT_TIMEOUT_MS;
            int deletedCount = 0;

            // Scan all namespaces
            List<String> namespaces = listNamespaces();
            for (String namespace : namespaces) {
                List<TableId> tables = listTables(namespace);
                for (TableId tableId : tables) {
                    byte[] intentPrefix = Keys.commitIntentPrefix(tableId);
                    List<byte[]> toDelete = new ArrayList<>();
                    try (CloseableIterator<KeyValue> it = store.scan(intentPrefix)) {
                        while (it.hasNext()) {
                            KeyValue kv = it.next();
                            try {
                                CommitIntentRecord record = deserializeIntent(kv.value());
                                if (record.timestamp() < cutoffTime) {
                                    toDelete.add(kv.key());
                                    log.debug(
                                            "GC found orphan intent: key={}, age={}min",
                                            HexFormat.of().formatHex(kv.key()),
                                            (System.currentTimeMillis() - record.timestamp())
                                                    / 60000);
                                }
                            } catch (Exception e) {
                                log.warn(
                                        "GC deleting corrupt intent entry: {}",
                                        HexFormat.of().formatHex(kv.key()));
                                toDelete.add(kv.key());
                            }
                        }
                    }
                    if (!toDelete.isEmpty()) {
                        List<Mutation> batch = new ArrayList<>(toDelete.size());
                        for (byte[] key : toDelete) {
                            batch.add(new Mutation.Delete(key));
                        }
                        store.batchWrite(batch);
                        deletedCount += toDelete.size();
                    }
                }
            }

            if (deletedCount > 0) {
                log.info("Orphan intent GC completed: {} intents cleaned up", deletedCount);
            }
        } catch (Exception e) {
            log.error("Error during orphan intent GC", e);
        } finally {
            gcRunning.set(false);
        }
    }

    // ============ Internal: Key encoding helpers ============

    /**
     * Returns the KV key for table metadata. Uses table prefix (e.g. "mydb:mytable:") as the
     * metadata key.
     */
    private static byte[] tableMetaKey(TableId id) {
        return Keys.tablePrefix(id);
    }

    /** Returns the KV key for namespace entries. */
    private static byte[] namespaceKey(String namespace) {
        byte[] nsBytes = namespace.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[NAMESPACE_PREFIX.length + nsBytes.length];
        System.arraycopy(NAMESPACE_PREFIX, 0, key, 0, NAMESPACE_PREFIX.length);
        System.arraycopy(nsBytes, 0, key, NAMESPACE_PREFIX.length, nsBytes.length);
        return key;
    }

    // ============ Internal: Serialization ============

    private static byte[] longToBytes(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    private static long bytesToLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    /** Commit intent record for serialization into the intent key. */
    private record CommitIntentRecord(long baseSnapshotId, long timestamp, long tableId) {}

    /**
     * Persisted record for idempotent commit detection. Written atomically as part of the CAS batch
     * on successful commit.
     */
    private record CommitResultRecord(long snapshotId, long commitId, long timestampMillis) {
        byte[] toBytes() {
            ByteBuffer buf = ByteBuffer.allocate(24);
            buf.putLong(snapshotId);
            buf.putLong(commitId);
            buf.putLong(timestampMillis);
            return buf.array();
        }

        static CommitResultRecord fromBytes(byte[] bytes) {
            if (bytes == null || bytes.length < 24) {
                throw new IllegalArgumentException(
                        "Corrupt commit-result record: expected 24 bytes, got "
                                + (bytes == null ? 0 : bytes.length));
            }
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            return new CommitResultRecord(buf.getLong(), buf.getLong(), buf.getLong());
        }
    }

    private static byte[] serializeIntent(CommitIntentRecord intent) {
        byte[] data = new byte[24];
        ByteBuffer buf = ByteBuffer.wrap(data);
        buf.putLong(intent.baseSnapshotId());
        buf.putLong(intent.timestamp());
        buf.putLong(intent.tableId());
        return data;
    }

    private static CommitIntentRecord deserializeIntent(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        long baseSnapshotId = buf.getLong();
        long timestamp = buf.getLong();
        long tableId = data.length >= 24 ? buf.getLong() : 0L;
        return new CommitIntentRecord(baseSnapshotId, timestamp, tableId);
    }

    // ============ Internal: Helpers ============

    private String buildTableIdentifier(TableId id) {
        return name + "." + id.database() + "." + id.tableName();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Catalog is closed: " + name);
        }
    }

    private void validateTableId(TableId id) {
        if (id == null) {
            throw new IllegalArgumentException("TableId cannot be null");
        }
        if (id.database() == null || id.database().isBlank()) {
            throw new IllegalArgumentException("Database name cannot be null or blank");
        }
        if (id.tableName() == null || id.tableName().isBlank()) {
            throw new IllegalArgumentException("Table name cannot be null or blank");
        }
    }

    /**
     * Validates the {@code body.codec} property.
     *
     * <p>The body codec name must be registered in {@link
     * io.github.rawwise.engine.storage.codec.BodyCodecRegistry}. This validation is applied only
     * when the property is explicitly set; the default "fixed-offset" is always valid.
     */
    private static void validateBodyCodecProperty(TableProperties properties) {
        if (properties == null) return;
        String bodyCodec = properties.props().get("body.codec");
        if (bodyCodec == null || bodyCodec.isEmpty()) return;
        if (!BodyCodecRegistry.isRegistered(bodyCodec)) {
            throw new IllegalArgumentException(
                    "Unknown body.codec: '" + bodyCodec + "'. " + "Supported: fixed-offset, tlv");
        }
    }

    /**
     * Validates that column names and ColumnGroup names do not contain reserved delimiter
     * characters used by the serialization format.
     *
     * <p>Reserved characters: {@code ':'}, {@code ','}, {@code ';'}. These are used in the compact
     * CG serialization format for Parquet footer metadata.
     */
    private static void validateColumnAndCgNames(Schema schema) {
        if (schema == null) return;

        for (var field : schema.fields()) {
            String name = field.name();
            if (name.indexOf(':') >= 0 || name.indexOf(',') >= 0 || name.indexOf(';') >= 0) {
                throw new IllegalArgumentException(
                        "Column name '" + name + "' contains reserved characters: ':', ',', ';'");
            }
        }

        for (var cg : schema.columnGroups()) {
            String cgName = cg.name();
            if (cgName.indexOf(':') >= 0 || cgName.indexOf(',') >= 0 || cgName.indexOf(';') >= 0) {
                throw new IllegalArgumentException(
                        "ColumnGroup name '"
                                + cgName
                                + "' contains reserved characters: ':', ',', ';'");
            }
        }
    }

    /**
     * Determines the table type from properties. Defaults to PRIMARY_KEY if 'primary-key' is set;
     * otherwise APPEND_ONLY.
     */
    private static TableMeta.TableType determineTableType(TableProperties properties) {
        String typeStr = properties.props().get("table.type");
        if (typeStr != null) {
            return switch (typeStr.toLowerCase()) {
                case "primary_key", "primary-key" -> TableMeta.TableType.PRIMARY_KEY;
                case "append_only", "append-only" -> TableMeta.TableType.APPEND_ONLY;
                default -> TableMeta.TableType.PRIMARY_KEY;
            };
        }
        // Default: PRIMARY_KEY if primary-key fields are specified
        String pkFields = properties.props().get("primary-key");
        if (pkFields != null && !pkFields.isBlank()) {
            return TableMeta.TableType.PRIMARY_KEY;
        }
        return TableMeta.TableType.APPEND_ONLY;
    }

    /** Resolves primary key from schema or properties. */
    private static PrimaryKey resolvePrimaryKey(
            Schema schema, TableMeta.TableType tableType, TableProperties properties) {
        if (tableType == TableMeta.TableType.PRIMARY_KEY) {
            var pkConstraints =
                    schema.constraints().stream()
                            .filter(c -> c instanceof PrimaryKeyConstraint)
                            .map(PrimaryKeyConstraint.class::cast)
                            .toList();
            if (!pkConstraints.isEmpty()) {
                return new PrimaryKey(new ArrayList<>(pkConstraints.get(0).fieldNames()));
            }
            String pkFields = properties.props().get("primary-key");
            if (pkFields != null && !pkFields.isBlank()) {
                return new PrimaryKey(new ArrayList<>(List.of(pkFields.split(","))));
            }
        }
        return PrimaryKey.none();
    }

    // RENAME_FIELD is currently unreachable via alterSchema (SchemaValidator rejects renames);
    // retained for future rename-aware compatibility modes.
    static String classifySchemaChange(Schema current, Schema proposed) {
        Map<Integer, StructField> currentById = SchemaValidator.buildFieldIdIndex(current);
        Map<Integer, StructField> proposedById = SchemaValidator.buildFieldIdIndex(proposed);
        Set<Integer> currentIds = currentById.keySet();
        Set<Integer> proposedIds = proposedById.keySet();
        boolean hasAdd;
        boolean hasDrop;
        boolean hasRename = false;
        boolean hasWiden = false;

        if (!currentIds.isEmpty()) {
            hasAdd = !currentIds.containsAll(proposedIds);
            hasDrop = !proposedIds.containsAll(currentIds);
            for (int fid : currentIds) {
                StructField cf = currentById.get(fid);
                StructField pf = proposedById.get(fid);
                if (cf != null && pf != null) {
                    if (!cf.name().equals(pf.name())) hasRename = true;
                    if (!cf.dataType().name().equals(pf.dataType().name())) hasWiden = true;
                    if (cf.dataType().isNullable() != pf.dataType().isNullable()) hasWiden = true;
                }
            }
            Map<String, StructField> currentUnassigned = new LinkedHashMap<>();
            Map<String, StructField> proposedUnassigned = new LinkedHashMap<>();
            for (StructField f : current.fields()) {
                if (!f.hasAssignedFieldId()) currentUnassigned.put(f.name(), f);
            }
            for (StructField f : proposed.fields()) {
                if (!f.hasAssignedFieldId()) proposedUnassigned.put(f.name(), f);
            }
            if (!currentUnassigned.keySet().containsAll(proposedUnassigned.keySet())) hasAdd = true;
            if (!proposedUnassigned.keySet().containsAll(currentUnassigned.keySet()))
                hasDrop = true;
            for (String name : currentUnassigned.keySet()) {
                StructField cu = currentUnassigned.get(name);
                StructField pu = proposedUnassigned.get(name);
                if (cu != null && pu != null) {
                    if (!cu.dataType().name().equals(pu.dataType().name())) hasWiden = true;
                    if (cu.dataType().isNullable() != pu.dataType().isNullable()) hasWiden = true;
                }
            }
        } else {
            List<StructField> cf = current.fields(), pf = proposed.fields();
            int minSize = Math.min(cf.size(), pf.size());
            for (int i = 0; i < minSize; i++) {
                if (!cf.get(i).name().equals(pf.get(i).name())) hasRename = true;
                if (!cf.get(i).dataType().name().equals(pf.get(i).dataType().name()))
                    hasWiden = true;
                if (cf.get(i).dataType().isNullable() != pf.get(i).dataType().isNullable())
                    hasWiden = true;
            }
            hasAdd = pf.size() > cf.size();
            hasDrop = cf.size() > pf.size();
        }

        int count = (hasAdd ? 1 : 0) + (hasDrop ? 1 : 0) + (hasRename ? 1 : 0) + (hasWiden ? 1 : 0);
        if (count > 1) return "MIXED";
        if (hasAdd) return "ADD_FIELD";
        if (hasDrop) return "DROP_FIELD";
        if (hasRename) return "RENAME_FIELD";
        if (hasWiden) return "WIDEN_TYPE";
        return "NO_CHANGE";
    }

    private static String computeFieldIdSetDigest(Schema current, Schema proposed) {
        Set<Integer> allIds = new TreeSet<>();
        for (StructField f : current.fields()) {
            if (f.hasAssignedFieldId()) allIds.add(f.fieldId());
        }
        for (StructField f : proposed.fields()) {
            if (f.hasAssignedFieldId()) allIds.add(f.fieldId());
        }
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            for (int id : allIds) {
                digest.update(java.nio.ByteBuffer.allocate(4).putInt(id).array());
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private static String computeDigest(byte[] data) {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(data));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
