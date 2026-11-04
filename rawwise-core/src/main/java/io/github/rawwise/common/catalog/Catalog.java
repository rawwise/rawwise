package io.github.rawwise.common.catalog;

import io.github.rawwise.common.exception.CatalogException;
import io.github.rawwise.model.record.PartitionSpec;
import io.github.rawwise.model.record.RawWiseTable;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.Snapshot;
import io.github.rawwise.model.record.TableId;
import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lake catalog: domain interface for managing namespaces / tables / snapshots / commits.
 * Implementations: MetastoreCatalog (Metastore-based) / HadoopFileCatalog (filesystem-based) /
 * HmsCatalog / RestCatalog ...
 *
 * <p>All methods throw {@link CatalogException} and its subtypes to express business exceptions,
 * callers should not catch unchecked exceptions and rewrap them.
 */
public interface Catalog extends Closeable {

    /** Shared logger for default method implementations. */
    Logger LOG = LoggerFactory.getLogger(Catalog.class);

    // ============ Basic Info ============

    /** Returns the name of this Catalog implementation (e.g. "metastore", "hadoop-file"). */
    String name();

    // ============ Namespace (namespace / database) ============

    void createNamespace(String namespace) throws CatalogException;

    /** Drop namespace. Returns false if namespace is not empty. */
    boolean dropNamespace(String namespace) throws CatalogException;

    boolean namespaceExists(String namespace) throws CatalogException;

    List<String> listNamespaces() throws CatalogException;

    // ============ Table DDL ============

    RawWiseTable createTable(
            TableId id, Schema schema, PartitionSpec partitionSpec, TableProperties properties)
            throws CatalogException;

    /**
     * Drop table.
     *
     * @param purge when true, bypasses recycle bin and deletes data files directly
     */
    boolean dropTable(TableId id, boolean purge) throws CatalogException;

    /** Rename table. Returns false if target already exists. */
    boolean renameTable(TableId from, TableId to) throws CatalogException;

    boolean tableExists(TableId id) throws CatalogException;

    /** List all tables in the specified namespace. */
    List<TableId> listTables(String namespace) throws CatalogException;

    // ============ Table Loading (Read Path) ============

    RawWiseTable loadTable(TableId id) throws CatalogException;

    /** Load read-only table at the specified snapshot version. */
    RawWiseTable loadTable(TableId id, long snapshotId) throws CatalogException;

    // ============ Metadata Changes ============

    /**
     * Alter table Schema. Executes Schema compatibility check before apply. Throws {@link
     * io.github.rawwise.common.exception.SchemaIncompatibleException} when incompatible.
     */
    void alterSchema(TableId id, Schema newSchema) throws CatalogException;

    Optional<Schema> loadSchemaVersion(TableId id, long version) throws CatalogException;

    /**
     * Update table properties. Existing keys in changes will be overwritten, non-existent keys will
     * be added.
     */
    void updateProperties(TableId id, TableProperties changes) throws CatalogException;

    // ============ Snapshot / Transaction (Write Path) ============

    /**
     * Start a commit transaction, return transaction handle. commitId is persistently allocated and
     * cannot be rolled back.
     */
    CommitTransaction newCommit(TableId id) throws CatalogException;

    /**
     * Commit transaction. Throws CommitConflictException on conflict.
     *
     * @throws io.github.rawwise.common.exception.CommitConflictException if baseSnapshotId is stale
     *     (concurrent conflict)
     */
    void commit(CommitTransaction txn) throws CatalogException;

    /** Rollback transaction. */
    void abort(CommitTransaction txn) throws CatalogException;

    /**
     * Commit with retry. Automatically retries with exponential backoff on conflict.
     *
     * @param maxRetries Maximum retry count
     * @param backoffMultiplier Backoff multiplier (base 100ms)
     * @throws io.github.rawwise.common.exception.CommitConflictException if still conflicting after
     *     maximum retry count
     */
    void commitWithRetry(CommitTransaction txn, int maxRetries, double backoffMultiplier)
            throws CatalogException;

    /**
     * Commits with retry and operation rebuild callback. On OCC conflict, calls
     * onConflict.rebuild(freshBaseSnapshotId) to rebuild operations from the latest snapshot state
     * before retrying.
     *
     * <p><b>Warning:</b> This default implementation does NOT invoke {@code onConflict.rebuild}. It
     * silently delegates to the basic 3-arg {@link #commitWithRetry(CommitTransaction, int,
     * double)}. Catalog implementations that support operation rebuild MUST override this method.
     */
    default void commitWithRetry(
            CommitTransaction txn,
            int maxRetries,
            double backoffMultiplier,
            OperationRebuilder onConflict)
            throws CatalogException {
        LOG.warn(
                "Catalog {} does not implement callback-aware commitWithRetry; "
                        + "onConflict.rebuild() will NOT be called during retry",
                name());
        commitWithRetry(txn, maxRetries, backoffMultiplier);
    }

    // ============ Snapshot Management ============

    /** List all snapshots of a table, sorted by snapshotId descending. */
    List<Snapshot> listSnapshots(TableId id) throws CatalogException;

    /** Get snapshot by specified ID, returns Optional.empty if not found. */
    Optional<Snapshot> getSnapshot(TableId id, long snapshotId) throws CatalogException;

    /** Expire snapshots cleanup. */
    void expireSnapshots(TableId id, SnapshotRetention retention) throws CatalogException;

    // ============ Secondary Index ============

    /**
     * Return all secondary index metadata for the specified table. Returns empty list when no
     * indexes exist.
     */
    List<io.github.rawwise.model.record.IndexMeta> indexesOf(TableId id) throws CatalogException;

    // ============ Cache Management ============

    /** Invalidate metadata cache for the specified table. */
    void invalidateCache(TableId id);

    // ============ Lifecycle ============

    @Override
    void close();
}
