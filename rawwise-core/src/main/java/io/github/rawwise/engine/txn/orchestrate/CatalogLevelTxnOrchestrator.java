package io.github.rawwise.engine.txn.orchestrate;

import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.ShardVersionTracker;
import io.github.rawwise.engine.txn.state.TxnContext;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.Getter;

/**
 * Catalog level TxnOrchestrator(entry point interface for GlobalCommitMarker implementation).
 *
 * <p>this interface extends {@link TxnOrchestrator}, adds ability to parse owned resources by
 * tableId(LmdbInstance、 ShardVersionTracker)capability, laying foundation for subsequent B2 stage
 * multi-table commit pipeline.
 *
 * <h3>current stage(B1)</h3>
 *
 * <ul>
 *   <li>provides complete catalog-level begin / commit / abort interface(inherited from
 *       TxnOrchestrator)
 *   <li>exposes tableId → LmdbInstance / Tracker lookup, providesubsequent WriteStage /
 *       DurableStage do per-table dispatchuse
 *   <li>internal implementation still delegates to single delegate orchestrator(B2 stage will truly
 *       dispatch by tableId)
 * </ul>
 *
 * <h3>subsequentstage(B2 / B3)</h3>
 *
 * <ul>
 *   <li>B2: WriteStage dispatches by {@link #lmdbInstanceFor(String)} dispatch; DurableStage on
 *       each table's LmdbInstance each batchPutWithMarker; adds GlobalCommitMarker CAS as unique
 *       commit decision point
 *   <li>B3: cross-table crash recovery via Metastore GlobalCommitMarker decides replay or rollback
 * </ul>
 *
 * <p><b>Deployment boundary</b>: this interface implementation belongs to Backend Node process, by
 * Backend Server via gRPC betweenconnectcall, Frontend does not directly hold reference.
 */
public interface CatalogLevelTxnOrchestrator extends TxnOrchestrator {

    /** Catalog name. */
    String catalogName();

    /** Database name. */
    String databaseName();

    /** all tables currently registered in this catalog(tableId set). */
    Set<String> registeredTableIds();

    /**
     * parse the corresponding LMDB instance for tableId.
     *
     * @throws IllegalArgumentException if tableId not registered
     */
    LmdbInstance lmdbInstanceFor(String tableId);

    /**
     * parse the corresponding ShardVersionTracker for tableId. May return null(if per-key OCC not
     * enabled for this table).
     */
    ShardVersionTracker trackerFor(String tableId);

    /**
     * validates all tableIds declared in transaction ctx are registered in this catalog.
     *
     * @throws IllegalArgumentException if any tableId not registered
     */
    default void requireRegistered(TxnContext ctx) {
        Objects.requireNonNull(ctx, "ctx");
        for (String tableId : ctx.tableIds()) {
            if (!registeredTableIds().contains(tableId)) {
                throw new IllegalArgumentException(
                        "table "
                                + tableId
                                + " not registered in catalog "
                                + catalogName()
                                + "/"
                                + databaseName()
                                + "; registered="
                                + registeredTableIds());
            }
        }
    }

    /**
     * Catalog-level orchestrator registration entry.
     *
     * @param tableId complete table identifier(catalog.database.table)
     * @param lmdbInstance LMDB instance for this table
     * @param tracker per-key OCC tracker; may be null
     */
    @Getter
    final class TableResource {
        private final String tableId;
        private final LmdbInstance lmdbInstance;
        private final ShardVersionTracker tracker;

        public TableResource(
                String tableId, LmdbInstance lmdbInstance, ShardVersionTracker tracker) {
            this.tableId = Objects.requireNonNull(tableId, "tableId");
            this.lmdbInstance = Objects.requireNonNull(lmdbInstance, "lmdbInstance");
            this.tracker = tracker;
        }
    }

    /**
     * input parameters received by factory method.Map must be immutable or caller ensures no longer
     * modified.
     */
    record CatalogResources(
            String catalogName, String databaseName, Map<String, TableResource> tables) {

        public CatalogResources {
            if (catalogName == null || catalogName.isBlank())
                throw new IllegalArgumentException("catalogName must not be blank");
            if (databaseName == null || databaseName.isBlank())
                throw new IllegalArgumentException("databaseName must not be blank");
            if (tables == null || tables.isEmpty())
                throw new IllegalArgumentException("tables must not be empty");
            tables = Map.copyOf(tables);
        }
    }
}
