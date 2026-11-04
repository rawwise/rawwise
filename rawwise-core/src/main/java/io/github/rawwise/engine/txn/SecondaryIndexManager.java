package io.github.rawwise.engine.txn;

import io.github.rawwise.common.catalog.Catalog;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.model.record.IndexMeta;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * secondlevelindexlifecyclemanager.
 *
 * <p>responsibility:
 *
 * <ul>
 *   <li>{@link #registerHook(IndexMeta, TableStore, String)} — registerwritesync Hook
 *   <li>{@link #unregisterHook(String)} — remove Hook
 *   <li>{@link #getHook(String)} — query Hook(fortest / monitor)
 * </ul>
 *
 * <h3>integratedpoint</h3>
 *
 * <p>currently Hook in TableStore writemethodinbecall,
 * eachtimemaintablewriteaftersyncgenerateindexentry. After implementationmodifyasin {@code
 * CatalogLevelTxnOrchestrator.doCommit()} withinbatchexecuteindexwrite,
 * ensureandmaintablewriteatomicity.
 */
@Slf4j
public class SecondaryIndexManager {

    private final Catalog catalog;
    private final Map<String, SecondaryIndexWriteHook> hooks = new ConcurrentHashMap<>();

    public SecondaryIndexManager(Catalog catalog) {
        this.catalog = catalog;
    }

    /** asspecifiedindexregisterwritesync Hook to TableStore. */
    public void registerHook(IndexMeta indexMeta, TableStore mainTableStore, String mainTableId) {
        SecondaryIndexWriteHook hook = new SecondaryIndexWriteHook(indexMeta, mainTableId);
        hooks.put(indexMeta.indexName(), hook);
        // TODO: registerto TableStore postWriteHooks list
        log.info(
                "Registered SecondaryIndexWriteHook: index={} table={}",
                indexMeta.indexName(),
                mainTableId);
    }

    /** removeindex Hook. */
    public void unregisterHook(String indexName) {
        hooks.remove(indexName);
        log.info("Unregistered SecondaryIndexWriteHook: index={}", indexName);
    }

    /** getregistered Hook(fortest/monitor). */
    public SecondaryIndexWriteHook getHook(String indexName) {
        return hooks.get(indexName);
    }

    /** returnallregistered Hook. */
    public Collection<SecondaryIndexWriteHook> allHooks() {
        return Collections.unmodifiableCollection(hooks.values());
    }
}
