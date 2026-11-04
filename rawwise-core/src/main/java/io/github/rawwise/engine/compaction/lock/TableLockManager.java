package io.github.rawwise.engine.compaction.lock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Unified per-table lock manager for Compaction and Clean operations.
 *
 * <p>Lock hierarchy: L1 (TableLockManager) -> L2 (Metastore commitLock). Always acquire
 * TableLockManager lock BEFORE any Metastore commit lock. Never reverse this order to prevent
 * deadlock.
 */
public final class TableLockManager {

    private static final TableLockManager INSTANCE = new TableLockManager();

    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

    private TableLockManager() {}

    public static TableLockManager getInstance() {
        return INSTANCE;
    }

    public ReentrantLock getLock(String tableId) {
        return locks.computeIfAbsent(tableId, k -> new ReentrantLock());
    }
}
