package io.github.rawwise.engine.storage;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import io.github.rawwise.engine.storage.table.TableStore;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages the lifecycle of LMDB instances with memory-aware allocation, double-check locking for
 * safe concurrent creation, and flush coordination.
 *
 * <p>Key design decisions:
 *
 * <ul>
 *   <li>FlushTrigger interface decouples flush initiation from this pool
 *   <li>Constructor injection of tableStoreProvider avoids method-level coupling
 *   <li>Caffeine maximumSize=Long.MAX_VALUE: actual eviction driven by MemoryBudget + eviction
 *       listener
 *   <li>volatile started: protects eviction listener from firing before initialization
 *   <li>Double-check pattern: memory check outside lock (optimization) + re-check inside lock
 *       (safety net)
 *   <li>ConcurrentHashMap instanceToTable: O(1) reverse index for findTableIdentifier
 *   <li>Set flushingInstances: prevents concurrent duplicate flush of same instance
 *   <li>Two-phase init: constructor creates components, start() activates eviction listener +
 *       monitor
 * </ul>
 */
@Slf4j
public class InstancePool {

    /**
     * Interface for triggering flush operations. Implemented by DynamicMmapStore to decouple the
     * flush initiation from the pool's memory management logic.
     */
    public interface FlushTrigger {
        /** Trigger an asynchronous flush of the coldest instance. */
        void triggerFlushAsync();

        /**
         * Trigger a synchronous flush of the coldest instance.
         *
         * @return true if a flush was actually performed, false otherwise
         */
        boolean triggerFlushSync();
    }

    private static final long ALLOCATE_WAIT_MS = 100;
    private static final long ALLOCATE_TIMEOUT_MS = 30_000;

    private final MemoryBudget memoryBudget;
    private final ReentrantLock globalLock;
    private final Condition instanceFreed;
    private final ConcurrentHashMap<String, ReentrantLock> tableLocks;
    private final AsyncLoadingCache<String, String> instances;
    private final ConcurrentHashMap<String, String> instanceToTable = new ConcurrentHashMap<>();
    private final Set<String> flushingInstances = ConcurrentHashMap.newKeySet();
    private final Function<String, TableStore> tableStoreProvider;
    private final FlushTrigger flushTrigger;
    private final String basePath;
    private final long instanceMappedBytes;
    private final int maxDbs;
    private final ContainerMemoryGuard containerMemoryGuard;
    private volatile boolean started = false;

    public InstancePool(
            MemoryBudget memoryBudget,
            FlushTrigger flushTrigger,
            Function<String, TableStore> tableStoreProvider,
            String basePath,
            int writableNum,
            long instanceMappedBytes,
            int maxDbs,
            ContainerMemoryGuard containerMemoryGuard) {
        this.memoryBudget = memoryBudget;
        this.flushTrigger = flushTrigger;
        this.tableStoreProvider = tableStoreProvider;
        this.basePath = basePath;
        this.instanceMappedBytes = instanceMappedBytes;
        this.maxDbs = maxDbs;
        this.containerMemoryGuard = containerMemoryGuard;
        this.globalLock = new ReentrantLock();
        this.instanceFreed = globalLock.newCondition();
        this.tableLocks = new ConcurrentHashMap<>();

        this.instances =
                Caffeine.newBuilder()
                        .maximumSize(Long.MAX_VALUE)
                        .removalListener(
                                (key, value, cause) -> {
                                    if (!started) {
                                        log.debug(
                                                "InstancePool not started, ignoring removal: {}",
                                                key);
                                        return;
                                    }
                                    if (cause == RemovalCause.SIZE
                                            || cause == RemovalCause.EXPLICIT) {
                                        log.debug(
                                                "Instance removed from cache: {}, cause: {}",
                                                key,
                                                cause);
                                    }
                                })
                        .buildAsync(key -> key);
    }

    /**
     * Two-phase initialization: activates eviction listener and memory monitor. Must be called
     * after constructor, before any write operations.
     *
     * @param monitorService the scheduled executor for periodic memory monitoring; if null, no
     *     periodic monitoring is scheduled
     */
    public void start(ScheduledExecutorService monitorService) {
        this.started = true;
        if (monitorService != null) {
            monitorService.scheduleAtFixedRate(this::monitorMemory, 5, 5, TimeUnit.MINUTES);
        }
        log.info("InstancePool started");
    }

    /** Stop the pool, disabling eviction listener. */
    public void stop() {
        this.started = false;
        log.info("InstancePool stopped");
    }

    /**
     * @return the memory budget used by this pool
     */
    public MemoryBudget memoryBudget() {
        return memoryBudget;
    }

    /**
     * Get the current writable LMDB instance for the given table. If none exists, allocate a new
     * one (blocking until memory is available).
     *
     * @param tableIdentifier the table identifier
     * @return the current writable instance, or null if interrupted
     */
    public LmdbInstance currentLmdbInstance(String tableIdentifier) {
        TableStore store = tableStoreProvider.apply(tableIdentifier);
        LmdbInstance instance = store.getCurrentWritableInstance();
        if (instance == null) {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ALLOCATE_TIMEOUT_MS);
            while (!tryAllocateNewWritableInstance(tableIdentifier)) {
                if (!awaitInstanceFreed(deadline, tableIdentifier)) {
                    log.error(
                            "Timeout: could not allocate instance for table {} after {}ms",
                            tableIdentifier,
                            ALLOCATE_TIMEOUT_MS);
                    return null;
                }
            }
            instance = store.getCurrentWritableInstance();
        }
        if (instance != null) {
            touchInstance(instance.instanceId());
        }
        return instance;
    }

    /**
     * Get a writable LMDB instance for the given table based on key-based routing. Uses the
     * KeyRoutingTable's consistent hash ring to route the key to a specific instance. Falls back to
     * currentLmdbInstance() if the routing table is empty or routing fails.
     *
     * @param tableIdentifier the table identifier
     * @param key the write key for routing
     * @return the routed writable instance, or null if interrupted
     */
    public LmdbInstance keyBasedLmdbInstance(String tableIdentifier, byte[] key) {
        if (key == null) {
            return currentLmdbInstance(tableIdentifier);
        }
        TableStore store = tableStoreProvider.apply(tableIdentifier);
        LmdbInstance instance = store.getWritableInstance(key);
        if (instance == null) {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ALLOCATE_TIMEOUT_MS);
            while (!tryAllocateNewWritableInstance(tableIdentifier)) {
                if (!awaitInstanceFreed(deadline, tableIdentifier)) {
                    log.error(
                            "Timeout: could not allocate instance for table {} after {}ms",
                            tableIdentifier,
                            ALLOCATE_TIMEOUT_MS);
                    return null;
                }
            }
            instance = store.getWritableInstance(key);
        }
        if (instance != null) {
            touchInstance(instance.instanceId());
        }
        return instance;
    }

    /**
     * Try to allocate a new writable instance for the given table. Uses double-check pattern:
     * optimistic memory check outside lock, then defensive re-check inside lock for safety.
     *
     * @param tableIdentifier the table identifier
     * @return true if a new instance was successfully allocated
     */
    private ReentrantLock getTableLock(String tableIdentifier) {
        return tableLocks.computeIfAbsent(tableIdentifier, k -> new ReentrantLock());
    }

    private boolean awaitInstanceFreed(long deadline, String tableIdentifier) {
        long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
        if (remainingMs <= 0) return false;
        globalLock.lock();
        try {
            flushTrigger.triggerFlushAsync();
            instanceFreed.await(Math.min(remainingMs, ALLOCATE_WAIT_MS), TimeUnit.MILLISECONDS);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            globalLock.unlock();
        }
    }

    public void signalInstanceFreed() {
        globalLock.lock();
        try {
            instanceFreed.signalAll();
        } finally {
            globalLock.unlock();
        }
    }

    public boolean tryAllocateNewWritableInstance(String tableIdentifier) {
        if (!memoryBudget.canAllocateOneInstance()) {
            if (!flushTrigger.triggerFlushSync()) return false;
            if (!memoryBudget.canAllocateOneInstance()) return false;
        }
        if (containerMemoryGuard != null && containerMemoryGuard.isNativeMemoryLow()) {
            ContainerMemoryGuard.MemoryPressureLevel level =
                    containerMemoryGuard.getMemoryPressureLevel();
            if (level == ContainerMemoryGuard.MemoryPressureLevel.CRITICAL) {
                log.error(
                        "Container memory CRITICAL, rejecting allocation and triggering sync flush");
                flushTrigger.triggerFlushSync();
            } else {
                log.warn("Container memory HIGH, rejecting allocation and triggering async flush");
                flushTrigger.triggerFlushAsync();
            }
            return false;
        }
        ReentrantLock tableLock = getTableLock(tableIdentifier);
        globalLock.lock();
        try {
            tableLock.lock();
            try {
                if (!memoryBudget.canAllocateOneInstance()) return false;
                createNewInstance(tableIdentifier);
                return true;
            } finally {
                tableLock.unlock();
            }
        } finally {
            globalLock.unlock();
        }
    }

    /**
     * Switch to a new writable instance for the given table by rotating the current writable
     * instance to read-only, then creating a new one.
     *
     * @param tableIdentifier the table identifier
     * @return true if the switch was successful
     */
    public boolean switchToNewInstance(String tableIdentifier) {
        if (!memoryBudget.canAllocateOneInstance()) {
            flushTrigger.triggerFlushSync();
        }
        ReentrantLock tableLock = getTableLock(tableIdentifier);
        globalLock.lock();
        try {
            tableLock.lock();
            try {
                tableStoreProvider.apply(tableIdentifier).rotateWritableInstance();
                if (!memoryBudget.canAllocateOneInstance()) return false;
                createNewInstance(tableIdentifier);
                return true;
            } finally {
                tableLock.unlock();
            }
        } finally {
            globalLock.unlock();
        }
    }

    /**
     * Create a new LMDB instance for the given table and register it. Must be called under
     * globalLock or in a context where concurrent creation is not possible.
     *
     * @param tableIdentifier the table identifier
     */
    public void createNewInstance(String tableIdentifier) {
        String instanceId = LmdbInstance.generateInstanceId(tableIdentifier);
        LmdbInstance instance =
                new LmdbInstance(
                        instanceId, tablePath(tableIdentifier), instanceMappedBytes, maxDbs);
        tableStoreProvider.apply(tableIdentifier).addWritableInstance(instance);
        if (!memoryBudget.tryAllocate(instanceMappedBytes)) {
            throw new RuntimeException(
                    "Memory budget exceeded: cannot create new instance for " + tableIdentifier);
        }
        registerInstance(instanceId, tableIdentifier);
        log.info("Created new LMDB instance: {}", instanceId);
    }

    /**
     * Get an LMDB instance by its ID.
     *
     * @param instanceId the instance ID
     * @return the LMDB instance, or null if not found
     */
    public LmdbInstance getLmdbInstance(String instanceId) {
        String tableId = instanceToTable.get(instanceId);
        if (tableId != null) {
            return tableStoreProvider.apply(tableId).getInstance(instanceId);
        }
        return null;
    }

    /**
     * Find the table identifier for a given instance ID using O(1) reverse index.
     *
     * @param instanceId the instance ID
     * @return the table identifier, or null if not found
     */
    public String findTableIdentifier(String instanceId) {
        return instanceToTable.get(instanceId);
    }

    /**
     * Find the coldest (least recently used) instance across all instances. Uses Caffeine cache's
     * eviction policy to determine access order.
     *
     * @return Optional containing the instance ID of the coldest instance
     */
    public Optional<String> coldest() {
        var eviction = instances.synchronous().policy().eviction();
        if (eviction.isPresent()) {
            Map<String, String> coldest = eviction.get().coldest(1);
            if (!coldest.isEmpty()) {
                return Optional.of(coldest.keySet().iterator().next());
            }
        }
        return Optional.empty();
    }

    /**
     * Register an instance in the reverse index and Caffeine cache.
     *
     * @param instanceId the instance ID
     * @param tableIdentifier the table identifier
     */
    public void registerInstance(String instanceId, String tableIdentifier) {
        instanceToTable.put(instanceId, tableIdentifier);
        instances.put(instanceId, CompletableFuture.completedFuture(instanceId));
    }

    /**
     * Update the Caffeine cache access timestamp for an instance. Must be called whenever an
     * instance is accessed (read or write) so that cold() returns the actual least recently used
     * instance.
     */
    private void touchInstance(String instanceId) {
        instances.put(instanceId, CompletableFuture.completedFuture(instanceId));
    }

    /**
     * Ensure the routing owner lease is held for the given instance.
     *
     * @param instance the LMDB instance
     * @param ownerContext the routing owner context (may be null)
     * @return true if the lease is held or no owner context is required
     */
    public boolean ensureOwnerLease(LmdbInstance instance, RoutingOwnerContext ownerContext) {
        if (ownerContext == null) return true;
        return instance.ensureRoutingOwnerLease(ownerContext.executorId(), ownerContext.epoch());
    }

    /**
     * Claim exclusive flush rights for an instance. Prevents concurrent duplicate flush of the same
     * instance.
     *
     * @param instanceId the instance ID to claim
     * @return true if the claim was acquired (no other thread is flushing this instance)
     */
    public boolean claimFlush(String instanceId) {
        return flushingInstances.add(instanceId);
    }

    /**
     * Release flush claim for an instance. Must be called after flush completes (whether success or
     * failure) to allow future flushes.
     *
     * @param instanceId the instance ID to release
     */
    public void releaseFlushClaim(String instanceId) {
        flushingInstances.remove(instanceId);
    }

    private void monitorMemory() {
        if (!memoryBudget.canAllocateOneInstance()) {
            log.warn("Memory usage high, triggering async flush");
            flushTrigger.triggerFlushAsync();
        }
        if (containerMemoryGuard != null) {
            ContainerMemoryGuard.MemoryPressureLevel level =
                    containerMemoryGuard.getMemoryPressureLevel();
            switch (level) {
                case HIGH -> {
                    log.warn("Container memory pressure HIGH, triggering async flush");
                    flushTrigger.triggerFlushAsync();
                }
                case CRITICAL -> {
                    log.error("Container memory pressure CRITICAL, triggering sync flush");
                    flushTrigger.triggerFlushSync();
                }
                case UNKNOWN, LOW -> {}
            }
        }
    }

    private Path tablePath(String tableIdentifier) {
        return Paths.get(basePath).resolve(tableIdentifier);
    }

    public boolean allocatePendingInstanceBytes() {
        globalLock.lock();
        try {
            return memoryBudget.tryAllocate(instanceMappedBytes);
        } finally {
            globalLock.unlock();
        }
    }

    public void releasePendingInstanceBytes() {
        globalLock.lock();
        try {
            memoryBudget.release(instanceMappedBytes);
            instanceFreed.signalAll();
        } finally {
            globalLock.unlock();
        }
    }

    public String tablePathStr(String tableIdentifier) {
        return tablePath(tableIdentifier).toString();
    }

    public long instanceMappedBytes() {
        return instanceMappedBytes;
    }

    /** Returns all registered LMDB instances for sentinel scanning and diagnostics. */
    public java.util.Collection<LmdbInstance> getAllInstances() {
        return instanceToTable.keySet().stream()
                .map(this::getLmdbInstance)
                .filter(java.util.Objects::nonNull)
                .collect(java.util.stream.Collectors.toList());
    }
}
