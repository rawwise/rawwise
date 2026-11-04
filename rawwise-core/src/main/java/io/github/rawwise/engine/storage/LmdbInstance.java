package io.github.rawwise.engine.storage;

import static org.lmdbjava.DbiFlags.MDB_CREATE;

import io.github.rawwise.common.collection.iterator.CloseableIterator;
import io.github.rawwise.engine.spi.FaultInjector;
import io.github.rawwise.engine.txn.RecordHeaderProbe;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.lmdbjava.*;

@Slf4j
public class LmdbInstance implements RecordHeaderProbe {
    public enum LeaseState {
        ACTIVE,
        RELEASED,
        READ_ONLY,
        MARKED_FOR_DELETION,
        CLOSED,
        DESTROYED
    }

    // Readonly marker file (permanent until destroy)
    public static final String LOCK_EXTENSION = ".lock";
    // Write lease file suffix (placed under table/base path, not inside instance dir)
    public static final String LEASE_EXTENSION = ".lease";
    // System property to override default lease TTL (ms)
    public static final String CONF_LEASE_TTL_MS = "rawwise.lmdb.lease.ttl.ms";
    // Default TTL 10s
    private static final long DEFAULT_LEASE_TTL_MS = 10_000L;
    public static final String RANGE_FILE = "range";
    private static final String DEFAULT_DB = "default";
    static final String META_DB_NAME = "__rawwise_meta__";
    private static final byte[] SENTINEL_KEY = "max_commit_ts".getBytes(StandardCharsets.UTF_8);

    // State machine: uniformly manages lifecycle
    private enum State {
        OPEN,
        READ_ONLY,
        MARKED_FOR_DELETION,
        CLOSED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.OPEN);

    @Getter private final Env<ByteBuffer> env;
    @Getter private final long maxBytes;
    private final String instanceId;
    private final Path instancePath;
    private final Path lockPath;
    private final Path leasePath; // <tablePath>/<instanceId>.lease
    private final Path rangePath;
    @Getter private final Dbi<ByteBuffer> dbi;

    private final Dbi<ByteBuffer> metaDbi;

    private final ReentrantLock stateLock = new ReentrantLock();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantLock writeTxnLock = new ReentrantLock();

    // Reference count: used to assist in determining whether safe deletion is possible
    private final AtomicInteger refCount = new AtomicInteger(0);

    // When markAsReadOnly() cannot acquire writeLock because the current thread holds readLock,
    // this flag is set. tryEnterScope() rejects new writers, avoiding deadlock
    // while ensuring the instance is no longer written to. Subsequent markAsReadOnly() retries will
    // complete the state transition.
    private final java.util.concurrent.atomic.AtomicBoolean pendingReadOnly =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    // Statistics - LongAdder for thread-safe concurrent updates
    private final LongAdder totalKeySize = new LongAdder();
    private final LongAdder totalValueSize = new LongAdder();
    private final LongAdder totalUsageSize = new LongAdder();
    private final LongAdder totalRecords = new LongAdder();
    @Getter private long createTime;

    public long getTotalKeySize() {
        return totalKeySize.sum();
    }

    public long getTotalValueSize() {
        return totalValueSize.sum();
    }

    public long getTotalUsageSize() {
        return totalUsageSize.sum();
    }

    public long getTotalRecords() {
        return totalRecords.sum();
    }

    public long maxBytes() {
        return maxBytes;
    }

    private byte[] minKey;
    private byte[] maxKey;

    private static final long UNASSIGNED_SEQ = Long.MAX_VALUE;
    private final java.util.concurrent.atomic.AtomicLong minSequenceNumber =
            new java.util.concurrent.atomic.AtomicLong(UNASSIGNED_SEQ);
    private final java.util.concurrent.atomic.AtomicLong maxSequenceNumber =
            new java.util.concurrent.atomic.AtomicLong(0L);

    public long minSequenceNumber() {
        long min = minSequenceNumber.get();
        return min == UNASSIGNED_SEQ ? 0L : min;
    }

    public long maxSequenceNumber() {
        return maxSequenceNumber.get();
    }

    public void recordSequenceNumber(long seq) {
        if (seq <= 0L) return;
        minSequenceNumber.updateAndGet(
                current -> current == UNASSIGNED_SEQ ? seq : Math.min(current, seq));
        maxSequenceNumber.updateAndGet(current -> Math.max(current, seq));
    }

    private static final int DEFAULT_KEY_BUFFER_SIZE = 256;
    private static final int DEFAULT_VALUE_BUFFER_SIZE = 1024;
    private static final int MAX_DIRECT_CAPACITY = 4 * 1024 * 1024;
    private static final int MAX_BUFFERS_PER_THREAD = 4;
    private static final double DEFAULT_USAGE_THRESHOLD = 0.70;

    private volatile double cachedUsageRatio = 0.0;
    private volatile long usageRatioLastUpdated = 0;
    private static final long USAGE_RATIO_REFRESH_INTERVAL_MS = 1000;
    private static final ThreadLocal<ByteBuffer> KEY_BUFFER_HOLDER =
            ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(DEFAULT_KEY_BUFFER_SIZE));
    private static final ThreadLocal<ByteBuffer> VALUE_BUFFER_HOLDER =
            ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(DEFAULT_VALUE_BUFFER_SIZE));
    private static final ThreadLocal<Long> CURRENT_THREAD_ID =
            ThreadLocal.withInitial(() -> Thread.currentThread().threadId());
    private static final ConcurrentHashMap<Long, List<ByteBuffer>> THREAD_BUFFERS =
            new ConcurrentHashMap<>();

    // Process identity used for lease ownership.
    private static final String PROCESS_OWNER_ID = buildProcessOwnerId();
    private final long leaseTtlMs;
    private volatile String activeLeaseOwnerId;
    private volatile boolean leaseOwner;

    private record LeaseOwnerState(boolean leaseOwner, String activeLeaseOwnerId) {
        static final LeaseOwnerState EMPTY = new LeaseOwnerState(false, null);
    }

    private final AtomicReference<LeaseOwnerState> leaseOwnerState =
            new AtomicReference<>(LeaseOwnerState.EMPTY);
    private volatile ScheduledFuture<?> leaseRenewTask;
    private final FaultInjector faultInjector;
    private static final ScheduledExecutorService LEASE_RENEWER =
            Executors.newScheduledThreadPool(
                    1,
                    new ThreadFactory() {
                        @Override
                        public Thread newThread(Runnable r) {
                            Thread t = new Thread(r, "Lmdb-Lease-Renewer");
                            t.setDaemon(true);
                            return t;
                        }
                    });

    public enum PutStatus {
        SUCCESS,

        MAP_FULL,

        READ_ONLY,

        ERROR
    }

    public LmdbInstance(String instanceId, Path basePath, long mappedSize) {
        this(instanceId, basePath, mappedSize, 2);
    }

    public LmdbInstance(String instanceId, Path basePath, long mappedSize, int maxDbs) {
        this(instanceId, basePath, mappedSize, maxDbs, null);
    }

    public LmdbInstance(
            String instanceId,
            Path basePath,
            long mappedSize,
            int maxDbs,
            FaultInjector faultInjector) {
        this.instanceId = instanceId;
        this.instancePath = basePath.resolve(instanceId);
        this.lockPath = instancePath.resolve(LOCK_EXTENSION);
        this.leasePath = basePath.resolve(instanceId + LEASE_EXTENSION);
        this.rangePath = instancePath.resolve(RANGE_FILE);
        this.maxBytes = mappedSize;
        this.leaseTtlMs = defaultLeaseTtlMs();
        this.faultInjector = faultInjector != null ? faultInjector : FaultInjector.NOOP;

        if (!Files.exists(instancePath)) {
            try {
                Files.createDirectories(instancePath);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create LMDB directory: " + instancePath, e);
            }
        } else {
            if (Files.exists(lockPath)) {
                this.state.set(State.READ_ONLY);
            }
        }

        Env.Builder<ByteBuffer> builder =
                Env.create().setMapSize(maxBytes).setMaxDbs(maxDbs).setMaxReaders(126);

        // MDB_MAPASYNC: txn.commit() only writes to mmap memory, no fsync, OS asynchronously
        // flushes.
        // Significantly reduces write p99 (eliminates 5-10ms fsync per commit), but OS crash may
        // lose unflushed data.
        // Requires WAL group-commit for fallback durability. Disabled by default, enable via
        // -Drawwise.lmdb.map-async=true.
        boolean mapAsync =
                "true".equalsIgnoreCase(System.getProperty("rawwise.lmdb.map-async", "false"));

        if (isReadOnly()) {
            this.env = builder.open(instancePath.toFile(), EnvFlags.MDB_RDONLY_ENV);
        } else if (mapAsync) {
            this.env = builder.open(instancePath.toFile(), EnvFlags.MDB_MAPASYNC);
            log.info(
                    "LMDB instance {} opened with MDB_MAPASYNC (async flush, no fsync on commit)",
                    instanceId);
        } else {
            this.env = builder.open(instancePath.toFile());
        }

        if (isReadOnly()) {
            loadKeyRange();
        }

        this.dbi = env.openDbi(DEFAULT_DB, MDB_CREATE);
        this.metaDbi = maxDbs >= 2 ? env.openDbi(META_DB_NAME, MDB_CREATE) : null;
        this.createTime = System.currentTimeMillis();
    }

    public Dbi<ByteBuffer> openDbi(String name) {
        return env.openDbi(name, MDB_CREATE);
    }

    public Dbi<ByteBuffer> defaultDbi() {
        return dbi;
    }

    public PutStatus put(byte[] key, byte[] value) {
        if (isReadOnly() || isMarkedForDeletion()) {
            return PutStatus.READ_ONLY;
        }

        rwLock.readLock().lock();
        boolean needsMarkReadOnly = false;
        try {
            if (isReadOnly() || isMarkedForDeletion()) {
                return PutStatus.READ_ONLY;
            }
            if (!isLeaseWritableByThisProcess()) {
                return PutStatus.READ_ONLY;
            }
            if (!hasSpaceForWrite(value.length)) {
                return PutStatus.MAP_FULL;
            }
            writeTxnLock.lock();
            try {
                try (Txn<ByteBuffer> txn = env.txnWrite()) {
                    ByteBuffer keyBuf = getKeyBuffer(key.length);
                    keyBuf.put(key).flip();
                    ByteBuffer valBuf = getValueBuffer(value.length);
                    valBuf.put(value).flip();

                    dbi.put(txn, keyBuf, valBuf);
                    txn.commit();
                    updateWriteStatistics(key.length, value.length);
                    return PutStatus.SUCCESS;
                }
            } catch (Env.MapFullException e) {
                log.warn(
                        "MapFullException on instance {}: maxBytes={}, currentUsage={}, keySize={}, valueSize={}",
                        instanceId,
                        maxBytes,
                        totalUsageSize.sum(),
                        key.length,
                        value.length);
                needsMarkReadOnly = true;
                return PutStatus.MAP_FULL;
            } finally {
                writeTxnLock.unlock();
            }
        } finally {
            rwLock.readLock().unlock();
            if (needsMarkReadOnly) {
                markAsReadOnly();
            }
        }
    }

    public BatchPutResult batchPut(List<byte[]> keys, List<byte[]> values) {
        if (isReadOnly() || isMarkedForDeletion()) {
            return new BatchPutResult(0, PutStatus.READ_ONLY);
        }
        if (keys.size() != values.size()) {
            throw new IllegalArgumentException("keys and values must have same size");
        }
        rwLock.readLock().lock();
        boolean needsMarkReadOnly = false;
        BatchPutResult result;
        try {
            if (isReadOnly() || isMarkedForDeletion()) {
                return new BatchPutResult(0, PutStatus.READ_ONLY);
            }
            if (!isLeaseWritableByThisProcess()) {
                return new BatchPutResult(0, PutStatus.READ_ONLY);
            }
            writeTxnLock.lock();
            try {
                int[] writtenHolder = {0};
                try (Txn<ByteBuffer> txn = env.txnWrite()) {
                    for (int i = 0; i < keys.size(); i++) {
                        byte[] key = keys.get(i);
                        byte[] value = values.get(i);
                        if (!hasSpaceForWrite(value.length)) {
                            needsMarkReadOnly = true;
                            result = new BatchPutResult(0, PutStatus.MAP_FULL);
                            return result;
                        }
                        ByteBuffer keyBuf = getKeyBuffer(key.length);
                        keyBuf.put(key).flip();
                        ByteBuffer valBuf = getValueBuffer(value.length);
                        valBuf.put(value).flip();
                        dbi.put(txn, keyBuf, valBuf);
                        writtenHolder[0]++;
                    }
                    txn.commit();
                    for (int i = 0; i < keys.size(); i++) {
                        updateWriteStatistics(keys.get(i).length, values.get(i).length);
                    }
                    return new BatchPutResult(writtenHolder[0], PutStatus.SUCCESS);
                }
            } catch (Env.MapFullException e) {
                needsMarkReadOnly = true;
                return new BatchPutResult(0, PutStatus.MAP_FULL);
            } finally {
                writeTxnLock.unlock();
            }
        } finally {
            rwLock.readLock().unlock();
            if (needsMarkReadOnly) {
                markAsReadOnly();
            }
        }
    }

    public BatchPutResult batchPutWithMarker(
            List<byte[]> keys, List<byte[]> values, byte[] markerKey, byte[] markerValue) {
        if (isReadOnly() || isMarkedForDeletion()) {
            return new BatchPutResult(0, PutStatus.READ_ONLY);
        }
        if (keys.size() != values.size()) {
            throw new IllegalArgumentException("keys and values must have same size");
        }
        rwLock.readLock().lock();
        boolean needsMarkReadOnly = false;
        BatchPutResult result;
        try {
            if (isReadOnly() || isMarkedForDeletion()) {
                return new BatchPutResult(0, PutStatus.READ_ONLY);
            }
            if (!isLeaseWritableByThisProcess()) {
                return new BatchPutResult(0, PutStatus.READ_ONLY);
            }
            writeTxnLock.lock();
            try {
                int[] writtenHolder = {0};
                try (Txn<ByteBuffer> txn = env.txnWrite()) {
                    for (int i = 0; i < keys.size(); i++) {
                        byte[] key = keys.get(i);
                        byte[] value = values.get(i);
                        if (!hasSpaceForWrite(value.length)) {
                            needsMarkReadOnly = true;
                            result = new BatchPutResult(0, PutStatus.MAP_FULL);
                            return result;
                        }
                        ByteBuffer keyBuf = getKeyBuffer(key.length);
                        keyBuf.put(key).flip();
                        ByteBuffer valBuf = getValueBuffer(value.length);
                        valBuf.put(value).flip();
                        dbi.put(txn, keyBuf, valBuf);
                        writtenHolder[0]++;
                    }
                    ByteBuffer mKeyBuf = getKeyBuffer(markerKey.length);
                    mKeyBuf.put(markerKey).flip();
                    ByteBuffer mValBuf = getValueBuffer(markerValue.length);
                    mValBuf.put(markerValue).flip();
                    dbi.put(txn, mKeyBuf, mValBuf);
                    faultInjector.inject(FaultInjector.InjectionPoint.DURING_LMDB_WRITE_TXN);
                    txn.commit();
                    for (int i = 0; i < keys.size(); i++) {
                        updateWriteStatistics(keys.get(i).length, values.get(i).length);
                    }
                    updateWriteStatistics(markerKey.length, markerValue.length);
                    return new BatchPutResult(writtenHolder[0], PutStatus.SUCCESS);
                }
            } catch (Env.MapFullException e) {
                needsMarkReadOnly = true;
                return new BatchPutResult(0, PutStatus.MAP_FULL);
            } finally {
                writeTxnLock.unlock();
            }
        } finally {
            rwLock.readLock().unlock();
            if (needsMarkReadOnly) {
                markAsReadOnly();
            }
        }
    }

    public BatchPutResult batchAppendPut(List<byte[]> keys, List<byte[]> values) {
        if (isReadOnly() || isMarkedForDeletion()) {
            return new BatchPutResult(0, PutStatus.READ_ONLY);
        }
        if (keys.size() != values.size()) {
            throw new IllegalArgumentException("keys and values must have same size");
        }
        rwLock.readLock().lock();
        boolean needsMarkReadOnly = false;
        BatchPutResult result;
        try {
            if (isReadOnly() || isMarkedForDeletion()) {
                return new BatchPutResult(0, PutStatus.READ_ONLY);
            }
            if (!isLeaseWritableByThisProcess()) {
                return new BatchPutResult(0, PutStatus.READ_ONLY);
            }
            writeTxnLock.lock();
            try {
                int[] writtenHolder = {0};
                try (Txn<ByteBuffer> txn = env.txnWrite()) {
                    for (int i = 0; i < keys.size(); i++) {
                        byte[] key = keys.get(i);
                        byte[] value = values.get(i);
                        if (!hasSpaceForWrite(value.length)) {
                            needsMarkReadOnly = true;
                            result = new BatchPutResult(0, PutStatus.MAP_FULL);
                            return result;
                        }
                        ByteBuffer keyBuf = getKeyBuffer(key.length);
                        keyBuf.put(key).flip();
                        ByteBuffer valBuf = getValueBuffer(value.length);
                        valBuf.put(value).flip();
                        dbi.put(txn, keyBuf, valBuf, PutFlags.MDB_APPEND);
                        writtenHolder[0]++;
                    }
                    txn.commit();
                    for (int i = 0; i < keys.size(); i++) {
                        updateWriteStatistics(keys.get(i).length, values.get(i).length);
                    }
                    return new BatchPutResult(writtenHolder[0], PutStatus.SUCCESS);
                }
            } catch (Env.MapFullException e) {
                needsMarkReadOnly = true;
                return new BatchPutResult(0, PutStatus.MAP_FULL);
            } finally {
                writeTxnLock.unlock();
            }
        } finally {
            rwLock.readLock().unlock();
            if (needsMarkReadOnly) {
                markAsReadOnly();
            }
        }
    }

    public record BatchPutResult(int writtenCount, PutStatus status) {
        public boolean isAllSuccess() {
            return status == PutStatus.SUCCESS;
        }
    }

    public record BatchDeleteResult(int deletedCount, int notFoundCount) {}

    public BatchDeleteResult batchDelete(List<byte[]> keys) {
        if (isReadOnly() || isMarkedForDeletion()) {
            return new BatchDeleteResult(0, keys.size());
        }
        rwLock.readLock().lock();
        try {
            if (isReadOnly() || isMarkedForDeletion()) {
                return new BatchDeleteResult(0, keys.size());
            }
            if (!isLeaseWritableByThisProcess()) {
                return new BatchDeleteResult(0, keys.size());
            }
            writeTxnLock.lock();
            try {
                int deletedCount = 0;
                int notFoundCount = 0;
                try (Txn<ByteBuffer> txn = env.txnWrite()) {
                    for (byte[] key : keys) {
                        ByteBuffer keyBuf = getKeyBuffer(key.length);
                        keyBuf.put(key).flip();
                        if (dbi.delete(txn, keyBuf)) {
                            deletedCount++;
                        } else {
                            notFoundCount++;
                        }
                    }
                    txn.commit();
                }
                return new BatchDeleteResult(deletedCount, notFoundCount);
            } finally {
                writeTxnLock.unlock();
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public byte[] get(byte[] key) {
        rwLock.readLock().lock();
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer keyBuf = getKeyBuffer(key.length);
            keyBuf.put(key).flip();

            ByteBuffer valBuf = dbi.get(txn, keyBuf);
            if (valBuf == null) return null;
            byte[] result = new byte[valBuf.remaining()];
            valBuf.get(result);
            return result;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public Iterator<Pair<byte[], byte[]>> iterator() {
        return closeableIterator();
    }

    public CloseableIterator<Pair<byte[], byte[]>> closeableIterator() {
        rwLock.readLock().lock();
        try {
            return new LockedIterator(new LmdbIterator(), rwLock.readLock());
        } catch (RuntimeException e) {
            rwLock.readLock().unlock();
            throw e;
        }
    }

    public Path path() {
        return this.instancePath;
    }

    public Path leasePath() {
        return this.leasePath;
    }

    public static long defaultLeaseTtlMs() {
        return defaultLeaseTtlMs(RawWiseConfig.empty());
    }

    public static long defaultLeaseTtlMs(RawWiseConfig config) {
        if (config != null) {
            Long configured = config.get(RawWiseOptions.LMDB_LEASE_TTL_MS);
            if (configured != null) {
                return configured;
            }
        }
        try {
            return Long.getLong(CONF_LEASE_TTL_MS, DEFAULT_LEASE_TTL_MS);
        } catch (Throwable ignored) {
            return DEFAULT_LEASE_TTL_MS;
        }
    }

    public boolean tryAcquireLease(String ownerId, long ttlMs) {
        long now = System.currentTimeMillis();
        long expireAt = now + Math.max(1L, ttlMs);
        try (FileChannel channel =
                FileChannel.open(
                        leasePath,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE)) {
            FileLock lock = tryLockLeaseFile(channel);
            if (lock == null) {
                return false;
            }
            try {
                LeaseInfo current = readLeaseFromChannel(channel);
                if (current != null && !current.allowsAcquire(now)) {
                    return false;
                }
                long nextVersion = current == null ? 1L : current.version() + 1L;
                writeLeaseToChannel(channel, ownerId, expireAt, nextVersion, LeaseState.ACTIVE);
                activeLeaseOwnerId = ownerId;
                return true;
            } finally {
                releaseQuietly(lock);
            }
        } catch (IOException e) {
            log.warn("tryAcquireLease failed for {}: {}", instanceId, e.toString());
            return false;
        }
    }

    public boolean renewLeaseIfOwner(String ownerId, long ttlMs) {
        long now = System.currentTimeMillis();
        long expireAt = now + Math.max(1L, ttlMs);
        if (!Files.exists(leasePath)) {
            return false;
        }
        try (FileChannel channel =
                FileChannel.open(leasePath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            FileLock lock = tryLockLeaseFile(channel);
            if (lock == null) {
                return false;
            }
            try {
                LeaseInfo current = readLeaseFromChannel(channel);
                if (current == null
                        || current.state() != LeaseState.ACTIVE
                        || !ownerId.equals(current.ownerId())) {
                    return false;
                }
                writeLeaseToChannel(
                        channel, ownerId, expireAt, current.version() + 1L, LeaseState.ACTIVE);
                activeLeaseOwnerId = ownerId;
                return true;
            } finally {
                releaseQuietly(lock);
            }
        } catch (IOException e) {
            log.warn("renewLeaseIfOwner failed for {}: {}", instanceId, e.toString());
            return false;
        }
    }

    public boolean releaseLeaseIfOwner(String ownerId) {
        try (FileChannel channel =
                FileChannel.open(
                        leasePath,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE)) {
            FileLock lock = tryLockLeaseFile(channel);
            if (lock == null) {
                return false;
            }
            try {
                LeaseInfo current = readLeaseFromChannel(channel);
                if (current == null || !ownerId.equals(current.ownerId())) {
                    return false;
                }
                writeLeaseToChannel(
                        channel, ownerId, 0L, current.version() + 1L, LeaseState.RELEASED);
            } finally {
                releaseQuietly(lock);
            }
            clearActiveLeaseOwner(ownerId);
            return true;
        } catch (IOException e) {
            log.warn("releaseLeaseIfOwner failed for {}: {}", instanceId, e.toString());
            return false;
        }
    }

    public LeaseInfo currentLease() {
        return readLeaseInternal();
    }

    public void markAsReadOnly() {
        stateLock.lock();
        try {
            if (state.get() != State.OPEN) {
                return;
            }
            // Use tryLock to avoid self-deadlock when upgrading read lock to write lock.
            // When caller holds rwLock.readLock() (e.g., put()'s finally block or
            // WritePathExecutor.execute() tryEnterScope() within scope),
            // writeLock.lock() will block forever, causing the entire service to hang.
            if (!rwLock.writeLock().tryLock()) {
                // Cannot acquire write lock (likely because current thread holds read lock).
                // Set pendingReadOnly flag, let tryEnterScope() reject new writers,
                // and let put()/batchPut() isReadOnly() check take effect via state comparison.
                pendingReadOnly.set(true);
                log.warn(
                        "Instance {} markAsReadOnly deferred (writeLock unavailable, "
                                + "likely readLock held by caller). Set pendingReadOnly=true. Caller: {}",
                        instanceId,
                        Thread.currentThread().getStackTrace()[2]);
                return;
            }
            try {
                if (state.get() != State.OPEN) {
                    return;
                }
                computeKeyRange();
                saveKeyRange();
                try {
                    Files.createFile(lockPath);
                } catch (FileAlreadyExistsException ignored) {
                }
                stopLeaseRenewal();
                updateLeaseState(LeaseState.READ_ONLY, activeLeaseOwnerId);
                transitionTo(State.READ_ONLY);
                pendingReadOnly.set(false);
            } finally {
                rwLock.writeLock().unlock();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to mark as read-only: " + instanceId, e);
        } finally {
            stateLock.unlock();
        }
    }

    public void unmarkReadOnly() {
        stateLock.lock();
        try {
            pendingReadOnly.set(false);
            if (state.get() != State.READ_ONLY) {
                return;
            }
            rwLock.writeLock().lock();
            try {
                if (state.get() != State.READ_ONLY) {
                    return;
                }
                updateLeaseState(LeaseState.ACTIVE, activeLeaseOwnerId);
                transitionTo(State.OPEN);
            } finally {
                rwLock.writeLock().unlock();
            }
        } finally {
            stateLock.unlock();
        }
    }

    public void close() {
        if (state.get() == State.CLOSED) {
            return;
        }
        stateLock.lock();
        try {
            rwLock.writeLock().lock();
            try {
                try {
                    stopLeaseRenewal();
                    updateLeaseState(LeaseState.CLOSED, leaseOwnerState.get().activeLeaseOwnerId());
                } catch (Exception e) {
                    log.warn("Failed to update lease state during close: {}", e.getMessage());
                }
            } finally {
                try {
                    closeUnsafe();
                } finally {
                    rwLock.writeLock().unlock();
                }
            }
        } finally {
            stateLock.unlock();
        }
    }

    private static final sun.misc.Unsafe UNSAFE;

    static {
        sun.misc.Unsafe unsafe;
        try {
            java.lang.reflect.Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (sun.misc.Unsafe) field.get(null);
        } catch (Exception e) {
            unsafe = null;
        }
        UNSAFE = unsafe;
    }

    private static void cleanDirectBuffer(ByteBuffer buffer) {
        if (UNSAFE == null) {
            return;
        }
        try {
            UNSAFE.invokeCleaner(buffer);
        } catch (Exception e) {
            log.warn("Failed to clean DirectByteBuffer: {}", e.getMessage());
        }
    }

    public static void cleanupThreadBuffers(long threadId) {
        List<ByteBuffer> buffers = THREAD_BUFFERS.remove(threadId);
        if (buffers != null) {
            for (ByteBuffer buf : buffers) {
                cleanDirectBuffer(buf);
            }
        }
    }

    public static void cleanupCurrentThreadDirectBuffers() {
        long threadId = Thread.currentThread().threadId();
        cleanupThreadBuffers(threadId);
        KEY_BUFFER_HOLDER.remove();
        VALUE_BUFFER_HOLDER.remove();
    }

    public boolean readOnly() {
        State current = state.get();
        return current == State.READ_ONLY || current == State.MARKED_FOR_DELETION;
    }

    public String instanceId() {
        return this.instanceId;
    }

    public byte[] minKey() {
        return minKey == null ? new byte[0] : Arrays.copyOf(minKey, minKey.length);
    }

    public byte[] maxKey() {
        return maxKey == null ? new byte[0] : Arrays.copyOf(maxKey, maxKey.length);
    }

    public void destroy() {
        stateLock.lock();
        try {
            rwLock.writeLock().lock();
            try {
                stopLeaseRenewal();
                updateLeaseState(LeaseState.DESTROYED, activeLeaseOwnerId);
                closeUnsafe();

                // 3. Inter-process mutual exclusion: acquire exclusive lock on .loc file
                // Note: FileChannel.open creates file if it does not exist
                try (FileChannel channel =
                        FileChannel.open(
                                lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {

                    FileLock fileLock = channel.tryLock();
                    if (fileLock == null) {
                        throw new IOException(
                                "Failed to acquire file lock, instance may be in use by another process: "
                                        + lockPath);
                    }

                    try {
                        // 4. safely delete files
                        try (Stream<Path> stream = Files.walk(instancePath)) {
                            stream.sorted(Comparator.reverseOrder())
                                    .forEach(
                                            path -> {
                                                try {
                                                    Files.deleteIfExists(path);
                                                } catch (IOException ignored) {
                                                }
                                            });
                        }
                    } finally {
                        fileLock.release();
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to destroy instance: " + instanceId, e);
            } finally {
                rwLock.writeLock().unlock();
            }
        } finally {
            stateLock.unlock();
        }
    }

    public boolean tryWriteLock() {
        return rwLock.writeLock().tryLock();
    }

    public void unlockWrite() {
        rwLock.writeLock().unlock();
    }

    public void markForDeletion() {
        stateLock.lock();
        try {
            State current = state.get();
            if (current == State.CLOSED || current == State.MARKED_FOR_DELETION) {
                return;
            }
            rwLock.writeLock().lock();
            try {
                current = state.get();
                if (current == State.CLOSED || current == State.MARKED_FOR_DELETION) {
                    return;
                }
                if (current == State.OPEN) {
                    computeKeyRange();
                    saveKeyRange();
                    try {
                        Files.createFile(lockPath);
                    } catch (FileAlreadyExistsException ignored) {
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to create lock file: " + lockPath, e);
                    }
                }
                stopLeaseRenewal();
                updateLeaseState(LeaseState.MARKED_FOR_DELETION, activeLeaseOwnerId);
                transitionTo(State.MARKED_FOR_DELETION);
            } finally {
                rwLock.writeLock().unlock();
            }
        } finally {
            stateLock.unlock();
        }
    }

    public boolean isMarkedForDeletion() {
        return state.get() == State.MARKED_FOR_DELETION;
    }

    public void unmarkForDeletion() {
        stateLock.lock();
        try {
            State current = state.get();
            if (current != State.MARKED_FOR_DELETION) {
                return;
            }
            rwLock.writeLock().lock();
            try {
                current = state.get();
                if (current != State.MARKED_FOR_DELETION) {
                    return;
                }
                if (Files.exists(lockPath)) {
                    try {
                        Files.delete(lockPath);
                    } catch (IOException e) {
                        log.warn(
                                "Failed to delete lock file during unmarkForDeletion: {}",
                                lockPath,
                                e);
                    }
                }
                transitionTo(State.READ_ONLY);
            } finally {
                rwLock.writeLock().unlock();
            }
        } finally {
            stateLock.unlock();
        }
    }

    public boolean isReadOnly() {
        return state.get() == State.READ_ONLY;
    }

    /**
     * Instance is lazily marked as read-only (markAsReadOnly delayed due to tryLock failure).
     * Instance state remains OPEN, but should no longer accept new writes. WritePathExecutor
     * triggers switchToNewInstance based on this.
     */
    public boolean isPendingReadOnly() {
        return pendingReadOnly.get();
    }

    public boolean tryEnterScope() {
        // pendingReadOnly indicates markAsReadOnly() is delayed due to deadlock prevention.
        // Reject new writers, ensure the instance is no longer written to.
        if (pendingReadOnly.get()) {
            return false;
        }
        rwLock.readLock().lock();
        refCount.incrementAndGet();
        if (isMarkedForDeletion() || pendingReadOnly.get()) {
            // If already marked for deletion or pendingReadOnly, fall back immediately
            refCount.decrementAndGet();
            rwLock.readLock().unlock();
            return false;
        }
        return true;
    }

    public void exitScope() {
        refCount.decrementAndGet();
        rwLock.readLock().unlock();
    }

    public int getRefCount() {
        return refCount.get();
    }

    public static String generateInstanceId(String tableIdentifier) {
        return "inst-"
                + tableIdentifier
                + "-"
                + System.currentTimeMillis()
                + "-"
                + java.util.UUID.randomUUID().toString().substring(0, 8);
    }

    private void computeKeyRange() {
        minKey = null;
        maxKey = null;
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            try (CursorIterable<ByteBuffer> cursor = dbi.iterate(txn)) {
                for (CursorIterable.KeyVal<ByteBuffer> kv : cursor) {
                    ByteBuffer keyBuffer = kv.key();
                    byte[] key = new byte[keyBuffer.remaining()];
                    keyBuffer.get(key);
                    if (minKey == null) {
                        minKey = key;
                    }
                    maxKey = key;
                }
                if (minKey == null) {
                    minKey = maxKey = new byte[0];
                }
            }
        }
    }

    private void saveKeyRange() {
        try (DataOutputStream dos = new DataOutputStream(Files.newOutputStream(rangePath))) {
            dos.writeInt(minKey.length);
            dos.write(minKey);
            dos.writeInt(maxKey.length);
            dos.write(maxKey);
        } catch (IOException e) {
            log.error("Failed to save key range for {}", instanceId, e);
        }
    }

    private void loadKeyRange() {
        if (!Files.exists(rangePath)) {
            return;
        }
        try (DataInputStream dis = new DataInputStream(Files.newInputStream(rangePath))) {
            int len = dis.readInt();
            minKey = new byte[len];
            dis.readFully(minKey);
            len = dis.readInt();
            maxKey = new byte[len];
            dis.readFully(maxKey);
        } catch (IOException e) {
            log.error("Failed to load key range for lmdb: {}, exception: {}", instanceId, e);
        }
    }

    private boolean hasSpaceForWrite(int dataSize) {
        long currentUsage = totalUsageSize.sum();
        // Add safety margin
        long projectedUsage = currentUsage + dataSize + 1024;
        // 90% threshold
        return projectedUsage < maxBytes * 0.9;
    }

    public double getUsageRatio() {
        long now = System.currentTimeMillis();
        if (now - usageRatioLastUpdated < USAGE_RATIO_REFRESH_INTERVAL_MS) {
            return cachedUsageRatio;
        }
        return refreshUsageRatio();
    }

    public double refreshUsageRatio() {
        if (state.get() == State.CLOSED) {
            return cachedUsageRatio;
        }
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            Stat stat = dbi.stat(txn);
            long usedPages = stat.branchPages + stat.leafPages + stat.overflowPages;
            long usedBytes = usedPages * stat.pageSize;
            double ratio = maxBytes > 0 ? (double) usedBytes / maxBytes : 0.0;
            cachedUsageRatio = ratio;
            usageRatioLastUpdated = System.currentTimeMillis();
            return ratio;
        } catch (Exception e) {
            log.warn("Failed to refresh usage ratio for {}: {}", instanceId, e.getMessage());
            return cachedUsageRatio;
        }
    }

    public boolean isApproachingFull(double threshold) {
        if (isReadOnly() || isMarkedForDeletion()) {
            return true;
        }
        return getUsageRatio() >= threshold;
    }

    public boolean isApproachingFull() {
        return isApproachingFull(DEFAULT_USAGE_THRESHOLD);
    }

    private void updateWriteStatistics(long curKeySize, long curValueSize) {
        long curEntrySize = curKeySize + curValueSize;
        totalRecords.increment();
        totalKeySize.add(curKeySize);
        totalValueSize.add(curValueSize);
        totalUsageSize.add(curEntrySize);
        usageRatioLastUpdated = 0;
    }

    private void closeUnsafe() {
        if (state.get() != State.CLOSED) {
            transitionTo(State.CLOSED);
        }
        if (this.dbi != null) {
            this.dbi.close();
        }
        if (env != null) {
            env.close();
        }
    }

    private void transitionTo(State target) {
        State current = state.get();
        if (current == target) {
            return;
        }
        if (!canTransition(current, target)) {
            throw new IllegalStateException(
                    "Illegal state transition: " + current + " -> " + target);
        }
        state.set(target);
    }

    private static boolean canTransition(State from, State to) {
        if (from == to) {
            return true;
        }
        return switch (from) {
            case OPEN ->
                    to == State.READ_ONLY || to == State.MARKED_FOR_DELETION || to == State.CLOSED;
            case READ_ONLY ->
                    to == State.OPEN || to == State.MARKED_FOR_DELETION || to == State.CLOSED;
            case MARKED_FOR_DELETION -> to == State.READ_ONLY || to == State.CLOSED;
            case CLOSED -> false;
        };
    }

    private static final class LockedIterator implements CloseableIterator<Pair<byte[], byte[]>> {
        private final CloseableIterator<Pair<byte[], byte[]>> delegate;
        private final ReentrantReadWriteLock.ReadLock lock;
        private boolean released;

        private LockedIterator(
                CloseableIterator<Pair<byte[], byte[]>> delegate,
                ReentrantReadWriteLock.ReadLock lock) {
            this.delegate = delegate;
            this.lock = lock;
        }

        @Override
        public boolean hasNext() {
            try {
                boolean hasNext = delegate.hasNext();
                if (!hasNext) {
                    release();
                }
                return hasNext;
            } catch (RuntimeException e) {
                release();
                throw e;
            }
        }

        @Override
        public Pair<byte[], byte[]> next() {
            try {
                return delegate.next();
            } catch (RuntimeException e) {
                release();
                throw e;
            }
        }

        @Override
        public void close() {
            release();
        }

        private void release() {
            if (!released) {
                released = true;
                try {
                    delegate.close();
                } catch (Exception ignored) {
                }
                lock.unlock();
            }
        }
    }

    private class LmdbIterator implements CloseableIterator<Pair<byte[], byte[]>> {
        private final CursorIterable<ByteBuffer> cursorIterable;
        private final Iterator<CursorIterable.KeyVal<ByteBuffer>> cursor;
        private final Txn<ByteBuffer> txn;
        private boolean closed;

        LmdbIterator() {
            this.txn = env.txnRead();
            this.cursorIterable = dbi.iterate(txn);
            this.cursor = cursorIterable.iterator();
        }

        @Override
        public boolean hasNext() {
            boolean has = cursor.hasNext();
            if (!has) {
                close();
            }
            return has;
        }

        @Override
        public Pair<byte[], byte[]> next() {
            CursorIterable.KeyVal<ByteBuffer> kv = cursor.next();
            ByteBuffer keyBuf = kv.key();
            ByteBuffer valBuf = kv.val();
            byte[] key = new byte[keyBuf.remaining()];
            keyBuf.get(key);
            byte[] value = new byte[valBuf.remaining()];
            valBuf.get(value);
            return Pair.of(key, value);
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            try {
                cursorIterable.close();
            } catch (Exception ignored) {
            }
            try {
                txn.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static ByteBuffer directBuffer(ThreadLocal<ByteBuffer> holder, int length) {
        if (length > MAX_DIRECT_CAPACITY) {
            return ByteBuffer.allocate(length);
        }
        // FIX: Removed pool eviction + cleanDirectBuffer logic.
        // The previous code evicted and cleaned (deallocated native memory of)
        // the smallest buffer when the pool reached MAX_BUFFERS_PER_THREAD.
        // This caused a use-after-free: if getKeyBuffer() allocated a buffer
        // that was the smallest in the pool, getValueBuffer() would free it
        // and potentially reuse the same native address, causing the value
        // to overwrite the key data. Each call allocates a fresh buffer,
        // so aliasing cannot occur. The pool is only for cleanup tracking.
        ByteBuffer buffer = ByteBuffer.allocateDirect(length);
        holder.set(buffer);
        long threadId = CURRENT_THREAD_ID.get();
        THREAD_BUFFERS
                .computeIfAbsent(threadId, k -> Collections.synchronizedList(new ArrayList<>()))
                .add(buffer);
        buffer.clear();
        buffer.limit(length);
        return buffer;
    }

    private static ByteBuffer getKeyBuffer(int length) {
        return directBuffer(getKeyBufferHolder(), length);
    }

    private static ByteBuffer getValueBuffer(int length) {
        return directBuffer(getValueBufferHolder(), length);
    }

    private static ThreadLocal<ByteBuffer> getKeyBufferHolder() {
        return KEY_BUFFER_HOLDER;
    }

    private static ThreadLocal<ByteBuffer> getValueBufferHolder() {
        return VALUE_BUFFER_HOLDER;
    }

    private LeaseInfo readLeaseInternal() {
        if (!Files.exists(leasePath)) {
            return null;
        }
        try (FileChannel channel = FileChannel.open(leasePath, StandardOpenOption.READ)) {
            return readLeaseFromChannel(channel);
        } catch (IOException e) {
            return null;
        }
    }

    public record LeaseInfo(String ownerId, long expireAt, long version, LeaseState state) {
        public boolean expired() {
            return !isActive(System.currentTimeMillis());
        }

        public boolean isActive(long now) {
            return state == LeaseState.ACTIVE && now < expireAt;
        }

        public boolean allowsAcquire(long now) {
            return switch (state) {
                case ACTIVE -> now >= expireAt;
                case RELEASED -> true;
                case READ_ONLY, MARKED_FOR_DELETION, CLOSED, DESTROYED -> false;
            };
        }

        @Override
        public String toString() {
            return "Lease(owner="
                    + ownerId
                    + ", expireAt="
                    + Instant.ofEpochMilli(expireAt)
                    + ", version="
                    + version
                    + ", state="
                    + state
                    + ")";
        }
    }

    private boolean isLeaseWritableByThisProcess() {
        LeaseInfo info = readLeaseInternal();
        if (info == null) {
            return true;
        }
        if (!info.isActive(System.currentTimeMillis())) {
            return true;
        }
        return activeLeaseOwnerId != null && activeLeaseOwnerId.equals(info.ownerId());
    }

    private void startLeaseRenewalIfNeeded(String ownerId) {
        if (!leaseOwner) {
            return;
        }
        if (leaseRenewTask != null && !leaseRenewTask.isCancelled()) {
            return;
        }
        long periodMs = Math.max(1_000L, leaseTtlMs / 3);
        leaseRenewTask =
                LEASE_RENEWER.scheduleAtFixedRate(
                        () -> {
                            try {
                                if (!renewLeaseIfOwner(ownerId, leaseTtlMs)) {
                                    stopLeaseRenewal();
                                }
                            } catch (Throwable t) {
                                // swallow to keep scheduling alive
                            }
                        },
                        periodMs,
                        periodMs,
                        TimeUnit.MILLISECONDS);
    }

    private void stopLeaseRenewal() {
        ScheduledFuture<?> task = leaseRenewTask;
        leaseRenewTask = null;
        leaseOwner = false;
        if (task != null) {
            task.cancel(false);
        }
    }

    private static String buildProcessOwnerId() {
        long pid;
        try {
            pid = ProcessHandle.current().pid();
        } catch (Throwable t) {
            pid = -1L;
        }
        String host;
        try {
            host = java.net.InetAddress.getLocalHost().getHostName();
        } catch (Throwable t) {
            host = "unknown-host";
        }
        return host + ":" + pid + ":" + UUID.randomUUID();
    }

    public static String routingOwnerId(String executorId, long epoch) {
        return "routing-owner:" + executorId + ":" + epoch;
    }

    public boolean ensureRoutingOwnerLease(String executorId, long epoch) {
        String ownerId = routingOwnerId(executorId, epoch);
        LeaseInfo info = readLeaseInternal();
        if (info != null
                && info.isActive(System.currentTimeMillis())
                && ownerId.equals(info.ownerId())) {
            activeLeaseOwnerId = ownerId;
            leaseOwner = true;
            startLeaseRenewalIfNeeded(ownerId);
            renewLeaseIfOwner(ownerId, leaseTtlMs);
            return true;
        }
        stopLeaseRenewal();
        boolean acquired = tryAcquireLease(ownerId, leaseTtlMs);
        if (acquired) {
            activeLeaseOwnerId = ownerId;
            leaseOwner = true;
            startLeaseRenewalIfNeeded(ownerId);
        }
        return acquired;
    }

    private LeaseInfo readLeaseFromChannel(FileChannel channel) throws IOException {
        channel.position(0L);
        ByteBuffer buffer =
                ByteBuffer.allocate((int) Math.min(Math.max(channel.size(), 0L), 4096L));
        while (buffer.hasRemaining() && channel.read(buffer) > 0) {
            // keep reading
        }
        buffer.flip();
        if (!buffer.hasRemaining()) {
            return null;
        }
        String content = StandardCharsets.UTF_8.decode(buffer).toString();
        String[] parts = content.split("\n");
        if (parts.length < 2) {
            return null;
        }
        String owner = parts[0].trim();
        long expireAt = Long.parseLong(parts[1].trim());
        long version = parts.length >= 3 ? Long.parseLong(parts[2].trim()) : 1L;
        LeaseState state =
                parts.length >= 4 ? LeaseState.valueOf(parts[3].trim()) : LeaseState.ACTIVE;
        return new LeaseInfo(owner, expireAt, version, state);
    }

    private void writeLeaseToChannel(
            FileChannel channel, String ownerId, long expireAt, long version, LeaseState state)
            throws IOException {
        byte[] content =
                (ownerId + "\n" + expireAt + "\n" + version + "\n" + state.name())
                        .getBytes(StandardCharsets.UTF_8);
        channel.truncate(0L);
        channel.position(0L);
        channel.write(ByteBuffer.wrap(content));
        channel.force(true);
    }

    private void updateLeaseState(LeaseState state, String preferredOwnerId) {
        try (FileChannel channel =
                FileChannel.open(
                        leasePath,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE)) {
            FileLock lock = tryLockLeaseFile(channel);
            if (lock == null) {
                throw new IOException(
                        "Failed to acquire lease file lock for state transition: " + leasePath);
            }
            try {
                LeaseInfo current = readLeaseFromChannel(channel);
                String ownerId = preferredOwnerId;
                long nextVersion = 1L;
                if (current != null) {
                    if (ownerId == null || ownerId.isBlank()) {
                        ownerId = current.ownerId();
                    }
                    nextVersion = current.version() + 1L;
                }
                if (ownerId == null) {
                    ownerId = "";
                }
                writeLeaseToChannel(channel, ownerId, 0L, nextVersion, state);
            } finally {
                releaseQuietly(lock);
            }
            clearActiveLeaseOwner(preferredOwnerId);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to update lease state for " + instanceId + " to " + state, e);
        }
    }

    private FileLock tryLockLeaseFile(FileChannel channel) throws IOException {
        try {
            return channel.tryLock();
        } catch (OverlappingFileLockException e) {
            return null;
        }
    }

    private void releaseQuietly(FileLock lock) {
        try {
            if (lock != null && lock.isValid()) {
                lock.release();
            }
        } catch (IOException ignored) {
        }
    }

    private void clearActiveLeaseOwner(String ownerId) {
        if (ownerId != null && ownerId.equals(activeLeaseOwnerId)) {
            activeLeaseOwnerId = null;
        }
    }

    // ==================== Sentinel (Phase 3) ====================

    /** Returns the maximum commitTs from the sentinel sub-DB, or 0 if none. */
    public long getMaxCommitTsSentinel() {
        if (metaDbi == null) {
            return 0L;
        }
        rwLock.readLock().lock();
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer valBuf =
                    metaDbi.get(txn, getKeyBuffer(SENTINEL_KEY.length).put(SENTINEL_KEY).flip());
            if (valBuf == null) {
                return 0L;
            }
            byte[] bytes = new byte[valBuf.remaining()];
            valBuf.get(bytes);
            return ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.BIG_ENDIAN).getLong();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Atomically updates the sentinel to max(current_sentinel, commitTs) within the same LMDB write
     * transaction as the data put.
     */
    public PutStatus putRecordWithSentinel(byte[] key, byte[] value, long commitTs) {
        if (isReadOnly() || isMarkedForDeletion()) {
            return PutStatus.READ_ONLY;
        }

        rwLock.readLock().lock();
        boolean needsMarkReadOnly = false;
        try {
            if (isReadOnly() || isMarkedForDeletion()) {
                return PutStatus.READ_ONLY;
            }
            if (!isLeaseWritableByThisProcess()) {
                return PutStatus.READ_ONLY;
            }
            if (!hasSpaceForWrite(value.length)) {
                return PutStatus.MAP_FULL;
            }
            writeTxnLock.lock();
            try {
                try (Txn<ByteBuffer> txn = env.txnWrite()) {
                    ByteBuffer keyBuf = getKeyBuffer(key.length);
                    keyBuf.put(key).flip();
                    ByteBuffer valBuf = getValueBuffer(value.length);
                    valBuf.put(value).flip();

                    dbi.put(txn, keyBuf, valBuf);

                    if (metaDbi != null && commitTs > 0) {
                        atomicMaxUpdateSentinel(txn, commitTs);
                    }

                    txn.commit();
                    updateWriteStatistics(key.length, value.length);
                    return PutStatus.SUCCESS;
                }
            } catch (Env.MapFullException e) {
                needsMarkReadOnly = true;
                return PutStatus.MAP_FULL;
            } finally {
                writeTxnLock.unlock();
            }
        } finally {
            rwLock.readLock().unlock();
            if (needsMarkReadOnly) {
                markAsReadOnly();
            }
        }
    }

    private void atomicMaxUpdateSentinel(Txn<ByteBuffer> txn, long commitTs) {
        ByteBuffer keyBuf = getKeyBuffer(SENTINEL_KEY.length);
        keyBuf.put(SENTINEL_KEY).flip();
        ByteBuffer existing = metaDbi.get(txn, keyBuf);
        long currentMax = 0L;
        if (existing != null) {
            byte[] bytes = new byte[existing.remaining()];
            existing.get(bytes);
            currentMax = ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.BIG_ENDIAN).getLong();
        }
        if (commitTs > currentMax) {
            byte[] newBytes = new byte[Long.BYTES];
            ByteBuffer.wrap(newBytes).order(java.nio.ByteOrder.BIG_ENDIAN).putLong(commitTs);
            ByteBuffer valBuf = getValueBuffer(newBytes.length);
            valBuf.put(newBytes).flip();
            metaDbi.put(txn, keyBuf, valBuf);
            log.debug("Sentinel updated: {} -> {}", currentMax, commitTs);
        }
    }

    // ==================== Header Probe (Phase 2 + Phase 3) ====================

    /** Returns a {@link RecordHeaderProbe} backed by this LMDB instance. */
    public RecordHeaderProbe asHeaderProbe() {
        return this::probeCommitTs;
    }

    @Override
    public OptionalLong probeCommitTs(byte[] key) {
        byte[] raw = get(key);
        if (raw == null || raw.length == 0) {
            return OptionalLong.empty();
        }
        try {
            byte[] decoded = io.github.rawwise.engine.storage.codec.NullableValueCodec.decode(raw);
            if (decoded == null) {
                return OptionalLong.empty();
            }
            long commitTs =
                    io.github.rawwise.model.record.impl.DefaultRecordCodec.peekCommitTimestamp(
                            decoded);
            return OptionalLong.of(commitTs);
        } catch (Exception e) {
            throw new StorageException(
                    "Failed to probe commitTs from LMDB header for key "
                            + java.util.Arrays.hashCode(key),
                    e);
        }
    }

    // ==================== Sentinel Lag Scan (Phase 3 Task 3.4) ====================

    /**
     * Samples N random rows to check if sentinel is behind any row's commitTimestamp. If mismatch
     * found, triggers full scan to repair sentinel.
     *
     * @param sampleSize number of rows to sample (default 256)
     * @return true if sentinel was repaired
     */
    public boolean scanAndRepairSentinel(int sampleSize) {
        if (metaDbi == null) {
            return false;
        }
        long sentinel = getMaxCommitTsSentinel();
        long maxFound = sentinel;

        CloseableIterator<Pair<byte[], byte[]>> iter = closeableIterator();
        try {
            int count = 0;
            while (iter.hasNext() && count < sampleSize) {
                Pair<byte[], byte[]> kv = iter.next();
                try {
                    long ts = peekCommitTsFromWrapped(kv.getRight());
                    if (ts > maxFound) {
                        maxFound = ts;
                    }
                } catch (Exception e) {
                    // Non-record data (e.g. markers) — skip
                }
                count++;
            }
        } finally {
            try {
                iter.close();
            } catch (IOException ignored) {
            }
        }

        if (maxFound > sentinel) {
            log.warn(
                    "Sentinel mismatch detected: sentinel={}, maxFound={}. Triggering full scan repair.",
                    sentinel,
                    maxFound);

            iter = closeableIterator();
            try {
                while (iter.hasNext()) {
                    Pair<byte[], byte[]> kv = iter.next();
                    try {
                        long ts = peekCommitTsFromWrapped(kv.getRight());
                        if (ts > maxFound) {
                            maxFound = ts;
                        }
                    } catch (Exception e) {
                        // skip
                    }
                }
            } finally {
                try {
                    iter.close();
                } catch (IOException ignored) {
                }
            }

            if (maxFound > sentinel) {
                forceUpdateSentinel(maxFound);
                log.info("Sentinel repaired via full scan: {} -> {}", sentinel, maxFound);
                return true;
            }
        }
        return false;
    }

    private static long peekCommitTsFromWrapped(byte[] wrapped) {
        byte[] decoded = io.github.rawwise.engine.storage.codec.NullableValueCodec.decode(wrapped);
        if (decoded == null) {
            throw new IllegalArgumentException("NullableValueCodec.decode returned null");
        }
        return io.github.rawwise.model.record.impl.DefaultRecordCodec.peekCommitTimestamp(decoded);
    }

    private void forceUpdateSentinel(long commitTs) {
        if (metaDbi == null) {
            return;
        }
        rwLock.readLock().lock();
        try {
            writeTxnLock.lock();
            try {
                try (Txn<ByteBuffer> txn = env.txnWrite()) {
                    atomicMaxUpdateSentinel(txn, commitTs);
                    txn.commit();
                }
            } finally {
                writeTxnLock.unlock();
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /** Scans all provided instances and returns the maximum sentinel value. */
    public static long collectSentinelMaxFromInstances(
            java.util.Collection<LmdbInstance> instances) {
        long max = 0L;
        for (LmdbInstance inst : instances) {
            if (inst != null) {
                max = Math.max(max, inst.getMaxCommitTsSentinel());
            }
        }
        return max;
    }
}
