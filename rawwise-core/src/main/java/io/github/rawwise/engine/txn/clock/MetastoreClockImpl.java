package io.github.rawwise.engine.txn.clock;

import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

/**
 * Metastore CAS-based cross-node logical clock implementation.
 *
 * <p>Ensures global monotonically increasing via Metastore compareAndSwap atomic operation. Uses
 * batch pre-allocation (batchSize=256) to reduce Metastore CAS contention frequency.
 *
 * <h3>Design details</h3>
 *
 * <ul>
 *   <li>fast path: local AtomicLong localCursor incrementing, lazy &lt;10ns
 *   <li>Slow path: synchronized(allocLock) + double-check + Metastore CAS
 *   <li>localCursor uses getAndIncrement() compared with front boundary localUpperBound, ensures
 *       txnId is strictly monotonically increasing (INV-009)
 *   <li>now() returns localCache, zero-contention read
 * </ul>
 *
 * <h3>Sequence example</h3>
 *
 * <pre>
 * Initial: Metastore=100, batchSize=256
 * firsttime CAS: CAS(100→356), base=101, localCursor=102, upperBound=356
 * allocation sequence: 101, 102, 103, ..., 356, 357(→slow→allocate new batch), 358, ...
 * </pre>
 */
@Slf4j
public final class MetastoreClockImpl implements LogicalClock {

    private static final int BATCH_SIZE = 256;

    private final Metastore metastore;
    private final byte[] clockKey;
    private final int batchSize;
    private final AtomicLong localCache;
    private final AtomicLong localCursor;
    private volatile long localUpperBound;
    private final Object allocLock = new Object();

    private final Path localPersistPath;
    private boolean persistEnabled;

    /** Create Metastore clock. */
    public MetastoreClockImpl(Metastore metastore, byte[] clockKey) {
        this(metastore, clockKey, BATCH_SIZE, false, null);
    }

    /** Create Metastore clock (specified batch size, H-10.1). */
    public MetastoreClockImpl(Metastore metastore, byte[] clockKey, int batchSize) {
        this(metastore, clockKey, batchSize, false, null);
    }

    /**
     * Create Metastore clock with local persistence.
     *
     * @param metastore Metastore instance
     * @param clockKey clock key (byte array)
     * @param persistEnabled whether local file persistence is enabled
     * @param persistDir local persistence directory
     */
    public MetastoreClockImpl(
            Metastore metastore, byte[] clockKey, boolean persistEnabled, Path persistDir) {
        this(metastore, clockKey, BATCH_SIZE, persistEnabled, persistDir);
    }

    private MetastoreClockImpl(
            Metastore metastore,
            byte[] clockKey,
            int batchSize,
            boolean persistEnabled,
            Path persistDir) {
        this.metastore = metastore;
        this.clockKey = clockKey.clone();
        this.batchSize = batchSize;
        this.localCache = new AtomicLong(0);
        this.localCursor = new AtomicLong(0);
        this.localUpperBound = -1;
        this.persistEnabled = persistEnabled;
        this.localPersistPath =
                (persistDir != null) ? persistDir.resolve("logical_clock.dat") : null;

        try {
            long persisted = loadFromMetastore();
            if (persisted > 0) {
                localCache.set(persisted);
                localCursor.set(persisted + 1);
                localUpperBound = persisted;
            }
        } catch (IOException e) {
            log.warn("Failed to initialize MetastoreClockImpl from Metastore", e);
        }
    }

    @Override
    public long next() {
        long current = localCursor.getAndIncrement();
        if (current <= localUpperBound) {
            return current;
        }

        synchronized (allocLock) {
            current = localCursor.get();
            if (current <= localUpperBound) {
                return localCursor.getAndIncrement();
            }

            long base = casAllocate(batchSize);
            localCursor.set(base + 1);
            localUpperBound = base + batchSize - 1;
            return base;
        }
    }

    @Override
    public long now() {
        // CRITICAL-2: return last allocated value, not batch upper bound.
        // localCursor is at the next-to-allocate value, so last allocated = localCursor.get() - 1.
        // This avoids MVCC isolation breakage caused by returning uncommitted upper bounds.
        long cursor = localCursor.get();
        return cursor > 0 ? cursor - 1 : 0;
    }

    @Override
    public long advanceToAtLeast(long minValue) {
        return localCache.updateAndGet(
                current -> {
                    long target = Math.max(current, minValue);
                    try {
                        byte[] key = clockKey;
                        byte[] currentBytes = longToBytes(current);
                        byte[] targetBytes = longToBytes(target);

                        List<Precondition> preconditions =
                                List.of(new Precondition.ValueEquals(key, currentBytes));
                        List<Mutation> mutations = List.of(new Mutation.Put(key, targetBytes));
                        metastore.compareAndSwap(preconditions, mutations);
                    } catch (IOException e) {
                        // Re-throw as RuntimeException — silent swallowing masks clock desync bugs
                        throw new ClockAllocationException(
                                "advanceToAtLeast failed for minValue=" + minValue, e);
                    }
                    return target;
                });
    }

    /**
     * CAS allocates count logical ticks, returns the lower bound (base) of this allocation.
     *
     * @param count allocation count
     * @return starting tick of this allocation
     */
    private long casAllocate(int count) {
        long current, next;
        int retries = 0;
        do {
            try {
                current = readMetastoreLong();
                next = current + count;

                byte[] nextBytes = longToBytes(next);
                List<Mutation> mutations = List.of(new Mutation.Put(clockKey, nextBytes));

                boolean casSucceeded;
                if (current == 0L && !metastore.get(clockKey).isPresent()) {
                    List<Precondition> preconditions =
                            List.of(new Precondition.NotExists(clockKey));
                    casSucceeded = metastore.compareAndSwap(preconditions, mutations);
                } else {
                    byte[] currentBytes = longToBytes(current);
                    List<Precondition> preconditions =
                            List.of(new Precondition.ValueEquals(clockKey, currentBytes));
                    casSucceeded = metastore.compareAndSwap(preconditions, mutations);
                }

                if (casSucceeded) {
                    localCache.set(next);
                    persistToLocal(next);
                    return current + 1;
                }
                retries++;
            } catch (IOException e) {
                log.error("CAS allocation failed at retry {}", retries, e);
                throw new ClockAllocationException("Metastore CAS allocation failed", e);
            }
        } while (retries < 100);

        throw new ClockAllocationException("CAS allocation failed after " + retries + " retries");
    }

    /** Load current clock value from Metastore. */
    private long loadFromMetastore() throws IOException {
        return readMetastoreLong();
    }

    private long readMetastoreLong() throws IOException {
        return metastore.get(clockKey).map(MetastoreClockImpl::bytesToLong).orElse(0L);
    }

    /** Persist current value to local file during batch allocation. */
    private void persistToLocal(long value) {
        if (!persistEnabled || localPersistPath == null) {
            return;
        }
        try {
            byte[] bytes = longToBytes(value);
            Files.write(
                    localPersistPath,
                    bytes,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            log.warn("Failed to persist clock to local file: {}", localPersistPath, e);
        }
    }

    /**
     * Restore clock value from local file.
     *
     * @param persistDir persistence directory
     * @return restored clock value, returns 0 if file does not exist
     */
    public static long recoverFromLocal(Path persistDir) {
        Path path = persistDir.resolve("logical_clock.dat");
        if (!Files.exists(path)) {
            return 0L;
        }
        try {
            byte[] bytes = Files.readAllBytes(path);
            if (bytes.length >= 8) {
                return bytesToLong(bytes);
            }
        } catch (IOException e) {
            log.warn("Failed to recover clock from local file: {}", path, e);
        }
        return 0L;
    }

    // --- Utility methods ---

    private static byte[] longToBytes(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    private static long bytesToLong(byte[] bytes) {
        if (bytes == null || bytes.length < Long.BYTES) {
            return 0L;
        }
        return ByteBuffer.wrap(bytes).getLong();
    }

    /**
     * Advance this clock to at least the max commitTs found in LMDB sentinels.
     *
     * <p>Scans all provided LMDB instances for their sentinel values, takes the max across all
     * sources, and advances the clock if needed. Call this after recovery completes and all
     * instances are loaded.
     *
     * @param sentinelMax the max commitTs across all LMDB sentinels
     */
    public void advanceFromLmdbSentinels(long sentinelMax) {
        if (sentinelMax > now()) {
            advanceToAtLeast(sentinelMax + 1);
            log.info(
                    "Clock advanced from LMDB sentinels: sentinelMax={}, newNow={}",
                    sentinelMax,
                    now());
        }
    }

    /** Clock allocation failure exception. */
    public static final class ClockAllocationException extends RuntimeException {
        public ClockAllocationException(String message) {
            super(message);
        }

        public ClockAllocationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
