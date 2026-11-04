package io.github.rawwise.engine.txn;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * eachtransactionreadresultcache, for REPEATABLE_READ isolationlevelunderensure repeatable
 * readsemantics.
 *
 * <p>whentransactionfirstreadsome key when, willresultcache; subsequenttosameone key
 * readdirectlyreturncacheresult, i.e.letothertransactioninthisduringcommitnewversion,
 * alsoenergyensuresameonetransactionwithinreadresultconsistentity.
 *
 * <p>memoryhasboundary: whencachetotalbytescountexceedconfigmaximumvaluewhen, new put
 * operationthrow {@link WriteBufferOverflowException}.
 *
 * <p>Thread safety: bytable ID partitionstorage, use ConcurrentHashMap ensureThread safety.
 * typicalscenarioundereachtransactionsinglethreadaccess(ThreadLocal bound), becausethis CHM
 * mainneedforsecurityguarantee rather thanperformance.
 */
public class TxnReadCache {

    private static final long DEFAULT_MAX_BYTES = 32L * 1024 * 1024; // 32 MB

    /**
     * cacheinreadresult.
     *
     * @param exists true tableshowdataexist, false tableshowdatadoes not exist
     * @param value datavalue(preventdefenseitycopy), does not existwhenas null
     */
    public record CachedRead(boolean exists, byte[] value) {
        public CachedRead {
            if (exists && value == null) {
                throw new IllegalArgumentException("value must not be null when exists is true");
            }
            // preventdefenseitycopy
            value = (value != null) ? value.clone() : null;
        }

        /** return valuepreventdefenseitycopy. */
        public byte[] getValue() {
            return value != null ? value.clone() : null;
        }

        long memorySize() {
            return value != null ? value.length : 0;
        }
    }

    private final ConcurrentHashMap<String, ConcurrentHashMap<ByteBuffer, CachedRead>> cache;
    private final long maxBytes;
    private final AtomicLong currentBytes;

    public TxnReadCache() {
        this(DEFAULT_MAX_BYTES);
    }

    public TxnReadCache(long maxBytes) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive, got: " + maxBytes);
        }
        this.cache = new ConcurrentHashMap<>();
        this.maxBytes = maxBytes;
        this.currentBytes = new AtomicLong(0);
    }

    /**
     * cachea foundreadresult.
     *
     * @param tableId tableID
     * @param key datakey
     * @param value datavalue
     * @throws WriteBufferOverflowException ifadding this entry will exceed maxBytes
     */
    public void put(String tableId, byte[] key, byte[] value) {
        if (tableId == null || tableId.isBlank()) {
            throw new IllegalArgumentException("tableId must not be blank");
        }
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        if (value == null) {
            throw new IllegalArgumentException("value must not be null for put operation");
        }

        ByteBuffer keyBuffer = ByteBuffer.wrap(key.clone());
        CachedRead newEntry = new CachedRead(true, value);
        long entrySize = key.length + value.length;

        ConcurrentHashMap<ByteBuffer, CachedRead> tableCache =
                cache.computeIfAbsent(tableId, k -> new ConcurrentHashMap<>());

        boolean[] overflow = {false};
        tableCache.compute(
                keyBuffer,
                (k, oldEntry) -> {
                    long delta;
                    if (oldEntry != null) {
                        delta = entrySize - (key.length + oldEntry.memorySize());
                    } else {
                        delta = entrySize;
                    }

                    if (delta > 0) {
                        long newTotal = currentBytes.get() + delta;
                        if (newTotal > maxBytes) {
                            overflow[0] = true;
                            return oldEntry;
                        }
                    }

                    currentBytes.addAndGet(delta);
                    return newEntry;
                });

        if (overflow[0]) {
            throw new WriteBufferOverflowException(currentBytes.get(), maxBytes, entrySize);
        }
    }

    /**
     * cachea not foundresult(datadoes not exist).
     *
     * @param tableId tableID
     * @param key datakey
     * @throws WriteBufferOverflowException ifadding this entry will exceed maxBytes
     */
    public void putNotFound(String tableId, byte[] key) {
        if (tableId == null || tableId.isBlank()) {
            throw new IllegalArgumentException("tableId must not be blank");
        }
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }

        ByteBuffer keyBuffer = ByteBuffer.wrap(key.clone());
        CachedRead newEntry = new CachedRead(false, null);
        long entrySize = key.length;

        ConcurrentHashMap<ByteBuffer, CachedRead> tableCache =
                cache.computeIfAbsent(tableId, k -> new ConcurrentHashMap<>());

        boolean[] overflow = {false};
        tableCache.compute(
                keyBuffer,
                (k, oldEntry) -> {
                    long delta;
                    if (oldEntry != null) {
                        delta = entrySize - (key.length + oldEntry.memorySize());
                    } else {
                        delta = entrySize;
                    }

                    if (delta > 0) {
                        long newTotal = currentBytes.get() + delta;
                        if (newTotal > maxBytes) {
                            overflow[0] = true;
                            return oldEntry;
                        }
                    }

                    currentBytes.addAndGet(delta);
                    return newEntry;
                });

        if (overflow[0]) {
            throw new WriteBufferOverflowException(currentBytes.get(), maxBytes, entrySize);
        }
    }

    /**
     * fromcacheingetspecified tableId + key readresult.
     *
     * @param tableId tableID
     * @param key datakey
     * @return cachereadresult, ifnotcommandinreturn null
     */
    public CachedRead get(String tableId, byte[] key) {
        if (tableId == null || key == null) {
            return null;
        }

        ConcurrentHashMap<ByteBuffer, CachedRead> tableCache = cache.get(tableId);
        if (tableCache == null) {
            return null;
        }

        ByteBuffer keyBuffer = ByteBuffer.wrap(key.clone());
        return tableCache.get(keyBuffer);
    }

    /** clearemptyallcacheentry and resetbytescountcount. */
    public void clear() {
        cache.clear();
        currentBytes.set(0);
    }

    /**
     * @return currentlymemoryuseamount(bytes)
     */
    public long currentBytes() {
        return currentBytes.get();
    }

    /**
     * @return maximumallowmemoryuseamount(bytes)
     */
    public long maxBytes() {
        return maxBytes;
    }

    /**
     * @return alltableincachetotal entries
     */
    public int size() {
        int total = 0;
        for (ConcurrentHashMap<ByteBuffer, CachedRead> tableCache : cache.values()) {
            total += tableCache.size();
        }
        return total;
    }
}
