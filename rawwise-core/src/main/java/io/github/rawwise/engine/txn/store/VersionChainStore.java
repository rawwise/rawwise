package io.github.rawwise.engine.txn.store;

import io.github.rawwise.engine.storage.mvcc.LmdbVersionChainStore;
import io.github.rawwise.engine.storage.mvcc.VersionChainCodec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import lombok.extern.slf4j.Slf4j;

/**
 * MVCC version chain store, for storing and querying multi-version history of data.
 *
 * <p>Key format (length-prefixed encoding): [tableIdLen:4B][tableId_bytes:N][pk_bytes:N]
 *
 * <p>Value: data bytes
 *
 * <p>Version chain semantics:
 *
 * <ul>
 *   <li>Multiple versions exist for the same (tableId, pk) combination, sorted by commitTs
 *       descending
 *   <li>During PIT (Point-In-Time) read, floor seek from the latest version: returns the latest
 *       version with commitTs &le; snapshotTs from the version chain
 *   <li>Batch interface batchGetAtSnapshot merges multiple LMDB accesses into one batch query
 * </ul>
 *
 * <p>Version retention policy (R7/R8):
 *
 * <ul>
 *   <li>In-memory chains bounded by {@link #MAX_IN_MEMORY_VERSIONS_PER_CHAIN} (1,000) per key —
 *       oldest versions truncated on insert.
 *   <li>Total distinct keys bounded by {@code maxEntries} (default 10,000); overflow evicted via
 *       lexicographic order and flushed to LMDB.
 *   <li>LMDB-layer cap: {@link LmdbVersionChainStore#MAX_VERSIONS_PER_CHAIN} (1,000) enforced on
 *       every write. Background GC prunes by retention config.
 *   <li>No in-memory sparse compaction is performed; all versions are preserved until eviction or
 *       the per-chain cap.
 * </ul>
 */
@Slf4j
public class VersionChainStore {

    private static final int KEY_TABLE_ID_LEN_SIZE = 4; // int: tableId byte length prefix
    private static final int DEFAULT_MAX_ENTRIES = 10_000;
    static final int MAX_IN_MEMORY_VERSIONS_PER_CHAIN = 1_000;

    /** Version entry: a single data version. */
    public record VersionEntry(long commitTs, byte[] value, long dataSize) {

        public VersionEntry {
            if (commitTs <= 0) throw new IllegalArgumentException("commitTs must be positive");
            if (value == null) throw new IllegalArgumentException("value must not be null");
            if (dataSize <= 0) dataSize = value.length;
        }

        public byte[] valueCopy() {
            return value.clone();
        }

        /** Convert to the LMDB-level VersionChainCodec.VersionEntry. */
        VersionChainCodec.VersionEntry toCodecEntry() {
            return new VersionChainCodec.VersionEntry(commitTs, valueCopy());
        }
    }

    /**
     * MVCC version chain: all historical versions of a key, sorted by commitTs descending (latest
     * first).
     */
    public static final class VersionChain {
        private final List<VersionEntry> versions;

        public VersionChain() {
            this.versions = new ArrayList<>();
        }

        /**
         * Add version (maintains descending order).
         *
         * <p>R8 fix: capped at {@link #MAX_IN_MEMORY_VERSIONS_PER_CHAIN} versions. Excess oldest
         * versions are truncated in-place. Unlike the removed {@code compact()} call, this uses
         * simple head retention (newest N) rather than sparse sampling — the policy is predictable
         * and symmetric with the LMDB-layer {@code MAX_VERSIONS_PER_CHAIN} cap. Truncated versions
         * that have not been flushed to LMDB are permanently lost; this is an acceptable trade-off
         * to bound memory in hot-key scenarios where eviction has not yet triggered.
         */
        public void addVersion(VersionEntry entry) {
            int pos = 0;
            while (pos < versions.size() && versions.get(pos).commitTs() > entry.commitTs()) {
                pos++;
            }
            versions.add(pos, entry);
            while (versions.size() > MAX_IN_MEMORY_VERSIONS_PER_CHAIN) {
                VersionEntry removed = versions.remove(versions.size() - 1);
                log.debug(
                        "VersionChain truncated: dropped commitTs={} (chain exceeded {} cap)",
                        removed.commitTs(),
                        MAX_IN_MEMORY_VERSIONS_PER_CHAIN);
            }
        }

        /**
         * Get the latest visible version at the specified snapshot (floor seek).
         *
         * @param snapshotTs snapshot timestamp
         * @return commitTs &le; snapshotTs latest version, or null
         */
        public Optional<VersionEntry> getAtSnapshot(long snapshotTs) {
            for (VersionEntry entry : versions) {
                if (entry.commitTs() <= snapshotTs) {
                    return Optional.of(entry);
                }
            }
            return Optional.empty();
        }

        /** Get the latest version (maximum commitTs). */
        public Optional<VersionEntry> latest() {
            return versions.isEmpty() ? Optional.empty() : Optional.of(versions.get(0));
        }

        /** Return version count. */
        public int size() {
            return versions.size();
        }

        public List<VersionEntry> versions() {
            return Collections.unmodifiableList(versions);
        }

        /** Convert all entries to LMDB-level VersionChainCodec.VersionEntry list. */
        List<VersionChainCodec.VersionEntry> toCodecEntries() {
            List<VersionChainCodec.VersionEntry> result = new ArrayList<>(versions.size());
            for (VersionEntry ve : versions) {
                result.add(ve.toCodecEntry());
            }
            return result;
        }
    }

    private final ConcurrentSkipListMap<ByteBuffer, VersionChain> store;
    private final int maxEntries;
    private final LmdbVersionChainStore lmdbStore;

    /** Create a bounded version chain store with the default max capacity (10,000 entries). */
    public VersionChainStore() {
        this(DEFAULT_MAX_ENTRIES, null);
    }

    /**
     * Create a bounded version chain store with a configurable max capacity.
     *
     * @param maxEntries maximum number of distinct keys to retain in memory
     */
    public VersionChainStore(int maxEntries) {
        this(maxEntries, null);
    }

    /**
     * Create a bounded version chain store with LMDB backing for overflow eviction.
     *
     * @param maxEntries maximum number of distinct keys to retain in memory
     * @param lmdbStore optional LMDB store for evicted entries (may be null)
     */
    public VersionChainStore(int maxEntries, LmdbVersionChainStore lmdbStore) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be positive, got: " + maxEntries);
        }
        this.store = new ConcurrentSkipListMap<>();
        this.maxEntries = maxEntries;
        this.lmdbStore = lmdbStore;
    }

    /**
     * Add a version.
     *
     * <p>R7/R8 fix: removed automatic in-memory compact(). Version caps are enforced at both layers
     * — memory side via {@link VersionChain#addVersion(VersionEntry)} (truncates to {@link
     * #MAX_IN_MEMORY_VERSIONS_PER_CHAIN}), LMDB side via {@link
     * LmdbVersionChainStore#MAX_VERSIONS_PER_CHAIN} and scheduled GC.
     *
     * @param tableId table ID
     * @param key key
     * @param entry version entry
     */
    public void put(String tableId, byte[] key, VersionEntry entry) {
        ByteBuffer compositeKey = buildCompositeKey(tableId, key);
        store.compute(
                compositeKey,
                (k, chain) -> {
                    if (chain == null) {
                        chain = new VersionChain();
                    }
                    chain.addVersion(entry);
                    return chain;
                });
        evictIfNeeded();
    }

    /**
     * Evict oldest entries when the store exceeds the configured max capacity.
     *
     * <p>Evicts the first key in lexicographic order from the skip list. This is a pragmatic
     * stopgap: in production, a proper LRU cache (e.g. Caffeine) would provide better
     * access-pattern-aware eviction. The primary goal is to prevent unbounded memory growth that
     * would cause OOM under heavy write loads.
     *
     * <p>R6 fix: atomically removes the chain from memory first, then flushes to LMDB. If the LMDB
     * write fails, the chain is re-inserted into memory via {@code put()} (compute-based merge) to
     * avoid permanent data loss.
     */
    private void evictIfNeeded() {
        int evictionLimit = Math.max(store.size() - maxEntries + 1, 0);
        int rounds = 0;
        while (store.size() > maxEntries && rounds < evictionLimit) {
            ByteBuffer firstKey = store.firstKey();
            if (firstKey == null) {
                break;
            }
            VersionChain chain = store.remove(firstKey);
            rounds++;
            if (chain != null && lmdbStore != null && chain.size() > 0) {
                boolean flushed = false;
                try {
                    byte[] compositeKey = firstKey.array();
                    LmdbVersionChainStore.KeyPair keyPair =
                            LmdbVersionChainStore.splitCompositeKey(compositeKey);
                    List<VersionChainCodec.VersionEntry> codecEntries = chain.toCodecEntries();
                    flushed = lmdbStore.putAll(keyPair.tableId(), keyPair.pk(), codecEntries);
                } catch (RuntimeException e) {
                    log.warn("evictIfNeeded: flush to LMDB failed, re-inserting into memory", e);
                }
                if (!flushed) {
                    store.compute(
                            firstKey,
                            (k, existing) -> {
                                if (existing != null) {
                                    for (VersionEntry ve : chain.versions()) {
                                        existing.addVersion(ve);
                                    }
                                    return existing;
                                }
                                return chain;
                            });
                    log.warn(
                            "evictIfNeeded: LMDB write returned false, chain re-inserted "
                                    + "into memory (eviction deferred)");
                }
            }
        }
    }

    /**
     * PIT read: returns the latest version at the specified snapshot.
     *
     * <p>R6 fix: when the in-memory chain exists but does not contain a visible version for the
     * given snapshot (partial chain after eviction + re-insert), falls back to the LMDB backing
     * store which may retain older versions.
     *
     * @param tableId table ID
     * @param key key
     * @param snapshotTs snapshot timestamp
     * @return visible version, or Optional.empty() if none
     */
    public Optional<VersionEntry> getAtSnapshot(String tableId, byte[] key, long snapshotTs) {
        ByteBuffer compositeKey = buildCompositeKey(tableId, key);
        VersionChain chain = store.get(compositeKey);
        if (chain != null) {
            Optional<VersionEntry> memoryResult = chain.getAtSnapshot(snapshotTs);
            if (memoryResult.isPresent()) {
                return memoryResult;
            }
            if (lmdbStore != null) {
                Optional<VersionChainCodec.VersionEntry> lmdbEntry =
                        lmdbStore.getAtSnapshot(tableId, key, snapshotTs);
                if (lmdbEntry.isPresent()) {
                    return lmdbEntry.map(
                            e -> new VersionEntry(e.commitTs(), e.value(), e.value().length));
                }
            }
            return memoryResult;
        }
        if (lmdbStore != null) {
            Optional<VersionChainCodec.VersionEntry> lmdbEntry =
                    lmdbStore.getAtSnapshot(tableId, key, snapshotTs);
            return lmdbEntry.map(e -> new VersionEntry(e.commitTs(), e.value(), e.value().length));
        }
        return Optional.empty();
    }

    /**
     * Batch PIT read.
     *
     * @param keys key list
     * @param tableId table ID
     * @param snapshotTs snapshot timestamp
     * @return key → value mapping
     */
    public Map<byte[], byte[]> batchGetAtSnapshot(
            List<byte[]> keys, String tableId, long snapshotTs) {
        Map<byte[], byte[]> results = new HashMap<>();
        for (byte[] key : keys) {
            getAtSnapshot(tableId, key, snapshotTs)
                    .ifPresent(entry -> results.put(key, entry.valueCopy()));
        }
        return results;
    }

    /**
     * Get latest version.
     *
     * <p>R6 fix: falls back to LMDB when memory chain is empty or key is not in memory (partial
     * chain after eviction + re-insert).
     */
    public Optional<VersionEntry> latest(String tableId, byte[] key) {
        ByteBuffer compositeKey = buildCompositeKey(tableId, key);
        VersionChain chain = store.get(compositeKey);
        if (chain != null) {
            Optional<VersionEntry> memoryResult = chain.latest();
            if (memoryResult.isPresent()) {
                return memoryResult;
            }
            if (lmdbStore != null) {
                Optional<VersionChainCodec.VersionEntry> lmdbEntry = lmdbStore.latest(tableId, key);
                if (lmdbEntry.isPresent()) {
                    return lmdbEntry.map(
                            e -> new VersionEntry(e.commitTs(), e.value(), e.value().length));
                }
            }
            return memoryResult;
        }
        if (lmdbStore != null) {
            Optional<VersionChainCodec.VersionEntry> lmdbEntry = lmdbStore.latest(tableId, key);
            return lmdbEntry.map(e -> new VersionEntry(e.commitTs(), e.value(), e.value().length));
        }
        return Optional.empty();
    }

    /**
     * Range scan: returns versions visible at the specified snapshot timestamp within the specified
     * key range.
     *
     * <p>When LMDB backend storage is available, uses LMDB cursor for efficient range scan, applies
     * floor seek semantics to each key's version chain(returns the latest version with commitTs ≤
     * snapshotTs from the version chain). When LMDB backend is unavailable, falls back to linear
     * scan of in-memory store.
     *
     * <p>when {@code snapshotTs <= 0} when, returns the latest committed version for each key
     * (READ_COMMITTED semantics).
     *
     * @param tableId table ID
     * @param startKey start key (inclusive, unsigned byte comparison)
     * @param endKey end key (inclusive, unsigned byte comparison)
     * @param snapshotTs snapshot timestamp; only versions with commitTs ≤ snapshotTs are visible
     * @return list of (pk, visible version) pairs, only includes keys with visible versions
     * @throws IllegalArgumentException if tableId, startKey, or endKey is null
     */
    public List<RangeScanEntry> scanRangeAtSnapshot(
            String tableId, byte[] startKey, byte[] endKey, long snapshotTs) {
        if (tableId == null) throw new IllegalArgumentException("tableId must not be null");
        if (startKey == null) throw new IllegalArgumentException("startKey must not be null");
        if (endKey == null) throw new IllegalArgumentException("endKey must not be null");

        // Prefer LMDB backend for efficient range scan
        if (lmdbStore != null) {
            List<LmdbVersionChainStore.RangeScanEntry> lmdbResults =
                    lmdbStore.scanRangeAtSnapshot(tableId, startKey, endKey, snapshotTs);
            List<RangeScanEntry> results = new ArrayList<>(lmdbResults.size());
            for (LmdbVersionChainStore.RangeScanEntry e : lmdbResults) {
                results.add(new RangeScanEntry(e.pk(), e.value(), e.commitTs()));
            }
            return results;
        }

        // Fallback: in-memory store linear scan (for test scenarios without LMDB backend)
        return scanRangeInMemory(tableId, startKey, endKey, snapshotTs);
    }

    /**
     * In-memory store range scan fallback implementation.
     *
     * <p>Iterates in-memory ConcurrentSkipListMap, filters entries belonging to specified tableId
     * with pk in [startKey, endKey] range, applies floor seek to each version chain.
     */
    private List<RangeScanEntry> scanRangeInMemory(
            String tableId, byte[] startKey, byte[] endKey, long snapshotTs) {
        List<RangeScanEntry> results = new ArrayList<>();
        byte[] tableBytes = tableId.getBytes(StandardCharsets.UTF_8);
        int tablePrefixLen = KEY_TABLE_ID_LEN_SIZE + tableBytes.length;
        byte[] rangeStartComposite = buildCompositeKey(tableId, startKey).array();
        byte[] rangeEndComposite = buildCompositeKey(tableId, endKey).array();

        // ConcurrentSkipListMap.subMap returns [fromKey, toKey) view
        // We need [startKey, endKey] closed interval, so use subMap(fromKey, true, toKey, true)
        java.nio.ByteBuffer fromKey = java.nio.ByteBuffer.wrap(rangeStartComposite);
        java.nio.ByteBuffer toKey = java.nio.ByteBuffer.wrap(rangeEndComposite);

        java.util.NavigableMap<java.nio.ByteBuffer, VersionChain> subMap =
                store.subMap(fromKey, true, toKey, true);

        for (java.util.Map.Entry<java.nio.ByteBuffer, VersionChain> entry : subMap.entrySet()) {
            byte[] compositeKey = entry.getKey().array();

            // Verify tableId prefix match (subMap compares by byte, tableId prefix is included in
            // key)
            if (compositeKey.length < tablePrefixLen) continue;

            // Extract pk (skip tableId length prefix and tableId bytes)
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(compositeKey);
            int tidLen = buf.getInt();
            if (tidLen != tableBytes.length) continue;
            byte[] tidBytes = new byte[tidLen];
            buf.get(tidBytes);
            if (!java.util.Arrays.equals(tidBytes, tableBytes)) continue;

            byte[] pk = new byte[buf.remaining()];
            buf.get(pk);

            VersionChain chain = entry.getValue();
            Optional<VersionEntry> visible;
            if (snapshotTs <= 0) {
                visible = chain.latest();
            } else {
                visible = chain.getAtSnapshot(snapshotTs);
            }

            if (visible.isPresent()) {
                VersionEntry ve = visible.get();
                results.add(new RangeScanEntry(pk, ve.valueCopy(), ve.commitTs()));
            }
        }

        return results;
    }

    /**
     * Range scan result entry.
     *
     * @param pk primary key bytes
     * @param value visible value bytes (copied)
     * @param commitTs visible version's commit timestamp
     */
    public record RangeScanEntry(byte[] pk, byte[] value, long commitTs) {

        public RangeScanEntry {
            if (pk == null) throw new IllegalArgumentException("pk must not be null");
            if (value == null) throw new IllegalArgumentException("value must not be null");
        }
    }

    /** Delete all versions of the specified key. */
    public void remove(String tableId, byte[] key) {
        ByteBuffer compositeKey = buildCompositeKey(tableId, key);
        store.remove(compositeKey);
    }

    // ---- Introspection (FIX-6: for testability) ----

    /**
     * Count the number of version chains (keys) in the store.
     *
     * @return total number of version chain entries
     */
    public int keyCount() {
        return store.size();
    }

    /**
     * Flush all in-memory version chains to the LMDB-backed store.
     *
     * <p>C2 fix: Uses the new {@link LmdbVersionChainStore#putAll(String, byte[], List)} batch
     * method instead of calling {@code put()} for each version individually. This avoids O(n)
     * per-version LMDB transactions and instead writes each complete chain in a single put.
     *
     * <p>H4 fix: Uses the length-prefixed composite key format ({@code
     * [tableIdLen:4B][tableId_bytes:N][pk_bytes:N]}) instead of the 0x00-separator format, matching
     * the updated {@link LmdbVersionChainStore}.
     *
     * @param lmdbStore the LMDB version chain store to flush to
     * @return number of keys flushed
     */
    public int flushToLmdb(LmdbVersionChainStore lmdbStore) {
        if (lmdbStore == null) {
            log.warn("flushToLmdb called with null lmdbStore, skipping");
            return 0;
        }
        int flushed = 0;
        for (Map.Entry<ByteBuffer, VersionChain> entry : store.entrySet()) {
            byte[] compositeKey = entry.getKey().array();

            // H4 fix: Split length-prefixed composite key instead of 0x00 search
            LmdbVersionChainStore.KeyPair keyPair;
            try {
                keyPair = LmdbVersionChainStore.splitCompositeKey(compositeKey);
            } catch (RuntimeException e) {
                log.warn("flushToLmdb: malformed composite key, cannot split", e);
                continue;
            }

            VersionChain chain = entry.getValue();
            List<VersionChainCodec.VersionEntry> codecEntries = chain.toCodecEntries();

            // C2 fix: Use batch putAll() — one LMDB write txn per key, not per version
            try {
                boolean success = lmdbStore.putAll(keyPair.tableId(), keyPair.pk(), codecEntries);
                if (success) {
                    flushed++;
                } else {
                    log.error(
                            "flushToLmdb: putAll returned false for tableId={} keyLen={} versions={}",
                            keyPair.tableId(),
                            keyPair.pk().length,
                            codecEntries.size());
                }
            } catch (RuntimeException e) {
                log.error(
                        "flushToLmdb: failed to putAll for tableId={} keyLen={}",
                        keyPair.tableId(),
                        keyPair.pk().length,
                        e);
            }
        }
        log.info("flushToLmdb: flushed {} version chains to LMDB", flushed);
        return flushed;
    }

    /**
     * Build composite key (H4 fix: length-prefixed encoding): {@code
     * [tableIdLen:4B][tableId_bytes:N][pk_bytes:N]}
     */
    static ByteBuffer buildCompositeKey(String tableId, byte[] key) {
        byte[] tableBytes = tableId.getBytes(StandardCharsets.UTF_8);
        byte[] composite = new byte[KEY_TABLE_ID_LEN_SIZE + tableBytes.length + key.length];
        ByteBuffer buf = ByteBuffer.wrap(composite);
        buf.putInt(tableBytes.length);
        buf.put(tableBytes);
        buf.put(key);
        return ByteBuffer.wrap(composite);
    }
}
