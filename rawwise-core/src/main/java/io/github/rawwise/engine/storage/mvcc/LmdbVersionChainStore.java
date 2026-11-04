package io.github.rawwise.engine.storage.mvcc;

import static org.lmdbjava.DbiFlags.MDB_CREATE;

import io.github.rawwise.engine.storage.LmdbInstance;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.KeyRange;
import org.lmdbjava.Txn;

/**
 * LMDB-backed MVCC version chain store.
 *
 * <p>Stores full version history for each (tableId, key) pair in a separate LMDB environment,
 * enabling PIT (Point-In-Time) snapshot reads.
 *
 * <p>Key format (length-prefixed): {@code [tableIdLen:4B][tableId_bytes:N][pk_bytes:N]}. This
 * avoids separator ambiguity that would occur with 0x00-based delimiters.
 *
 * <p>Value format: binary blob with {@link VersionChainCodec} encoding.
 *
 * <p>Thread-safe: each operation opens its own LMDB transaction.
 */
@Slf4j
public final class LmdbVersionChainStore implements AutoCloseable {

    private static final String DBI_NAME = "version_chains";
    private static final String STORE_DIR_NAME = "version_chains_store";
    private static final long DEFAULT_MAP_SIZE = 8L * 1024L * 1024L * 1024L; // 8 GB
    private static final int KEY_TABLE_ID_LEN_SIZE = 4; // int: tableId byte length prefix
    private static final int GC_BATCH_SIZE = 100;
    static final int MAX_VERSIONS_PER_CHAIN = 1_000;

    private final Env<ByteBuffer> env;
    private final Dbi<ByteBuffer> dbi;
    private final Path storePath;
    private final long mapSize;

    // FIX-4: instance fields instead of static to avoid cross-instance interference.
    // Registry of direct ByteBuffers for cleanup in close()
    private final Set<ByteBuffer> directBuffers = ConcurrentHashMap.newKeySet();

    /**
     * Creates a new LmdbVersionChainStore with a dedicated LMDB environment, placed as a sibling
     * directory to the main data LmdbInstance.
     *
     * @param lmdbInstance the main data LMDB instance (used to derive the base path)
     */
    public LmdbVersionChainStore(LmdbInstance lmdbInstance) {
        this(lmdbInstance, DEFAULT_MAP_SIZE);
    }

    /**
     * Creates a new LmdbVersionChainStore with a custom map size.
     *
     * @param lmdbInstance the main data LMDB instance (used to derive the base path)
     * @param mapSize LMDB map size in bytes
     */
    public LmdbVersionChainStore(LmdbInstance lmdbInstance, long mapSize) {
        if (lmdbInstance == null) {
            throw new IllegalArgumentException("lmdbInstance must not be null");
        }
        if (mapSize <= 0) {
            throw new IllegalArgumentException("mapSize must be positive, got: " + mapSize);
        }
        this.mapSize = mapSize;
        // Place version chain store as a sibling directory under the parent of the data instance
        Path parentDir = lmdbInstance.path().getParent();
        if (parentDir == null) {
            throw new IllegalStateException(
                    "Cannot determine parent directory of LMDB instance: " + lmdbInstance.path());
        }
        this.storePath = parentDir.resolve(STORE_DIR_NAME);

        try {
            Files.createDirectories(storePath);
        } catch (java.io.IOException e) {
            throw new RuntimeException(
                    "Failed to create version chain store directory: " + storePath, e);
        }

        this.env =
                Env.create()
                        .setMapSize(mapSize)
                        .setMaxDbs(1)
                        .setMaxReaders(126)
                        .open(storePath.toFile());

        this.dbi = env.openDbi(DBI_NAME, MDB_CREATE);

        log.info(
                "LmdbVersionChainStore initialized at {}, mapSize={}MB",
                storePath,
                mapSize / (1024 * 1024));
    }

    // ---- Composite Key Encoding (length-prefixed, H4 fix) ----

    /**
     * Build the composite key with length-prefixed encoding: {@code
     * [tableIdLen:4B][tableId_bytes:N][pk_bytes:N]}.
     *
     * <p>This avoids separator ambiguity that would occur with 0x00-delimited keys when tableId or
     * pk bytes contain embedded 0x00.
     */
    private static byte[] buildCompositeKey(String tableId, byte[] key) {
        byte[] tableBytes = tableId.getBytes(StandardCharsets.UTF_8);
        byte[] composite = new byte[KEY_TABLE_ID_LEN_SIZE + tableBytes.length + key.length];
        ByteBuffer buf = ByteBuffer.wrap(composite);
        buf.putInt(tableBytes.length);
        buf.put(tableBytes);
        buf.put(key);
        return composite;
    }

    /**
     * Split a length-prefixed composite key back into tableId and pk.
     *
     * @param compositeKey the composite key bytes
     * @return a pair of [tableId, pk]
     */
    public static KeyPair splitCompositeKey(byte[] compositeKey) {
        if (compositeKey.length < KEY_TABLE_ID_LEN_SIZE) {
            throw new IllegalArgumentException("Composite key too short: " + compositeKey.length);
        }
        ByteBuffer buf = ByteBuffer.wrap(compositeKey);
        int tableIdLen = buf.getInt();
        if (tableIdLen < 0 || compositeKey.length < KEY_TABLE_ID_LEN_SIZE + tableIdLen) {
            throw new IllegalArgumentException(
                    "Invalid tableId length: "
                            + tableIdLen
                            + " in composite key of length "
                            + compositeKey.length);
        }
        byte[] tableIdBytes = new byte[tableIdLen];
        buf.get(tableIdBytes);
        String tableId = new String(tableIdBytes, StandardCharsets.UTF_8);

        int remaining = buf.remaining();
        byte[] pk = new byte[remaining];
        buf.get(pk);

        return new KeyPair(tableId, pk);
    }

    /** Holds the decomposed (tableId, pk) from a composite key. */
    public record KeyPair(String tableId, byte[] pk) {}

    // ---- Core Operations ----

    /**
     * Read the existing version chain from LMDB within an existing transaction.
     *
     * @param txn an active LMDB read or write transaction
     * @param tableId table identifier
     * @param key primary key bytes
     * @return the decoded version chain, or empty list if not found
     */
    private List<VersionChainCodec.VersionEntry> readChain(
            Txn<ByteBuffer> txn, String tableId, byte[] key) {
        byte[] compositeKey = buildCompositeKey(tableId, key);
        ByteBuffer keyBuf = allocateKeyBuffer(compositeKey);
        ByteBuffer valBuf = dbi.get(txn, keyBuf);
        if (valBuf == null) {
            return new ArrayList<>();
        }
        byte[] data = new byte[valBuf.remaining()];
        valBuf.get(data);
        return VersionChainCodec.decode(data);
    }

    /**
     * Read the version chain within a new read transaction. Convenience overload for read-only
     * operations (getAtSnapshot, latest).
     */
    private List<VersionChainCodec.VersionEntry> readChain(String tableId, byte[] key) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            return readChain(txn, tableId, key);
        }
    }

    /**
     * Append a new version to the version chain for the specified key.
     *
     * <p>B1 fix: Read existing chain AND write updated chain within a SINGLE LMDB write
     * transaction. This eliminates the RMW race where two concurrent put() calls could each read
     * the same old chain, append their version, and write back — losing the other's version.
     *
     * @param tableId table identifier
     * @param key primary key bytes
     * @param commitTs commit timestamp
     * @param value the data value for this version
     * @return true if the version was successfully persisted, false on MAP_FULL
     */
    public boolean put(String tableId, byte[] key, long commitTs, byte[] value) {
        if (tableId == null || key == null || value == null) {
            throw new IllegalArgumentException("tableId, key, and value must not be null");
        }
        byte[] compositeKey = buildCompositeKey(tableId, key);
        VersionChainCodec.VersionEntry newEntry =
                new VersionChainCodec.VersionEntry(commitTs, value);

        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            // Read existing chain within same write txn (B1: atomic read-modify-write)
            ByteBuffer keyBuf = allocateKeyBuffer(compositeKey);
            ByteBuffer valBuf = dbi.get(txn, keyBuf);
            List<VersionChainCodec.VersionEntry> chain;
            if (valBuf == null) {
                chain = new ArrayList<>();
            } else {
                byte[] data = new byte[valBuf.remaining()];
                valBuf.get(data);
                chain = VersionChainCodec.decode(data);
            }

            // Insert in descending order (newest first)
            int insertPos = 0;
            while (insertPos < chain.size() && chain.get(insertPos).commitTs() > commitTs) {
                insertPos++;
            }
            chain.add(insertPos, newEntry);

            // R6 fix: cap versions per chain to prevent unbounded LMDB growth
            if (chain.size() > MAX_VERSIONS_PER_CHAIN) {
                chain = VersionChainCodec.pruneByVersions(chain, MAX_VERSIONS_PER_CHAIN);
            }

            // Write back within same txn
            byte[] encoded = VersionChainCodec.encode(chain);
            ByteBuffer writeKeyBuf = allocateKeyBuffer(compositeKey);
            ByteBuffer writeValBuf = allocateValueBuffer(encoded);
            dbi.put(txn, writeKeyBuf, writeValBuf);
            txn.commit();

            log.debug(
                    "put version: tableId={} keyLen={} commitTs={} chainSize={}",
                    tableId,
                    key.length,
                    commitTs,
                    chain.size());
            return true;
        } catch (RuntimeException e) {
            log.error(
                    "put failed (MAP_FULL or other LMDB error): tableId={} keyLen={} commitTs={}",
                    tableId,
                    key.length,
                    commitTs,
                    e);
            return false;
        }
    }

    /**
     * Batch put: merge the incoming version chain entries with any existing chain in a single LMDB
     * read-modify-write transaction.
     *
     * <p>B3 fix: Unlike the original pure-overwrite implementation, this now reads the existing
     * chain within the same write txn, merges incoming entries (dedup by commitTs), and writes back
     * the merged chain. This prevents data loss when {@code putAll()} races with concurrent {@link
     * #put(String, byte[], long, byte[])} calls from the orchestrator.
     *
     * <p>Used by {@code VersionChainStore.flushToLmdb()} and {@code
     * VersionChainStore.evictIfNeeded()}.
     *
     * @param tableId table identifier
     * @param key primary key bytes
     * @param entries the version chain entries to merge (in descending commitTs order)
     * @return true if the batch was successfully persisted, false on MAP_FULL
     */
    public boolean putAll(
            String tableId, byte[] key, List<VersionChainCodec.VersionEntry> entries) {
        if (tableId == null || key == null || entries == null) {
            throw new IllegalArgumentException("tableId, key, and entries must not be null");
        }
        byte[] compositeKey = buildCompositeKey(tableId, key);
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            ByteBuffer keyBuf = allocateKeyBuffer(compositeKey);
            ByteBuffer existingVal = dbi.get(txn, keyBuf);
            List<VersionChainCodec.VersionEntry> merged;
            if (existingVal != null) {
                byte[] existingData = new byte[existingVal.remaining()];
                existingVal.get(existingData);
                List<VersionChainCodec.VersionEntry> existingChain =
                        VersionChainCodec.decode(existingData);
                merged = mergeChains(existingChain, entries);
            } else {
                merged = new ArrayList<>(entries);
            }
            // R6 fix: cap versions after merge to prevent unbounded LMDB growth
            if (merged.size() > MAX_VERSIONS_PER_CHAIN) {
                merged = VersionChainCodec.pruneByVersions(merged, MAX_VERSIONS_PER_CHAIN);
            }
            byte[] encoded = VersionChainCodec.encode(merged);
            ByteBuffer writeKeyBuf = allocateKeyBuffer(compositeKey);
            ByteBuffer writeValBuf = allocateValueBuffer(encoded);
            dbi.put(txn, writeKeyBuf, writeValBuf);
            txn.commit();
            log.debug(
                    "putAll: tableId={} keyLen={} existing={} incoming={} merged={}",
                    tableId,
                    key.length,
                    existingVal != null ? "yes" : "no",
                    entries.size(),
                    merged.size());
            return true;
        } catch (RuntimeException e) {
            log.error(
                    "putAll failed: tableId={} keyLen={} entries={}",
                    tableId,
                    key.length,
                    entries.size(),
                    e);
            return false;
        }
    }

    /**
     * Merge two version chains, deduplicating by commitTs. Both chains are assumed to be in
     * descending commitTs order. In case of duplicate commitTs, the incoming entry takes
     * precedence.
     */
    public static List<VersionChainCodec.VersionEntry> mergeChains(
            List<VersionChainCodec.VersionEntry> existing,
            List<VersionChainCodec.VersionEntry> incoming) {
        List<VersionChainCodec.VersionEntry> result =
                new ArrayList<>(existing.size() + incoming.size());
        int ei = 0, ii = 0;
        while (ei < existing.size() && ii < incoming.size()) {
            long eTs = existing.get(ei).commitTs();
            long iTs = incoming.get(ii).commitTs();
            if (eTs > iTs) {
                result.add(existing.get(ei++));
            } else if (eTs < iTs) {
                result.add(incoming.get(ii++));
            } else {
                result.add(incoming.get(ii++));
                ei++;
            }
        }
        while (ei < existing.size()) {
            result.add(existing.get(ei++));
        }
        while (ii < incoming.size()) {
            result.add(incoming.get(ii++));
        }
        return result;
    }

    /**
     * Get the version visible at the given snapshot timestamp.
     *
     * @param tableId table identifier
     * @param key primary key bytes
     * @param snapshotTs snapshot timestamp
     * @return the version entry with commitTs &le; snapshotTs, or empty if none
     */
    public Optional<VersionChainCodec.VersionEntry> getAtSnapshot(
            String tableId, byte[] key, long snapshotTs) {
        List<VersionChainCodec.VersionEntry> chain = readChain(tableId, key);
        if (chain.isEmpty()) {
            return Optional.empty();
        }
        return VersionChainCodec.floorSeek(chain, snapshotTs);
    }

    /**
     * Get the latest version (highest commitTs).
     *
     * @param tableId table identifier
     * @param key primary key bytes
     * @return the latest version entry, or empty if no versions exist
     */
    public Optional<VersionChainCodec.VersionEntry> latest(String tableId, byte[] key) {
        List<VersionChainCodec.VersionEntry> chain = readChain(tableId, key);
        if (chain.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(chain.get(0));
    }

    /**
     * Scan a key range and return the version visible at the given snapshot timestamp for each key
     * in the range.
     *
     * <p>Uses LMDB cursor with {@link KeyRange#closed(ByteBuffer, ByteBuffer)} to efficiently scan
     * only keys within {@code [startKey, endKey]} for the specified {@code tableId}. For each key,
     * applies {@link VersionChainCodec#floorSeek} to find the version with {@code commitTs <=
     * snapshotTs}.
     *
     * <p>Composite key format: {@code [tableIdLen:4B][tableId_bytes:N][pk_bytes:N]}. The range scan
     * constructs start/end composite keys by prefixing the tableId bytes, ensuring only keys
     * belonging to the specified table are scanned.
     *
     * <p>When {@code snapshotTs <= 0}, returns the latest committed version for each key
     * (READ_COMMITTED semantics).
     *
     * @param tableId table identifier
     * @param startKey start primary key (inclusive, unsigned byte comparison)
     * @param endKey end primary key (inclusive, unsigned byte comparison)
     * @param snapshotTs snapshot timestamp; only versions with commitTs ≤ snapshotTs are visible
     * @return list of (pk, visibleVersion) pairs for keys in range that have a visible version
     * @throws IllegalArgumentException if tableId, startKey, or endKey is null
     */
    public List<RangeScanEntry> scanRangeAtSnapshot(
            String tableId, byte[] startKey, byte[] endKey, long snapshotTs) {
        if (tableId == null) throw new IllegalArgumentException("tableId must not be null");
        if (startKey == null) throw new IllegalArgumentException("startKey must not be null");
        if (endKey == null) throw new IllegalArgumentException("endKey must not be null");

        byte[] startComposite = buildCompositeKey(tableId, startKey);
        byte[] endComposite = buildCompositeKey(tableId, endKey);

        List<RangeScanEntry> results = new ArrayList<>();
        long scannedKeys = 0L;
        long visibleKeys = 0L;

        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer startBuf = allocateKeyBuffer(startComposite);
            ByteBuffer endBuf = allocateKeyBuffer(endComposite);
            KeyRange<ByteBuffer> range = KeyRange.closed(startBuf, endBuf);

            try (CursorIterable<ByteBuffer> cursor = dbi.iterate(txn, range)) {
                for (CursorIterable.KeyVal<ByteBuffer> kv : cursor) {
                    scannedKeys++;

                    ByteBuffer keyBuf = kv.key();
                    byte[] compositeKey = new byte[keyBuf.remaining()];
                    keyBuf.get(compositeKey);

                    // Split composite key to extract pk (skip tableId prefix)
                    KeyPair kp;
                    try {
                        kp = splitCompositeKey(compositeKey);
                    } catch (RuntimeException e) {
                        log.warn("scanRangeAtSnapshot: malformed composite key, skipping", e);
                        continue;
                    }

                    ByteBuffer valBuf = kv.val();
                    byte[] data = new byte[valBuf.remaining()];
                    valBuf.get(data);

                    List<VersionChainCodec.VersionEntry> chain = VersionChainCodec.decode(data);
                    if (chain.isEmpty()) {
                        continue;
                    }

                    Optional<VersionChainCodec.VersionEntry> visible;
                    if (snapshotTs <= 0) {
                        visible = Optional.of(chain.get(0));
                    } else {
                        visible = VersionChainCodec.floorSeek(chain, snapshotTs);
                    }

                    if (visible.isPresent()) {
                        VersionChainCodec.VersionEntry entry = visible.get();
                        results.add(new RangeScanEntry(kp.pk(), entry.value(), entry.commitTs()));
                        visibleKeys++;
                    }
                }
            }
        }

        log.debug(
                "scanRangeAtSnapshot: tableId={} range=[{}..{}] snapshotTs={} scanned={} visible={}",
                tableId,
                bytesToHex(startKey),
                bytesToHex(endKey),
                snapshotTs,
                scannedKeys,
                visibleKeys);
        return results;
    }

    /**
     * A single entry returned by {@link #scanRangeAtSnapshot}.
     *
     * @param pk primary key bytes
     * @param value visible value bytes
     * @param commitTs commit timestamp of the visible version
     */
    public record RangeScanEntry(byte[] pk, byte[] value, long commitTs) {

        public RangeScanEntry {
            if (pk == null) throw new IllegalArgumentException("pk must not be null");
            if (value == null) throw new IllegalArgumentException("value must not be null");
        }
    }

    private static String bytesToHex(byte[] bytes) {
        if (bytes == null) return "null";
        int limit = Math.min(bytes.length, 16);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < limit; i++) {
            sb.append(String.format("%02x", bytes[i]));
        }
        if (bytes.length > limit) sb.append("..");
        return sb.toString();
    }

    /**
     * Garbage collect version chains.
     *
     * <p>B2 fix: Two-phase approach to avoid nested write txns inside read cursor. Phase 1 reads
     * all entries via cursor in a read txn, collecting (compositeKey, encoded) pairs. Phase 2
     * closes the read txn, then opens per-batch write txns to persist pruned chains.
     *
     * @param maxVersions maximum number of versions to retain per key
     * @param minCommitTs minimum commitTs to retain (versions with lower commitTs are removed)
     */
    public void gc(int maxVersions, long minCommitTs) {
        int totalKeys = 0;
        long startTime = System.currentTimeMillis();

        // Phase 1: Collect all pruned entries under a read transaction
        record GcEntry(byte[] compositeKey, byte[] encoded) {}
        List<GcEntry> prunedEntries = new ArrayList<>();

        try (Txn<ByteBuffer> readTxn = env.txnRead()) {
            var cursor = dbi.iterate(readTxn);
            for (var kv : cursor) {
                totalKeys++;
                ByteBuffer keyBuf = kv.key();
                byte[] compositeKey = new byte[keyBuf.remaining()];
                keyBuf.get(compositeKey);

                ByteBuffer valBuf = kv.val();
                byte[] data = new byte[valBuf.remaining()];
                valBuf.get(data);

                List<VersionChainCodec.VersionEntry> chain = VersionChainCodec.decode(data);
                if (chain.isEmpty()) {
                    continue;
                }

                int originalSize = chain.size();

                // Prune by max versions first
                chain = VersionChainCodec.pruneByVersions(chain, maxVersions);

                // Prune by age
                chain = VersionChainCodec.pruneByAge(chain, minCommitTs);

                if (chain.size() < originalSize) {
                    byte[] encoded = VersionChainCodec.encode(chain);
                    prunedEntries.add(new GcEntry(compositeKey, encoded));
                }
            }
        }
        // read transaction closed here

        // Phase 2: Write back pruned entries in batches
        // FIX-1: Re-read current chain inside write txn to avoid lost-update race
        //         with concurrent put() between Phase 1 read and Phase 2 write.
        // FIX-3: try-catch per batch so MAP_FULL in one batch doesn't skip remaining.
        int prunedCount = prunedEntries.size();
        if (!prunedEntries.isEmpty()) {
            int totalBatches = (prunedEntries.size() + GC_BATCH_SIZE - 1) / GC_BATCH_SIZE;
            int batchCount = 0;
            for (int i = 0; i < prunedEntries.size(); i += GC_BATCH_SIZE) {
                int end = Math.min(i + GC_BATCH_SIZE, prunedEntries.size());
                try {
                    try (Txn<ByteBuffer> writeTxn = env.txnWrite()) {
                        for (int j = i; j < end; j++) {
                            GcEntry entry = prunedEntries.get(j);

                            // FIX-1: Re-read current chain inside write txn
                            ByteBuffer reReadKeyBuf = allocateKeyBuffer(entry.compositeKey());
                            ByteBuffer currentValBuf = dbi.get(writeTxn, reReadKeyBuf);
                            byte[] encodedToWrite = entry.encoded();

                            if (currentValBuf != null) {
                                byte[] currentData = new byte[currentValBuf.remaining()];
                                currentValBuf.get(currentData);
                                List<VersionChainCodec.VersionEntry> currentChain =
                                        VersionChainCodec.decode(currentData);
                                List<VersionChainCodec.VersionEntry> phase1Chain =
                                        VersionChainCodec.decode(entry.encoded());

                                // If chain grew (new versions added since Phase 1), re-prune
                                if (currentChain.size() > phase1Chain.size()) {
                                    List<VersionChainCodec.VersionEntry> rePruned;
                                    rePruned =
                                            VersionChainCodec.pruneByVersions(
                                                    currentChain, maxVersions);
                                    rePruned = VersionChainCodec.pruneByAge(rePruned, minCommitTs);
                                    encodedToWrite = VersionChainCodec.encode(rePruned);
                                }
                            }

                            ByteBuffer writeKeyBuf = allocateKeyBuffer(entry.compositeKey());
                            ByteBuffer writeValBuf = allocateValueBuffer(encodedToWrite);
                            dbi.put(writeTxn, writeKeyBuf, writeValBuf);
                        }
                        writeTxn.commit();
                        batchCount++;
                    }
                    log.debug(
                            "gc batch {}/{}: wrote {} entries", batchCount, totalBatches, end - i);
                } catch (RuntimeException e) {
                    // FIX-3: don't let one batch failure skip all remaining batches
                    log.error(
                            "gc batch {}/{} failed (MAP_FULL or LMDB error), skipping: {}",
                            batchCount + 1,
                            totalBatches,
                            e.getMessage());
                }
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        log.info(
                "gc completed: totalKeys={} prunedKeys={} maxVersions={} minCommitTs={} elapsedMs={}",
                totalKeys,
                prunedCount,
                maxVersions,
                minCommitTs,
                elapsed);
    }

    /**
     * Remove the version chain for the specified key.
     *
     * @param tableId table identifier
     * @param key primary key bytes
     */
    public void remove(String tableId, byte[] key) {
        byte[] compositeKey = buildCompositeKey(tableId, key);
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            ByteBuffer keyBuf = allocateKeyBuffer(compositeKey);
            dbi.delete(txn, keyBuf);
            txn.commit();
            log.debug("removed version chain: tableId={} keyLen={}", tableId, key.length);
        }
    }

    // ---- Introspection (FIX-6: for testability) ----

    /**
     * Count the number of keys in the version chain store.
     *
     * @return total number of version chain entries
     */
    public long keyCount() {
        long count = 0;
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            var cursor = dbi.iterate(txn);
            for (var ignored : cursor) {
                count++;
            }
        }
        return count;
    }

    /**
     * Estimate the required LMDB map size based on expected data volume.
     *
     * @param expectedVersionsPerKey expected number of versions retained per key
     * @param expectedKeys estimated number of distinct keys
     * @param avgValueSize average size of a version value in bytes
     * @return recommended map size in bytes (with 50% overhead)
     */
    public static long estimateMapSize(
            int expectedVersionsPerKey, long expectedKeys, int avgValueSize) {
        long keyOverhead = 50L + expectedKeys * 50L;
        long valueSize = expectedKeys * (long) expectedVersionsPerKey * avgValueSize;
        return (long) ((keyOverhead + valueSize) * 1.5);
    }

    @Override
    public void close() {
        // Sync LMDB environment before closing
        try {
            if (env != null) {
                env.sync(true);
            }
        } catch (Exception e) {
            log.warn("Error syncing version chain env", e);
        }
        try {
            if (dbi != null) {
                dbi.close();
            }
        } catch (Exception e) {
            log.warn("Error closing version chain DBI", e);
        }
        try {
            if (env != null) {
                env.close();
            }
        } catch (Exception e) {
            log.warn("Error closing version chain env", e);
        }

        // FIX-4: Only clear this instance's direct buffers and ThreadLocals.
        directBuffers.clear();
        keyBuffer.remove();
        valueBuffer.remove();

        log.info("LmdbVersionChainStore closed: {}", storePath);
    }

    // ---- Buffer management (FIX-4: instance ThreadLocals to avoid cross-instance interference)
    // ----

    private final ThreadLocal<ByteBuffer> keyBuffer =
            ThreadLocal.withInitial(
                    () -> {
                        ByteBuffer buf = ByteBuffer.allocateDirect(256);
                        directBuffers.add(buf);
                        return buf;
                    });

    private final ThreadLocal<ByteBuffer> valueBuffer =
            ThreadLocal.withInitial(
                    () -> {
                        ByteBuffer buf = ByteBuffer.allocateDirect(1024);
                        directBuffers.add(buf);
                        return buf;
                    });

    private ByteBuffer allocateKeyBuffer(byte[] data) {
        ByteBuffer buf = keyBuffer.get();
        if (buf.capacity() < data.length) {
            buf = ByteBuffer.allocateDirect(data.length);
            directBuffers.add(buf);
            keyBuffer.set(buf);
        }
        buf.clear();
        buf.limit(data.length);
        buf.put(data).flip();
        return buf;
    }

    private ByteBuffer allocateValueBuffer(byte[] data) {
        ByteBuffer buf = valueBuffer.get();
        if (buf.capacity() < data.length) {
            buf = ByteBuffer.allocateDirect(data.length);
            directBuffers.add(buf);
            valueBuffer.set(buf);
        }
        buf.clear();
        buf.limit(data.length);
        buf.put(data).flip();
        return buf;
    }
}
