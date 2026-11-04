package io.github.rawwise.engine.storage.mvcc;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Binary codec for MVCC version chains.
 *
 * <p>Format: {@code [version_count:4B][commitTs:8B][valueLen:4B][value:N]...} repeated for each
 * version. Versions stored in DESCENDING commitTs order (newest first).
 *
 * <p>All encoding/decoding uses {@link ByteBuffer} for zero-copy potential. This codec is stateless
 * and thread-safe.
 */
public final class VersionChainCodec {

    // Binary format field sizes
    private static final int VERSION_COUNT_SIZE = 4; // int: number of versions
    private static final int COMMIT_TS_SIZE = 8; // long: commit timestamp
    private static final int VALUE_LEN_SIZE = 4; // int: value length
    private static final int PER_VERSION_HEADER =
            COMMIT_TS_SIZE + VALUE_LEN_SIZE; // 12 bytes per version

    private VersionChainCodec() {
        // utility class
    }

    /** A single version entry with commit timestamp and value. */
    public record VersionEntry(long commitTs, byte[] value) {
        public VersionEntry {
            if (commitTs <= 0) {
                throw new IllegalArgumentException("commitTs must be positive, got: " + commitTs);
            }
            if (value == null) {
                throw new IllegalArgumentException("value must not be null");
            }
            // defensive copy
            value = value.clone();
        }

        /** Returns a defensive copy of the value. */
        public byte[] valueCopy() {
            return value.clone();
        }
    }

    /**
     * Encode a list of version entries into a binary blob.
     *
     * @param entries list of version entries in descending commitTs order
     * @return binary representation
     */
    public static byte[] encode(List<VersionEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return new byte[] {0, 0, 0, 0}; // just version_count = 0
        }

        int totalSize = VERSION_COUNT_SIZE;
        for (VersionEntry e : entries) {
            totalSize += PER_VERSION_HEADER + e.value().length;
        }

        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.putInt(entries.size());
        for (VersionEntry e : entries) {
            buf.putLong(e.commitTs());
            byte[] v = e.value();
            buf.putInt(v.length);
            buf.put(v);
        }
        return buf.array();
    }

    /**
     * Decode a binary blob into a list of version entries.
     *
     * @param data binary representation
     * @return list of version entries in descending commitTs order (newest first)
     */
    public static List<VersionEntry> decode(byte[] data) {
        if (data == null || data.length < VERSION_COUNT_SIZE) {
            return List.of();
        }

        ByteBuffer buf = ByteBuffer.wrap(data);
        int count = buf.getInt();
        if (count <= 0) {
            return List.of();
        }

        List<VersionEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            if (buf.remaining() < PER_VERSION_HEADER) {
                break; // malformed data, stop gracefully
            }
            long commitTs = buf.getLong();
            int valueLen = buf.getInt();
            if (valueLen < 0 || buf.remaining() < valueLen) {
                break; // malformed data
            }
            byte[] value = new byte[valueLen];
            buf.get(value);
            entries.add(new VersionEntry(commitTs, value));
        }
        return entries;
    }

    /**
     * Floor seek: find the version with the largest commitTs &le; snapshotTs. Assumes the chain is
     * in descending commitTs order (newest first).
     *
     * @param chain version entries in descending order
     * @param snapshotTs snapshot timestamp
     * @return the matching version entry, or empty if none visible
     */
    public static Optional<VersionEntry> floorSeek(List<VersionEntry> chain, long snapshotTs) {
        if (chain == null || chain.isEmpty()) {
            return Optional.empty();
        }
        // FIX-5: O(log n) binary search instead of O(n) linear scan.
        // Entries are in descending commitTs order (newest first).
        // Find the first entry with commitTs <= snapshotTs.
        int lo = 0, hi = chain.size() - 1;
        int result = chain.size(); // default: not found
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            long ts = chain.get(mid).commitTs();
            if (ts <= snapshotTs) {
                result = mid;
                hi = mid - 1; // look for larger ts (newer but still <= snapshotTs)
            } else {
                lo = mid + 1;
            }
        }
        return result < chain.size() ? Optional.of(chain.get(result)) : Optional.empty();
    }

    /**
     * Prune version chain to keep at most {@code maxVersions} entries. Keeps the newest versions
     * (already at the front since chain is descending).
     *
     * @param chain version entries in descending order
     * @param maxVersions maximum versions to retain
     * @return pruned list (may be the same instance if no change needed)
     */
    public static List<VersionEntry> pruneByVersions(List<VersionEntry> chain, int maxVersions) {
        if (chain == null || chain.size() <= maxVersions) {
            return chain;
        }
        return new ArrayList<>(chain.subList(0, maxVersions));
    }

    /**
     * Prune version chain: remove versions with commitTs &lt; minCommitTs.
     *
     * @param chain version entries in descending order
     * @param minCommitTs minimum commitTs to retain (exclusive: versions &lt; minCommitTs are
     *     removed)
     * @return pruned list (may be the same instance if no change needed)
     */
    public static List<VersionEntry> pruneByAge(List<VersionEntry> chain, long minCommitTs) {
        if (chain == null || chain.isEmpty()) {
            return chain;
        }
        // Since chain is descending, once we hit a version < minCommitTs,
        // all remaining versions are also < minCommitTs
        int cutoffIdx = chain.size();
        for (int i = chain.size() - 1; i >= 0; i--) {
            if (chain.get(i).commitTs() >= minCommitTs) {
                cutoffIdx = i + 1;
                break;
            }
        }
        if (cutoffIdx >= chain.size()) {
            return chain;
        }
        return new ArrayList<>(chain.subList(0, cutoffIdx));
    }

    /** Compute total binary size of a version chain (for capacity estimation). */
    public static int computeEncodedSize(List<VersionEntry> entries) {
        int size = VERSION_COUNT_SIZE;
        for (VersionEntry e : entries) {
            size += PER_VERSION_HEADER + e.value().length;
        }
        return size;
    }
}
