package io.github.rawwise.engine.storage.table;

import io.github.rawwise.common.hash.MurmurHash3;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Key-Based routing table: uses consistent hashing to route writes of different keys to different
 * LmdbInstances.
 *
 * <h3>Core design</h3>
 *
 * <ul>
 *   <li><b>consistent hashing ring</b>: Virtual nodes → LmdbInstanceindex, ensures even key
 *       distribution
 *   <li><b>Immutable design</b>: rebuild() returns new instance, original instance unaffected
 *   <li><b>Versioned</b>: version increments on each routing change, transaction checks version
 *       change on commit
 *   <li><b>MurmurHash3</b>: uses project's existing MurmurHash3.hash64() to compute hash value
 * </ul>
 *
 * <h3>Thread safety</h3>
 *
 * <p>KeyRoutingTable is immutable, all fields are final, no synchronization needed. TableStore
 * switches routing table instances via volatile reference, ensuring thread-safe publication.
 *
 * <h3>Virtual nodes</h3>
 *
 * <p>Each physical node (LmdbInstance) corresponds to 256 virtual nodes, ensuring even key
 * distribution even with few instances. Virtual node hash key format is
 * "{instanceIndex}#vn-{replicaIndex}".
 */
public class KeyRoutingTable {

    /** Default virtual node count, ensures even key distribution */
    public static final int VIRTUAL_NODES = 256;

    /** Routing table version number, increments on each routing change */
    private final long version;

    /** consistent hashing ring: Virtual nodeshash value → LmdbInstanceinactiveInstanceIdsinindex */
    private final NavigableMap<Long, String> hashRing;

    private final List<String> activeInstanceIds;

    public KeyRoutingTable() {
        this(0L, Collections.emptyList());
    }

    private KeyRoutingTable(
            long version, List<String> activeInstanceIds, NavigableMap<Long, String> hashRing) {
        this.version = version;
        this.activeInstanceIds = Collections.unmodifiableList(activeInstanceIds);
        this.hashRing = hashRing;
    }

    private KeyRoutingTable(long version, List<String> activeInstanceIds) {
        this.version = version;
        this.activeInstanceIds = Collections.unmodifiableList(activeInstanceIds);
        this.hashRing = buildHashRing(activeInstanceIds);
    }

    /**
     * Compute the routed LmdbInstance index based on key.
     *
     * <p>Uses MurmurHash3 to compute 64-bit hash of key, then on the consistent hash ring clockwise
     * finds the nearest virtual node, returns the corresponding physical node index.
     *
     * @param key write key byte array
     * @return routed LmdbInstance index in activeInstanceIds
     * @throws IllegalStateException if hash ring is empty (no active instances)
     */
    public String route(byte[] key) {
        if (hashRing.isEmpty()) {
            throw new IllegalStateException(
                    "KeyRoutingTable is empty, no active instances available");
        }
        long hash = hashKey(key);
        Map.Entry<Long, String> entry = hashRing.ceilingEntry(hash);
        if (entry != null) {
            return entry.getValue();
        }
        return hashRing.firstEntry().getValue();
    }

    /**
     * Get current routing table version number.
     *
     * @return version number
     */
    public long version() {
        return version;
    }

    /**
     * Get active instance ID list.
     *
     * @return immutable active instance ID list
     */
    public List<String> activeInstanceIds() {
        return activeInstanceIds;
    }

    /**
     * Whether the current routing table is empty (no active instances).
     *
     * @return true if no active instances
     */
    public boolean isEmpty() {
        return hashRing.isEmpty();
    }

    /**
     * Get total virtual node count in the hash ring.
     *
     * @return virtual node count
     */
    public int virtualNodeCount() {
        return hashRing.size();
    }

    /**
     * Rebuild routing table: create new routing table instance based on new active instance ID
     * list.
     *
     * <p>Immutable design: returns new instance, original instance unaffected. New routing table
     * version is current version + 1.
     *
     * @param newActiveInstanceIds new active instance ID list
     * @return new KeyRoutingTable instance
     */
    public KeyRoutingTable rebuild(List<String> newActiveInstanceIds) {
        return new KeyRoutingTable(this.version + 1, newActiveInstanceIds);
    }

    /**
     * Compute 64-bit hash of key. useMurmurHash3.hash64(), result is non-negative (&
     * 0x7FFFFFFFFFFFFFFFL), ensures hash value is in [0, Long.MAX_VALUE] range, consistent with
     * consistent hash ring key space.
     */
    private static long hashKey(byte[] key) {
        return MurmurHash3.hash64(key) & 0x7FFFFFFFFFFFFFFFL;
    }

    /**
     * Build consistent hash ring. Creates VIRTUAL_NODES virtual nodes for each physical node,
     * Virtual node hash key format is "{instanceIndex}#vn-{replicaIndex}".
     */
    private static NavigableMap<Long, String> buildHashRing(List<String> instanceIds) {
        NavigableMap<Long, String> ring = new TreeMap<>();
        for (String instanceId : instanceIds) {
            for (int replica = 0; replica < VIRTUAL_NODES; replica++) {
                String nodeKey = instanceId + "#vn-" + replica;
                long hash =
                        MurmurHash3.hash64(nodeKey.getBytes(StandardCharsets.UTF_8))
                                & 0x7FFFFFFFFFFFFFFFL;
                ring.put(hash, instanceId);
            }
        }
        return ring;
    }

    @Override
    public String toString() {
        return "KeyRoutingTable{version="
                + version
                + ", instanceCount="
                + activeInstanceIds.size()
                + ", virtualNodes="
                + hashRing.size()
                + "}";
    }
}
