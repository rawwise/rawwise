package io.github.rawwise.common.hash;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;

public class DefaultConsistentHashRing<T> implements ConsistentHashRing<T> {
    // Hash ring: stores mapping from virtual node hash to physical node
    private final NavigableMap<Long, VirtualNode<T>> hashRing = new ConcurrentSkipListMap<>();
    // Mapping from physical node to virtual nodes
    private final Map<T, Set<VirtualNode<T>>> nodeToVirtualNodes = new HashMap<>();

    // Configuration
    private final int virtualNodesPerNode;
    private final HashFunction hashFunction;
    private final boolean enableWeight;
    private final Map<T, Integer> nodeWeights;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    // Statistics
    @Getter private long totalHits = 0;
    @Getter private long totalMisses = 0;

    public DefaultConsistentHashRing() {
        this(160, false);
    }

    public DefaultConsistentHashRing(int virtualNodesPerNode, boolean enableWeight) {
        this(virtualNodesPerNode, enableWeight, DefaultConsistentHashRing::murmurHash64);
    }

    public DefaultConsistentHashRing(
            int virtualNodesPerNode, boolean enableWeight, HashFunction hashFunction) {
        if (virtualNodesPerNode <= 0) {
            throw new IllegalArgumentException("virtualNodesPerNode must be positive");
        }

        this.virtualNodesPerNode = virtualNodesPerNode;
        this.enableWeight = enableWeight;
        this.hashFunction = Objects.requireNonNull(hashFunction);
        this.nodeWeights = enableWeight ? new HashMap<>() : null;
    }

    private record VirtualNode<T>(
            @Getter T physicalNode, int replicaIndex, @Getter long hash, @Getter int weight) {

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            VirtualNode<?> that = (VirtualNode<?>) o;
            return hash == that.hash && Objects.equals(physicalNode, that.physicalNode);
        }

        @Override
        public int hashCode() {
            return Objects.hash(physicalNode, hash);
        }

        @Override
        public String toString() {
            return String.format(
                    "VirtualNode{node=%s, replica=%d, hash=%d, weight=%d}",
                    physicalNode, replicaIndex, hash, weight);
        }
    }

    @FunctionalInterface
    public interface HashFunction {
        long hash(byte[] data);

        default long hash(String str) {
            return hash(str.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void addNode(T node) {
        addNode(node, 1);
    }

    @Override
    public void removeNode(T node) {
        Objects.requireNonNull(node, "node cannot be null");

        writeLock.lock();
        try {
            Set<VirtualNode<T>> virtualNodes = nodeToVirtualNodes.remove(node);
            if (virtualNodes == null) {
                return; // node does not exist
            }

            // Remove all virtual nodes from hash ring
            for (VirtualNode<T> vnode : virtualNodes) {
                hashRing.remove(vnode.getHash());
            }

            // Remove weight
            if (enableWeight) {
                nodeWeights.remove(node);
            }

        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public T getNode(Object key) {
        Objects.requireNonNull(key, "key cannot be null");

        readLock.lock();
        try {
            if (hashRing.isEmpty()) {
                totalMisses++;
                return null;
            }

            long hash = computeKeyHash(key);
            VirtualNode<T> vnode = findVirtualNode(hash);

            if (vnode != null) {
                totalHits++;
                return vnode.getPhysicalNode();
            } else {
                totalMisses++;
                return null;
            }

        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Map<T, Collection<Object>> getNodes(Collection<?> keys) {
        Objects.requireNonNull(keys, "keys cannot be null");

        readLock.lock();
        try {
            Map<T, Collection<Object>> result = new HashMap<>();

            for (Object key : keys) {
                T node = getNode(key);
                if (node != null) {
                    result.computeIfAbsent(node, k -> new ArrayList<>()).add(key);
                }
            }

            return result;

        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int getNodeCount() {
        readLock.lock();
        try {
            return nodeToVirtualNodes.size();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int getVirtualNodeCount() {
        readLock.lock();
        try {
            return hashRing.size();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Collection<T> getAllNodes() {
        readLock.lock();
        try {
            return new ArrayList<>(nodeToVirtualNodes.keySet());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void rebalance() {
        writeLock.lock();
        try {
            // Backup existing nodes
            List<Map.Entry<T, Integer>> nodes = new ArrayList<>();

            if (enableWeight) {
                nodes.addAll(nodeWeights.entrySet());
            } else {
                nodes.addAll(
                        nodeToVirtualNodes.keySet().stream()
                                .map(node -> Map.entry(node, 1))
                                .toList());
            }

            // Clear hash ring
            clear();

            // Re-add all nodes
            for (Map.Entry<T, Integer> entry : nodes) {
                addNode(entry.getKey(), entry.getValue());
            }

        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void clear() {
        writeLock.lock();
        try {
            hashRing.clear();
            nodeToVirtualNodes.clear();
            if (enableWeight) {
                nodeWeights.clear();
            }
            totalHits = 0;
            totalMisses = 0;

        } finally {
            writeLock.unlock();
        }
    }

    public void addNode(T node, int weight) {
        Objects.requireNonNull(node, "node cannot be null");
        if (weight <= 0) {
            throw new IllegalArgumentException("weight must be positive");
        }

        writeLock.lock();
        try {
            // Check if node already exists
            if (nodeToVirtualNodes.containsKey(node)) {
                throw new IllegalArgumentException("Node already exists: " + node);
            }

            // Store weight
            if (enableWeight) {
                nodeWeights.put(node, weight);
            }

            // Create virtual nodes
            Set<VirtualNode<T>> virtualNodes = new HashSet<>();
            int actualVirtualNodes =
                    enableWeight ? virtualNodesPerNode * weight : virtualNodesPerNode;

            for (int i = 0; i < actualVirtualNodes; i++) {
                VirtualNode<T> vnode = createVirtualNode(node, i);
                virtualNodes.add(vnode);
                hashRing.put(vnode.getHash(), vnode);
            }

            nodeToVirtualNodes.put(node, virtualNodes);

        } finally {
            writeLock.unlock();
        }
    }

    private VirtualNode<T> createVirtualNode(T physicalNode, int replicaIndex) {
        String nodeKey = buildNodeKey(physicalNode, replicaIndex);
        long hash = hashFunction.hash(nodeKey);

        int weight = enableWeight ? nodeWeights.get(physicalNode) : 1;

        return new VirtualNode<>(physicalNode, replicaIndex, hash, weight);
    }

    private String buildNodeKey(T node, int replicaIndex) {
        return node.toString() + "#vnode-" + replicaIndex;
    }

    private static long murmurHash64(byte[] data) {
        return MurmurHash3.hash64(data);
    }

    private VirtualNode<T> findVirtualNode(long keyHash) {
        // If hash ring is empty, return null
        if (hashRing.isEmpty()) {
            return null;
        }

        // Find first virtual node with hash >= keyHash
        Map.Entry<Long, VirtualNode<T>> entry = hashRing.ceilingEntry(keyHash);

        if (entry != null) {
            return entry.getValue();
        } else {
            // Wrap around to first node
            return hashRing.firstEntry().getValue();
        }
    }

    private long computeKeyHash(Object key) {
        if (key instanceof String) {
            return hashFunction.hash((String) key);
        } else if (key instanceof byte[]) {
            return hashFunction.hash((byte[]) key);
        } else {
            return hashFunction.hash(key.toString());
        }
    }
}
