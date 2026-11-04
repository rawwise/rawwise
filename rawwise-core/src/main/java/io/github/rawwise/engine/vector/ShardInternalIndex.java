package io.github.rawwise.engine.vector;

import java.util.List;

public sealed interface ShardInternalIndex extends AutoCloseable
        permits JVectorDiskIndex, IvfPqIndex, RemoteShardIndex {

    void initialize(List<float[]> vectors, List<String> keys);

    void insert(String entryKey, float[] vector);

    List<SearchCandidate> search(float[] query, int topK);

    void updateLocation(String entryKey, VectorLocation location, String storageRef);

    VectorLocation getLocation(String entryKey);

    String getStorageRef(String entryKey);

    void markTombstone(String entryKey);

    int vectorCount();

    boolean isInitialized();

    float[] getVector(String entryKey);

    java.util.Set<String> entryKeys();

    @Override
    default void close() {}
}
