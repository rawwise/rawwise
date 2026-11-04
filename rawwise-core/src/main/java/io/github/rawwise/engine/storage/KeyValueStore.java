package io.github.rawwise.engine.storage;

import io.github.rawwise.common.collection.iterator.CloseableIterator;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public interface KeyValueStore extends Closeable {

    Optional<byte[]> get(byte[] key) throws IOException;

    void put(byte[] key, byte[] value) throws IOException;

    void delete(byte[] key) throws IOException;

    boolean exists(byte[] key) throws IOException;

    CloseableIterator<KeyValue> scan(byte[] prefix) throws IOException;

    CloseableIterator<KeyValue> scan(byte[] prefix, int limit) throws IOException;

    default List<KeyValue> scanAll(byte[] prefix) throws IOException {
        try (CloseableIterator<KeyValue> it = scan(prefix)) {
            List<KeyValue> result = new ArrayList<>();
            while (it.hasNext()) result.add(it.next());
            return result;
        }
    }

    /**
     * Atomically apply mutations iff all preconditions hold. Implementations MUST guarantee
     * atomicity across keys touched by this call. Implementations MAY hold per-key or global locks
     * during the operation.
     */
    boolean compareAndSwap(List<Precondition> preconditions, List<Mutation> mutations)
            throws IOException;

    /**
     * Apply a batch of mutations. Atomicity guarantee is implementation-specific: Metastore
     * guarantees cross-key atomicity; KnowledgeStore implementations may apply mutations
     * sequentially without cross-key atomicity. Callers requiring atomicity MUST use {@link
     * #compareAndSwap(List, List)} instead.
     */
    void batchWrite(List<Mutation> mutations) throws IOException;
}
