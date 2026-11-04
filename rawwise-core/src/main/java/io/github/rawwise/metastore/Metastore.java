package io.github.rawwise.metastore;

import io.github.rawwise.common.collection.iterator.CloseableIterator;
import io.github.rawwise.engine.storage.KeyValueStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public interface Metastore extends KeyValueStore {

    CloseableIterator<KeyValue> scanRange(byte[] startInclusive, byte[] endExclusive)
            throws IOException;

    CloseableIterator<KeyValue> scanRange(byte[] startInclusive, byte[] endExclusive, int limit)
            throws IOException;

    default List<KeyValue> scanRangeAll(byte[] startInclusive, byte[] endExclusive)
            throws IOException {
        try (CloseableIterator<KeyValue> it = scanRange(startInclusive, endExclusive)) {
            List<KeyValue> result = new ArrayList<>();
            while (it.hasNext()) result.add(it.next());
            return result;
        }
    }

    default boolean supportsCrossKeyCas() {
        return true;
    }

    default int maxCasBatchSize() {
        return -1;
    }

    default long incrementAndGet(byte[] counterKey, long delta) throws IOException {
        throw new UnsupportedOperationException("incrementAndGet not supported");
    }

    default void flush() throws IOException {}

    @Override
    void close() throws IOException;
}
