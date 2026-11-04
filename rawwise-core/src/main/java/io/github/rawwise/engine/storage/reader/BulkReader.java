package io.github.rawwise.engine.storage.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public interface BulkReader<T> extends Reader {
    void forEach(RecordPredicate<T> predicate) throws IOException;

    default List<T> readBatch(int maxRecords) throws IOException {
        if (maxRecords <= 0) {
            throw new IllegalArgumentException("maxRecords must be positive, got: " + maxRecords);
        }
        List<T> batch = new ArrayList<>(Math.min(maxRecords, 1024));
        forEach(
                t -> {
                    batch.add(t);
                    return batch.size() < maxRecords;
                });
        return batch;
    }

    default List<T> readAll() throws IOException {
        return readBatch(Integer.MAX_VALUE);
    }

    long estimatedSize();

    @FunctionalInterface
    interface RecordPredicate<T> {
        boolean test(T record) throws IOException;
    }
}
