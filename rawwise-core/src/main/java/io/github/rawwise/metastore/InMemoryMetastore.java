package io.github.rawwise.metastore;

import io.github.rawwise.common.collection.iterator.CloseableIterator;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Metastore memoryimplementation.
 *
 * <p><b>note</b>:{@code synchronized} CAS serializes CAS operations on different keys,not suitable
 * for multi-table high-concurrency production workloads. only for unit testing and local
 * development scenarios.
 */
public class InMemoryMetastore implements Metastore {

    private final ConcurrentSkipListMap<ByteArrayKey, byte[]> store = new ConcurrentSkipListMap<>();

    @Override
    public Optional<byte[]> get(byte[] key) throws IOException {
        return Optional.ofNullable(store.get(new ByteArrayKey(key)));
    }

    @Override
    public synchronized void put(byte[] key, byte[] value) throws IOException {
        store.put(new ByteArrayKey(key), value);
    }

    @Override
    public synchronized void delete(byte[] key) throws IOException {
        store.remove(new ByteArrayKey(key));
    }

    @Override
    public boolean exists(byte[] key) throws IOException {
        return store.containsKey(new ByteArrayKey(key));
    }

    @Override
    public CloseableIterator<KeyValue> scan(byte[] prefix) throws IOException {
        ByteArrayKey prefixKey = new ByteArrayKey(prefix);
        Iterator<Map.Entry<ByteArrayKey, byte[]>> entries =
                store.tailMap(prefixKey).entrySet().iterator();
        return new FilteringIterator(entries, prefix, -1);
    }

    @Override
    public CloseableIterator<KeyValue> scan(byte[] prefix, int limit) throws IOException {
        ByteArrayKey prefixKey = new ByteArrayKey(prefix);
        Iterator<Map.Entry<ByteArrayKey, byte[]>> entries =
                store.tailMap(prefixKey).entrySet().iterator();
        return new FilteringIterator(entries, prefix, limit);
    }

    @Override
    public CloseableIterator<KeyValue> scanRange(byte[] startInclusive, byte[] endExclusive)
            throws IOException {
        Iterator<Map.Entry<ByteArrayKey, byte[]>> entries =
                store.subMap(new ByteArrayKey(startInclusive), new ByteArrayKey(endExclusive))
                        .entrySet()
                        .iterator();
        return new RangeIterator(entries, -1);
    }

    @Override
    public CloseableIterator<KeyValue> scanRange(
            byte[] startInclusive, byte[] endExclusive, int limit) throws IOException {
        Iterator<Map.Entry<ByteArrayKey, byte[]>> entries =
                store.subMap(new ByteArrayKey(startInclusive), new ByteArrayKey(endExclusive))
                        .entrySet()
                        .iterator();
        return new RangeIterator(entries, limit);
    }

    @Override
    public synchronized boolean compareAndSwap(
            List<Precondition> preconditions, List<Mutation> mutations) throws IOException {
        for (Precondition pc : preconditions) {
            byte[] key = resolveKey(pc);
            switch (pc) {
                case Precondition.Exists e -> {
                    if (!store.containsKey(new ByteArrayKey(key))) return false;
                }
                case Precondition.NotExists ne -> {
                    if (store.containsKey(new ByteArrayKey(key))) return false;
                }
                case Precondition.ValueEquals ve -> {
                    byte[] current = store.get(new ByteArrayKey(key));
                    if (!Arrays.equals(current, ve.expected())) return false;
                }
            }
        }
        for (Mutation m : mutations) {
            switch (m) {
                case Mutation.Put p -> store.put(new ByteArrayKey(p.key()), p.value());
                case Mutation.Delete d -> store.remove(new ByteArrayKey(d.key()));
            }
        }
        return true;
    }

    @Override
    public synchronized void batchWrite(List<Mutation> mutations) throws IOException {
        for (Mutation m : mutations) {
            switch (m) {
                case Mutation.Put p -> store.put(new ByteArrayKey(p.key()), p.value());
                case Mutation.Delete d -> store.remove(new ByteArrayKey(d.key()));
            }
        }
    }

    @Override
    public void close() throws IOException {
        store.clear();
    }

    @Override
    public boolean supportsCrossKeyCas() {
        return true;
    }

    private static byte[] resolveKey(Precondition pc) {
        return switch (pc) {
            case Precondition.Exists e -> e.key();
            case Precondition.NotExists ne -> ne.key();
            case Precondition.ValueEquals ve -> ve.key();
        };
    }

    private static final class ByteArrayKey implements Comparable<ByteArrayKey> {
        private final byte[] data;

        ByteArrayKey(byte[] data) {
            this.data = data;
        }

        @Override
        public int compareTo(ByteArrayKey o) {
            return Arrays.compareUnsigned(data, o.data);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof ByteArrayKey that)) return false;
            return Arrays.equals(data, that.data);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }
    }

    private static final class FilteringIterator implements CloseableIterator<KeyValue> {
        private final Iterator<Map.Entry<ByteArrayKey, byte[]>> inner;
        private final byte[] prefix;
        private final int limit;
        private int count;
        private Map.Entry<ByteArrayKey, byte[]> next;

        FilteringIterator(
                Iterator<Map.Entry<ByteArrayKey, byte[]>> inner, byte[] prefix, int limit) {
            this.inner = inner;
            this.prefix = prefix;
            this.limit = limit;
        }

        @Override
        public boolean hasNext() {
            if (limit > 0 && count >= limit) return false;
            while (inner.hasNext()) {
                Map.Entry<ByteArrayKey, byte[]> e = inner.next();
                if (startsWith(e.getKey().data, prefix)) {
                    next = e;
                    return true;
                }
            }
            return false;
        }

        @Override
        public KeyValue next() {
            if (next == null && !hasNext()) throw new NoSuchElementException();
            count++;
            Map.Entry<ByteArrayKey, byte[]> e = next;
            next = null;
            return new KeyValue(e.getKey().data, e.getValue());
        }

        @Override
        public void close() {}

        private static boolean startsWith(byte[] data, byte[] prefix) {
            if (data.length < prefix.length) return false;
            for (int i = 0; i < prefix.length; i++) if (data[i] != prefix[i]) return false;
            return true;
        }
    }

    private static final class RangeIterator implements CloseableIterator<KeyValue> {
        private final Iterator<Map.Entry<ByteArrayKey, byte[]>> inner;
        private final int limit;
        private int count;

        RangeIterator(Iterator<Map.Entry<ByteArrayKey, byte[]>> inner, int limit) {
            this.inner = inner;
            this.limit = limit;
        }

        @Override
        public boolean hasNext() {
            if (limit > 0 && count >= limit) return false;
            return inner.hasNext();
        }

        @Override
        public KeyValue next() {
            if (!hasNext()) throw new NoSuchElementException();
            count++;
            Map.Entry<ByteArrayKey, byte[]> e = inner.next();
            return new KeyValue(e.getKey().data, e.getValue());
        }

        @Override
        public void close() {}
    }
}
