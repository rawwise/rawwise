package io.github.rawwise.engine.storage.block.read;

import java.util.LinkedHashMap;
import java.util.Map;

final class BoundedLRUCache<K, V> {

    private final int maxSize;
    private final Map<K, V> delegate;

    BoundedLRUCache(int maxSize) {
        this.maxSize = maxSize;
        this.delegate =
                new LinkedHashMap<>(16, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                        return size() > BoundedLRUCache.this.maxSize;
                    }
                };
    }

    synchronized V computeIfAbsent(
            K key, java.util.function.Function<? super K, ? extends V> mappingFunction) {
        return delegate.computeIfAbsent(key, mappingFunction);
    }

    synchronized V get(K key) {
        return delegate.get(key);
    }

    synchronized void clear() {
        delegate.clear();
    }

    synchronized int size() {
        return delegate.size();
    }
}
