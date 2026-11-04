package io.github.rawwise.common.collection.cache;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SortedCache<K extends Comparable<K>, V> {
    private final TreeMap<K, V> cache = new TreeMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final int capacity;

    public SortedCache(int capacity) {
        this.capacity = capacity;
    }

    public void put(K key, V value) {
        lock.writeLock().lock();
        try {
            cache.put(key, value);

            if (cache.size() > capacity) {
                cache.remove(cache.firstKey());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public V get(K key) {
        lock.readLock().lock();
        try {
            return cache.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public V remove(K key) {
        lock.writeLock().lock();
        try {
            return cache.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean containsKey(K key) {
        lock.readLock().lock();
        try {
            return cache.containsKey(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return cache.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            cache.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Map<K, V> getAll() {
        lock.readLock().lock();
        try {
            return new TreeMap<>(cache);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            return cache.toString();
        } finally {
            lock.readLock().unlock();
        }
    }
}
