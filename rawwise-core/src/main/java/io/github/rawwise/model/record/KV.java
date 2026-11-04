package io.github.rawwise.model.record;

public interface KV<K extends Comparable<K>, V> extends Comparable<KV<K, V>> {

    byte[] key();

    byte[] value();

    default int compare(K key1, K key2) {
        return key1.compareTo(key2);
    }
}
