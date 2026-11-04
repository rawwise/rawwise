package io.github.rawwise.engine.storage.cache;

import io.github.rawwise.model.record.ComparableByteArray;

public record CacheKey(String tableId, ComparableByteArray key) {
    public CacheKey {
        if (tableId == null || tableId.isBlank()) {
            throw new IllegalArgumentException("tableId must not be blank");
        }
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
    }

    public static CacheKey of(String tableId, byte[] key) {
        return new CacheKey(tableId, ComparableByteArray.wrap(key.clone()));
    }
}
