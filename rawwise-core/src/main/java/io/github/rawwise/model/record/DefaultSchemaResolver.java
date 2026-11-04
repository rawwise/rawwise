package io.github.rawwise.model.record;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class DefaultSchemaResolver implements RecordCodec.SchemaResolver {
    private final Cache<CacheKey, Schema> versionCache;
    private final RecordCodec.SchemaResolver delegate;

    public DefaultSchemaResolver(RecordCodec.SchemaResolver delegate) {
        this(delegate, 256);
    }

    public DefaultSchemaResolver(RecordCodec.SchemaResolver delegate, int maxSize) {
        this.delegate = delegate;
        this.versionCache = Caffeine.newBuilder().maximumSize(maxSize).build();
    }

    @Override
    public Schema resolve(long tableId, long schemaVersion) {
        CacheKey key = new CacheKey(tableId, schemaVersion);
        return versionCache.get(key, k -> delegate.resolve(k.tableId, k.schemaVersion));
    }

    record CacheKey(long tableId, long schemaVersion) {
        // Java record auto-generates correct equals() and hash Code() for long fields
    }
}
