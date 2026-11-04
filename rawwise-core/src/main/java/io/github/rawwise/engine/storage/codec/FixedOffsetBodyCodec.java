package io.github.rawwise.engine.storage.codec;

import io.github.rawwise.model.record.ColumnGroup;
import io.github.rawwise.model.record.ColumnGroupSchemaResolver;
import io.github.rawwise.model.record.FixedOffsetCodec;
import io.github.rawwise.model.record.RawWiseType;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.StructField;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Default {@link BodyCodec} implementation using FIXED_OFFSET encoding.
 *
 * <p>This codec implicitly creates a single ColumnGroup named "_row" containing ALL fields of the
 * schema, using {@link ColumnGroup.Encoding#FIXED_OFFSET}. It delegates to {@link
 * FixedOffsetCodec#INSTANCE} for the actual encode/decode.
 *
 * <h3>Caching</h3>
 *
 * <p>The derived ColumnGroup and its ColumnGroupSchemaResolver are cached per schema version in a
 * {@link ConcurrentHashMap} to avoid recomputation when decoding many rows with the same schema
 * version.
 */
@Slf4j
public final class FixedOffsetBodyCodec implements BodyCodec {

    private static final String COLUMN_GROUP_NAME = "_row";

    private final ConcurrentHashMap<Long, ColumnGroup> cache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ColumnGroupSchemaResolver> resolverCache =
            new ConcurrentHashMap<>();

    @Override
    public String name() {
        return "fixed-offset";
    }

    @Override
    public byte[] encode(Schema schema, Map<String, Object> fields) {
        ColumnGroup entry = resolveOrCache(schema);
        return FixedOffsetCodec.INSTANCE.encode(entry, fields);
    }

    @Override
    public Map<String, Object> decode(Schema schema, byte[] data) {
        ColumnGroup entry = resolveOrCache(schema);
        return FixedOffsetCodec.INSTANCE.decode(entry, data);
    }

    @Override
    public Object decodeField(Schema schema, byte[] data, String fieldName) {
        ColumnGroup entry = resolveOrCache(schema);
        return FixedOffsetCodec.INSTANCE.decodeField(entry, data, fieldName);
    }

    /** Resolve the ColumnGroup for the given schema, using cache when possible. */
    private ColumnGroup resolveOrCache(Schema schema) {
        long ver = schema.version();
        ColumnGroup cg = cache.get(ver);
        if (cg != null) return cg;
        return cache.computeIfAbsent(
                ver,
                v -> {
                    List<String> allColNames = new ArrayList<>();
                    List<RawWiseType> allColTypes = new ArrayList<>();
                    for (StructField field : schema.fields()) {
                        allColNames.add(field.name());
                        allColTypes.add(field.dataType());
                    }
                    ColumnGroup newCg =
                            new ColumnGroup(
                                    COLUMN_GROUP_NAME,
                                    allColNames,
                                    allColTypes,
                                    ColumnGroup.Encoding.FIXED_OFFSET,
                                    (int) schema.version());
                    // Pre-warm the resolver cache so FixedOffsetCodec methods
                    // benefit from the cached ColumnGroupSchemaResolver.
                    resolverCache.computeIfAbsent(
                            ver, v2 -> ColumnGroupSchemaResolver.resolve(newCg));
                    log.debug(
                            "Cached FixedOffset ColumnGroup for schema version {}: {} fields",
                            schema.version(),
                            allColNames.size());
                    return newCg;
                });
    }
}
