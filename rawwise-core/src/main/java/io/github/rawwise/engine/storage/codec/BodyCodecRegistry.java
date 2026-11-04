package io.github.rawwise.engine.storage.codec;

import io.github.rawwise.model.record.Schema;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Global registry for {@link BodyCodec} implementations.
 *
 * <p>Codecs are registered by name and instantiated lazily. Instance caching by {@code (codecName,
 * schemaVersion)} avoids repeated construction when the same table schema version is decoded many
 * times (e.g., scanning a large block).
 *
 * <p>Built-in codecs ("fixed-offset", "tlv") are registered in a static initializer. Additional
 * codecs can be registered at runtime via {@link #register}.
 *
 * <h3>Thread Safety</h3>
 *
 * <p>All maps are {@link ConcurrentHashMap}; registration and instance creation are fully
 * lock-free. Instance creation is idempotent (computeIfAbsent).
 */
@Slf4j
public final class BodyCodecRegistry {

    private static final ConcurrentHashMap<String, Factory> FACTORIES = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, BodyCodec> INSTANCE_CACHE =
            new ConcurrentHashMap<>();

    static {
        register("fixed-offset", (schema) -> new FixedOffsetBodyCodec());
        register("tlv", (schema) -> new TlvBodyCodec());
    }

    private BodyCodecRegistry() {}

    /**
     * Register a codec factory. Overwrites any existing registration for the same name.
     *
     * @param name codec name (must match {@code body.codec} property value)
     * @param factory factory that creates BodyCodec instances
     */
    public static void register(String name, Factory factory) {
        FACTORIES.put(name, factory);
        log.info("Registered BodyCodec: {}", name);
    }

    /**
     * Create or retrieve a cached {@link BodyCodec} instance for the given codec name and schema
     * version.
     *
     * @param name codec name (e.g. "fixed-offset", "tlv")
     * @param schema the resolved schema (used for codec initialization)
     * @return the BodyCodec instance
     * @throws IllegalArgumentException if no codec is registered for the given name
     */
    public static BodyCodec create(String name, Schema schema) {
        String cacheKey = name + "@" + schema.version();
        return INSTANCE_CACHE.computeIfAbsent(
                cacheKey,
                k -> {
                    Factory factory = FACTORIES.get(name);
                    if (factory == null) {
                        throw new IllegalArgumentException(
                                "Unknown BodyCodec: "
                                        + name
                                        + ". Registered codecs: "
                                        + FACTORIES.keySet());
                    }
                    BodyCodec codec = factory.create(schema);
                    log.debug(
                            "Created BodyCodec instance: key={}, class={}",
                            cacheKey,
                            codec.getClass().getSimpleName());
                    return codec;
                });
    }

    /** Check whether a codec with the given name is registered. */
    public static boolean isRegistered(String name) {
        return FACTORIES.containsKey(name);
    }

    /** Remove a cached instance. Useful when schema version is stale and needs refresh. */
    public static void evict(String name, long schemaVersion) {
        String cacheKey = name + "@" + schemaVersion;
        INSTANCE_CACHE.remove(cacheKey);
    }

    /** Factory interface for creating BodyCodec instances. */
    @FunctionalInterface
    public interface Factory {
        BodyCodec create(Schema schema);
    }
}
