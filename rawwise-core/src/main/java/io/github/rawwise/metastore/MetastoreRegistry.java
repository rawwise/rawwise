package io.github.rawwise.metastore;

import io.github.rawwise.governance.config.RawWiseConfig;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * service discovery and registry,management Metastore Factory register Metastore create.
 *
 * <p>uses singleton pattern,thread-safe.via {@link #register Defaults()} registerimplementation.
 */
public final class MetastoreRegistry {

    private static final MetastoreRegistry INSTANCE = new MetastoreRegistry();
    private final Map<String, MetastoreFactory> factories = new ConcurrentHashMap<>();

    // Visible For Testing - allows test code in same package to create isolated instances
    MetastoreRegistry() {}

    public static MetastoreRegistry getInstance() {
        return INSTANCE;
    }

    /**
     * register a Metastore Factory.
     *
     * @param factory factory to register, {@link Metastore Factory#kind()} as registration key
     */
    public void register(MetastoreFactory factory) {
        factories.put(factory.kind(), factory);
    }

    /** registerdefaultimplementation:inmemory and rocksdb. */
    public void registerDefaults() {
        register(new InMemoryMetastoreFactory());
        register(new RocksdbMetastoreFactory());
        register(new GrpcMetastoreFactory());
    }

    /**
     * kind registerfactorycreate Metastore instance.
     *
     * @param kind factory identifier, {@link Metastore Factory#kind()} match
     * @param config config object,passed to factory's create method
     * @return create Metastore instance
     * @throws Illegal Argument Exception if kind is not registered
     */
    public Metastore create(String kind, RawWiseConfig config) {
        MetastoreFactory factory = factories.get(kind);
        if (factory == null) {
            throw new IllegalArgumentException(
                    "Unknown Metastore kind: " + kind + ". Registered: " + factories.keySet());
        }
        return factory.create(config);
    }

    /**
     * check if specified kind is registered.
     *
     * @param kind factory identifier
     * @return returns true if registered
     */
    public boolean isRegistered(String kind) {
        return factories.containsKey(kind);
    }

    /**
     * get count of registered factories.
     *
     * @return number of registered factories
     */
    public int registeredCount() {
        return factories.size();
    }
}
