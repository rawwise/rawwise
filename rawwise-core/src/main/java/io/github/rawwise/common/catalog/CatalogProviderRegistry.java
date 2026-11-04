package io.github.rawwise.common.catalog;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class CatalogProviderRegistry {

    private static final CatalogProviderRegistry INSTANCE = new CatalogProviderRegistry();
    private final Map<String, CatalogFactory> factories = new ConcurrentHashMap<>();

    CatalogProviderRegistry() {}

    public static CatalogProviderRegistry getInstance() {
        return INSTANCE;
    }

    public void register(CatalogFactory factory) {
        factories.put(factory.kind(), factory);
    }

    public void registerDefaults() {
        register(new MetastoreCatalogFactory());
        register(new HadoopFileCatalogFactory());
    }

    public Catalog create(String kind, Map<String, String> properties) {
        CatalogFactory factory = factories.get(kind);
        if (factory == null) {
            throw new IllegalArgumentException(
                    "Unknown Catalog kind: " + kind + ". Registered: " + factories.keySet());
        }
        return factory.create(properties);
    }

    public boolean isRegistered(String kind) {
        return factories.containsKey(kind);
    }

    public int registeredCount() {
        return factories.size();
    }

    public Set<String> registeredKinds() {
        return Set.copyOf(factories.keySet());
    }
}
