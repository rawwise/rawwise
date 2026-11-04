package io.github.rawwise.engine.storage.spi;

import io.github.rawwise.common.SingletonContainer;
import io.github.rawwise.governance.config.RawWiseConfig;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StorageRegistry {

    private final ConcurrentHashMap<String, StorageProvider> providers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Storage> storageCache = new ConcurrentHashMap<>();

    private StorageRegistry() {}

    public static StorageRegistry instance() {
        return SingletonContainer.getInstance(StorageRegistry.class);
    }

    public void register(StorageProvider provider) {
        providers.compute(
                provider.scheme(),
                (scheme, existing) -> {
                    if (existing != null) {
                        log.warn(
                                "Replacing StorageProvider for scheme '{}': {} -> {}",
                                scheme,
                                existing.getClass().getName(),
                                provider.getClass().getName());
                    }
                    return provider;
                });
    }

    public Storage getStorage(Storage.StoragePath path, RawWiseConfig config) {
        return storageCache.computeIfAbsent(
                path.scheme(),
                scheme -> {
                    var provider = providers.get(scheme);
                    if (provider == null) {
                        throw new IllegalArgumentException(
                                "No StorageProvider registered for scheme: " + scheme);
                    }
                    try {
                        return provider.create(path, config);
                    } catch (IOException e) {
                        throw new RuntimeException(
                                "Failed to create Storage for scheme: " + scheme, e);
                    }
                });
    }

    public static void registerDefaults() {
        var registry = instance();
        registry.register(new LocalStorageProvider());
    }
}
