package io.github.rawwise.engine.storage.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.common.SingletonContainer;
import io.github.rawwise.governance.config.RawWiseConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StorageRegistryTest {

    private StorageRegistry registry;

    @BeforeEach
    void setUp() {
        SingletonContainer.destroyInstance(StorageRegistry.class);
        registry = StorageRegistry.instance();
    }

    @AfterEach
    void tearDown() {
        SingletonContainer.destroyInstance(StorageRegistry.class);
    }

    @Test
    void registerAddsProvider() {
        registry.register(new LocalStorageProvider());
        var storage =
                registry.getStorage(new Storage.StoragePath("file", "/tmp"), RawWiseConfig.empty());
        assertThat(storage).isNotNull();
        assertThat(storage).isInstanceOf(LocalStorage.class);
    }

    @Test
    void registerReplacesExistingProvider() {
        registry.register(new LocalStorageProvider());
        registry.register(new LocalStorageProvider());
        var storage =
                registry.getStorage(new Storage.StoragePath("file", "/tmp"), RawWiseConfig.empty());
        assertThat(storage).isInstanceOf(LocalStorage.class);
    }

    @Test
    void getStorageThrowsForUnknownScheme() {
        assertThatThrownBy(
                        () ->
                                registry.getStorage(
                                        new Storage.StoragePath("unknown", "/path"),
                                        RawWiseConfig.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown");
    }

    @Test
    void registerDefaultsRegistersLocalStorageProvider() {
        StorageRegistry.registerDefaults();
        var storage =
                registry.getStorage(new Storage.StoragePath("file", "/tmp"), RawWiseConfig.empty());
        assertThat(storage).isInstanceOf(LocalStorage.class);
    }

    @Test
    void getStorageCachesByScheme() {
        registry.register(new LocalStorageProvider());
        var s1 =
                registry.getStorage(new Storage.StoragePath("file", "/tmp"), RawWiseConfig.empty());
        var s2 =
                registry.getStorage(
                        new Storage.StoragePath("file", "/other"), RawWiseConfig.empty());
        assertThat(s1).isSameAs(s2);
    }
}
