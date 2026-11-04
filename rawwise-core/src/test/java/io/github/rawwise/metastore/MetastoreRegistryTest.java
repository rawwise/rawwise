package io.github.rawwise.metastore;

import static org.assertj.core.api.Assertions.*;

import io.github.rawwise.governance.config.ConfigFactory;
import io.github.rawwise.governance.config.RawWiseConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MetastoreRegistryTest {

    private MetastoreRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new MetastoreRegistry();
    }

    @Test
    void shouldRegisterAndCreateInMemoryStore() throws Exception {
        registry.register(new InMemoryMetastoreFactory());
        RawWiseConfig config = ConfigFactory.createDefault();
        Metastore store = registry.create("inmemory", config);
        assertThat(store).isInstanceOf(InMemoryMetastore.class);
        store.close();
    }

    @Test
    void shouldThrowForUnknownKind() {
        RawWiseConfig config = ConfigFactory.createDefault();
        assertThatThrownBy(() -> registry.create("unknown", config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown");
    }

    @Test
    void shouldRegisterDefaults() {
        registry.registerDefaults();
        assertThat(registry.registeredCount()).isGreaterThanOrEqualTo(1);
        assertThat(registry.isRegistered("inmemory")).isTrue();
    }
}
