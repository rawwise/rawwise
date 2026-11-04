package io.github.rawwise.backend.server.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.axiom.pipeline.derivation.AutoDerivationHook;
import io.github.rawwise.backend.server.service.AxiomServiceImpl;
import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.PostWriteHook;
import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.writer.WriterFactoryBootstrap;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 3.1-T8: AutoDerivationHook deployboundaryvalidation.
 *
 * <p>validationarchitecturedesign §3.1.1 deployboundaryconstraint:
 *
 * <ol>
 *   <li>S1: BE Node side — DynamicMmapStore.registerAutoDeriveHook() in TableStore aboveregister
 *       AutoDerivationHook
 *   <li>S2: BE Server side — AxiomServiceImpl notholdhas DynamicMmapStore / TableStore /
 *       PostWriteHook reference
 *   <li>S3: BE Server side — AxiomServiceImpl.ingest() viapass gRPC executorStubs delegateto BE
 *       Node, notdirectly call hook
 * </ol>
 *
 * <p>follow RawWise E2E teststandard: makeusetrue DynamicMmapStore + InMemoryMetastore, non- mock.
 */
@DisplayName("Phase 3.1-T8: AutoDerivationHook Deployment Boundary Verification")
class AutoDerivationHookBoundaryE2ETest {

    @TempDir static Path tempDir;

    private static String previousEmbeddingType;
    private static String previousEmbeddingDimension;
    private static String previousWarehousePath;

    private DynamicMmapStore store;
    private String warehousePath;

    @BeforeAll
    static void setUpAll() {
        try {
            StorageRegistry.instance().register(new LocalStorageProvider());
        } catch (Exception ignored) {
        }
        try {
            StorageRegistry.registerDefaults();
            WriterFactoryBootstrap.registerAll();
        } catch (Exception ignored) {
        }

        previousEmbeddingType = System.getProperty("rawwise.embedding.provider.type");
        previousEmbeddingDimension = System.getProperty("rawwise.embedding.provider.dimension");
        previousWarehousePath = System.getProperty("rawwise.warehouse.path");

        System.setProperty("rawwise.embedding.provider.type", "stub");
        System.setProperty("rawwise.embedding.provider.dimension", "128");
    }

    @AfterAll
    static void tearDownAll() {
        if (previousEmbeddingType != null) {
            System.setProperty("rawwise.embedding.provider.type", previousEmbeddingType);
        } else {
            System.clearProperty("rawwise.embedding.provider.type");
        }
        if (previousEmbeddingDimension != null) {
            System.setProperty("rawwise.embedding.provider.dimension", previousEmbeddingDimension);
        } else {
            System.clearProperty("rawwise.embedding.provider.dimension");
        }
        if (previousWarehousePath != null) {
            System.setProperty("rawwise.warehouse.path", previousWarehousePath);
        } else {
            System.clearProperty("rawwise.warehouse.path");
        }

        try {
            io.github.rawwise.engine.storage.DynamicMmapStoreFactory.closeAll();
        } catch (Exception ignored) {
        }
    }

    @BeforeEach
    void setUp() {
        warehousePath = tempDir.resolve("warehouse-" + UUID.randomUUID()).toString();
        System.setProperty("rawwise.warehouse.path", warehousePath);
    }

    @AfterEach
    void tearDown() {
        try {
            if (store != null) {
                store.close();
            }
        } catch (Exception ignored) {
        }
    }

    private DynamicMmapStore createStore() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("rawwise.catalog.warehouse-path", warehousePath);
        cfg.put("rawwise.storage.lmdb.initial-map-size-mb", 16L);
        cfg.put("rawwise.storage.lmdb.max-map-size-mb", 256L);
        RawWiseConfig config = RawWiseConfig.fromMap(cfg);

        store = DynamicMmapStore.getInstance(warehousePath, "test-node", config);
        store.start();

        Metastore metastore = new InMemoryMetastore();
        store.setMetastore(metastore);

        store.initAxiomEngines(config);
        return store;
    }

    @Test
    @DisplayName("S1: BE Node — registerAutoDeriveHook registers AutoDerivationHook on TableStore")
    void testHookRegisteredOnNodeSide() {
        DynamicMmapStore nodeStore = createStore();
        String tableId = "default.boundary_test_" + UUID.randomUUID().toString().substring(0, 8);

        nodeStore.registerAutoDeriveHook(tableId);

        TableStore ts = nodeStore.tableStore(tableId);
        List<PostWriteHook> hooks = ts.getPostWriteHooks();

        assertThat(hooks)
                .as(
                        "TableStore should have at least one PostWriteHook after registerAutoDeriveHook")
                .isNotEmpty();

        boolean hasAutoDerivationHook =
                hooks.stream().anyMatch(h -> h instanceof AutoDerivationHook);
        assertThat(hasAutoDerivationHook)
                .as("TableStore should have AutoDerivationHook registered")
                .isTrue();
    }

    @Test
    @DisplayName(
            "S2: BE Server — AxiomServiceImpl does NOT hold DynamicMmapStore or TableStore references")
    void testServerSideHasNoStoreOrHook() {
        Field[] fields = AxiomServiceImpl.class.getDeclaredFields();

        boolean hasDynamicMmapStore = false;
        boolean hasTableStore = false;
        boolean hasPostWriteHook = false;
        boolean hasAutoDerivationHook = false;

        for (Field f : fields) {
            Class<?> type = f.getType();
            if (type == DynamicMmapStore.class) hasDynamicMmapStore = true;
            if (type == TableStore.class) hasTableStore = true;
            if (type == PostWriteHook.class) hasPostWriteHook = true;
            if (type == AutoDerivationHook.class) hasAutoDerivationHook = true;
        }

        assertThat(hasDynamicMmapStore)
                .as(
                        "AxiomServiceImpl (BE Server) must NOT hold DynamicMmapStore reference — deployment boundary violation")
                .isFalse();
        assertThat(hasTableStore)
                .as(
                        "AxiomServiceImpl (BE Server) must NOT hold TableStore reference — deployment boundary violation")
                .isFalse();
        assertThat(hasPostWriteHook)
                .as(
                        "AxiomServiceImpl (BE Server) must NOT hold PostWriteHook reference — deployment boundary violation")
                .isFalse();
        assertThat(hasAutoDerivationHook)
                .as(
                        "AxiomServiceImpl (BE Server) must NOT hold AutoDerivationHook reference — deployment boundary violation")
                .isFalse();
    }

    @Test
    @DisplayName(
            "S3: BE Server — AxiomServiceImpl has executorStubs (gRPC delegation), not local store")
    void testServerSideUsesGrpcDelegation() {
        Field[] fields = AxiomServiceImpl.class.getDeclaredFields();

        boolean hasExecutorStubs = false;
        for (Field f : fields) {
            String name = f.getName();
            if ("executorStubs".equals(name)) {
                hasExecutorStubs = true;
                break;
            }
        }

        assertThat(hasExecutorStubs)
                .as(
                        "AxiomServiceImpl (BE Server) should have executorStubs field for gRPC delegation to BE Nodes")
                .isTrue();
    }
}
