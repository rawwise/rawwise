package io.github.rawwise.common.catalog;

import io.github.rawwise.governance.config.ConfigLoader;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.metastore.GrpcMetastoreClient;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.MetastoreRegistry;
import java.util.Map;

public class MetastoreCatalogFactory implements CatalogFactory {

    private static final String KEY_CATALOG_NAME = "catalog-name";
    private static final String KEY_BASE_PATH = "base-path";
    private static final String KEY_STORE_KIND = "store-kind";
    private static final String KEY_GRPC_HOST = "metastore-grpc-host";
    private static final String KEY_GRPC_PORT = "metastore-grpc-port";

    private final RawWiseConfig config;

    public MetastoreCatalogFactory() {
        this(ConfigLoader.load());
    }

    public MetastoreCatalogFactory(RawWiseConfig config) {
        this.config = config;
    }

    @Override
    public String kind() {
        return "metastore";
    }

    @Override
    public Catalog create(Map<String, String> properties) {
        String catalogName = properties.getOrDefault(KEY_CATALOG_NAME, "default");
        String basePath = properties.get(KEY_BASE_PATH);
        if (basePath == null || basePath.isBlank()) {
            throw new IllegalArgumentException("base-path is required for metastore catalog");
        }

        String storeKind = properties.getOrDefault(KEY_STORE_KIND, "inmemory");
        Metastore store;
        if ("grpc".equals(storeKind)) {
            String grpcHost = properties.getOrDefault(KEY_GRPC_HOST, "127.0.0.1");
            int grpcPort = Integer.parseInt(properties.getOrDefault(KEY_GRPC_PORT, "9001"));
            store = new GrpcMetastoreClient(grpcHost, grpcPort);
        } else {
            Metastore shared =
                    io.github.rawwise.backend.shared.BackendSharedRegistry.getMetastore();
            if (shared != null) {
                store = shared;
            } else {
                var storeRegistry = MetastoreRegistry.getInstance();
                if (!storeRegistry.isRegistered(storeKind)) {
                    storeRegistry.registerDefaults();
                }
                store = storeRegistry.create(storeKind, config);
            }
        }

        var catalogConfig =
                MetastoreCatalogConfig.builder()
                        .store(store)
                        .basePath(basePath)
                        .catalogName(catalogName)
                        .build();

        return new MetastoreCatalog(catalogConfig);
    }
}
