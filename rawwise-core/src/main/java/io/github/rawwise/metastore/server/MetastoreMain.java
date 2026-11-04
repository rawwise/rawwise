package io.github.rawwise.metastore.server;

import io.github.rawwise.common.catalog.Catalog;
import io.github.rawwise.common.catalog.MetastoreCatalog;
import io.github.rawwise.common.catalog.MetastoreCatalogConfig;
import io.github.rawwise.governance.config.ConfigLoader;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.MetastoreRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetastoreMain {

    public static void main(String[] args) {
        Map<String, String> overrides = parseCliArgs(args);

        RawWiseConfig config = ConfigLoader.builder().cliOverrides(overrides).load();

        String basePath = config.get(RawWiseOptions.METASTORE_BASE_PATH);
        int grpcPort = config.get(RawWiseOptions.METASTORE_GRPC_PORT);

        MetastoreRegistry registry = MetastoreRegistry.getInstance();
        registry.registerDefaults();
        Metastore store = registry.create("rocksdb", config);
        log.info("Metastore created: kind=rocksdb, basePath={}", basePath);

        Catalog catalog;
        try {
            MetastoreCatalogConfig catalogConfig =
                    MetastoreCatalogConfig.builder()
                            .store(store)
                            .basePath(config.get(RawWiseOptions.CATALOG_WAREHOUSE_PATH))
                            .build();
            catalog = new MetastoreCatalog(catalogConfig);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Catalog", e);
        }

        GrpcServer grpcServer = new GrpcServer(grpcPort, catalog, store);
        CountDownLatch shutdownLatch = new CountDownLatch(1);

        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    log.info("Shutting down Metastore server");
                                    grpcServer.shutdown();
                                    shutdownLatch.countDown();
                                }));

        try {
            grpcServer.start();
            log.info("Standalone Metastore server started on port {}", grpcPort);
            shutdownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Metastore server failed", e);
        } finally {
            grpcServer.shutdown();
        }
    }

    private static Map<String, String> parseCliArgs(String[] args) {
        Map<String, String> overrides = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port":
                    if (i + 1 < args.length)
                        overrides.put(RawWiseOptions.METASTORE_GRPC_PORT.key(), args[++i]);
                    break;
                case "--base-path":
                    if (i + 1 < args.length)
                        overrides.put(RawWiseOptions.METASTORE_BASE_PATH.key(), args[++i]);
                    break;
                case "--config":
                    if (i + 1 < args.length) System.setProperty("rawwise.config.file", args[++i]);
                    break;
                default:
                    break;
            }
        }
        return overrides;
    }
}
