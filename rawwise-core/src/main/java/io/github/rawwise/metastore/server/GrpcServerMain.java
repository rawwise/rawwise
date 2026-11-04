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
public class GrpcServerMain {

    private static Map<String, String> parseCliArgs(String[] args) {
        Map<String, String> overrides = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port":
                    if (i + 1 < args.length)
                        overrides.put(RawWiseOptions.METASTORE_GRPC_PORT.key(), args[++i]);
                    break;
                case "--config":
                    if (i + 1 < args.length) System.setProperty("rawwise.config.file", args[++i]);
                    break;
                case "--metastore":
                    if (i + 1 < args.length)
                        overrides.put(RawWiseOptions.METASTORE_TYPE.key(), args[++i]);
                    break;
                default:
                    break;
            }
        }
        return overrides;
    }

    public static void main(String[] args) {
        RawWiseConfig config = ConfigLoader.builder().cliOverrides(parseCliArgs(args)).load();

        MetastoreRegistry registry = MetastoreRegistry.getInstance();
        registry.registerDefaults();
        String metastoreKind = config.get(RawWiseOptions.METASTORE_TYPE);
        Metastore store = registry.create(metastoreKind, config);
        log.info("Metastore created: kind={}", metastoreKind);

        Catalog catalog;
        try {
            String warehousePath = config.get(RawWiseOptions.CATALOG_WAREHOUSE_PATH);
            if (warehousePath == null || warehousePath.isBlank()) {
                warehousePath = System.getProperty("rawwise.warehouse.path", "");
            }
            MetastoreCatalogConfig catalogConfig =
                    MetastoreCatalogConfig.builder().store(store).basePath(warehousePath).build();
            catalog = new MetastoreCatalog(catalogConfig);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Catalog", e);
        }

        int grpcPort = config.get(RawWiseOptions.METASTORE_GRPC_PORT);
        GrpcServer grpcServer = new GrpcServer(grpcPort, catalog, store);

        CountDownLatch shutdownLatch = new CountDownLatch(1);

        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    log.info("Shutting down Metastore gRPC server");
                                    grpcServer.shutdown();
                                    shutdownLatch.countDown();
                                }));

        try {
            grpcServer.start();
            log.info("Metastore gRPC server started on port {}", grpcPort);
            shutdownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Metastore gRPC server failed", e);
        } finally {
            grpcServer.shutdown();
        }
    }
}
