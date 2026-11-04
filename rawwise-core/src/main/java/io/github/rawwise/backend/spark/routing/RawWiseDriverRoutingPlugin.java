package io.github.rawwise.backend.spark.routing;

import io.github.rawwise.backend.routing.InMemoryRoutingCoordinator;
import io.github.rawwise.backend.routing.InMemoryShardRoutingTable;
import io.github.rawwise.backend.routing.NodeSlot;
import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.governance.config.ConfigFactory;
import io.github.rawwise.governance.config.ConfigLoader;
import io.github.rawwise.governance.config.DynamicConfigRegistry;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.rpc.RpcEnv;

@Slf4j
public final class RawWiseDriverRoutingPlugin implements DriverPlugin {
    private final RoutingCoordinator routingCoordinator;
    private transient RpcEnv rpcEnv;
    private transient DynamicConfigRegistry configRegistry;
    private transient volatile RawWiseConfig loadedConfig;

    public RawWiseDriverRoutingPlugin() {
        this(createDefaultCoordinator());
    }

    public RawWiseDriverRoutingPlugin(RoutingCoordinator routingCoordinator) {
        this.routingCoordinator =
                Objects.requireNonNull(routingCoordinator, "routingCoordinator must not be null");
    }

    private static RoutingCoordinator createDefaultCoordinator() {
        RawWiseConfig config = ConfigFactory.createDefault();
        return new InMemoryRoutingCoordinator(new InMemoryShardRoutingTable(), config);
    }

    @Override
    public Map<String, String> init(SparkContext sparkContext, PluginContext pluginContext) {
        Objects.requireNonNull(sparkContext, "sparkContext must not be null");
        Objects.requireNonNull(pluginContext, "pluginContext must not be null");
        RpcEnv resolvedRpcEnv = null;
        try {
            org.apache.spark.SparkEnv env = sparkContext.env();
            resolvedRpcEnv = env == null ? null : env.rpcEnv();
        } catch (Exception ignored) {
        }
        this.rpcEnv = resolvedRpcEnv;
        RawWiseConfig config = ConfigLoader.load(sparkContext.conf());
        this.loadedConfig = config;
        if (rpcEnv != null) {
            this.configRegistry = DynamicConfigRegistry.create(config);

            Path configFilePath = ConfigLoader.discoverConfigFilePath();
            if (configFilePath != null) {
                try {
                    configRegistry.start(configFilePath);
                    log.info("Driver-side config file watching started: {}", configFilePath);
                } catch (Exception e) {
                    log.warn(
                            "Failed to start config file watching for {}, hot reload disabled: {}",
                            configFilePath,
                            e.getMessage());
                }
            } else {
                log.info(
                        "No config file found, Driver-side hot reload disabled (propagation still active)");
            }

            rpcEnv.setupEndpoint(
                    RawWiseDriverRoutingEndpoint.ENDPOINT_NAME,
                    new RawWiseDriverRoutingEndpoint(
                            rpcEnv,
                            routingCoordinator,
                            sparkContext.conf(),
                            config,
                            configRegistry));
        }
        SparkRoutingBridge.install(routingCoordinator);
        sparkContext.addSparkListener(new RawWiseExecutorLossListener(routingCoordinator));
        return Map.of();
    }

    @Override
    public Object receive(Object message) {
        Objects.requireNonNull(message, "message must not be null");
        switch (message) {
            case RoutingMessages.RegisterExecutorMessage(
                    String executorId,
                    String nodeId,
                    int slotIndex) -> {
                routingCoordinator.onExecutorUp(executorId, new NodeSlot(nodeId, slotIndex));
                if (routingCoordinator instanceof InMemoryRoutingCoordinator coordinator) {
                    RawWiseConfig config =
                            configRegistry != null ? configRegistry.currentConfig() : loadedConfig;
                    String basePath =
                            config != null
                                    ? config.get(RawWiseOptions.STORAGE_MMAP_PATH)
                                    : RawWiseOptions.STORAGE_MMAP_PATH.defaultValue();
                    coordinator.registerExecutorBasePath(executorId, basePath);
                }
                return null;
            }
            case RoutingMessages.ExecutorReadyMessage(String executorId) -> {
                routingCoordinator.onExecutorReady(executorId);
                return null;
            }
            case RoutingMessages.ExecutorLostMessage(String executorId) -> {
                routingCoordinator.onExecutorDown(executorId);
                return null;
            }
            case RoutingMessages.LookupShardOwnerMessage(int shardId) -> {
                return routingCoordinator.lookupOwner(new ShardId(shardId));
            }
            case RoutingMessages.LookupShardOwnersMessage(java.util.Set<Integer> shardIds) -> {
                return routingCoordinator.lookupOwners(
                        shardIds.stream()
                                .map(ShardId::new)
                                .collect(
                                        java.util.stream.Collectors.toCollection(
                                                LinkedHashSet::new)));
            }
            default -> {}
        }
        throw new IllegalArgumentException(
                "Unsupported routing message type: " + message.getClass().getName());
    }

    @Override
    public void shutdown() {
        if (configRegistry != null) {
            try {
                configRegistry.stop();
                log.info("Driver-side DynamicConfigRegistry stopped");
            } catch (Exception e) {
                log.warn("Error stopping DynamicConfigRegistry: {}", e.getMessage());
            }
            configRegistry = null;
        }
        SparkRoutingBridge.clear();
        this.rpcEnv = null;
    }
}
