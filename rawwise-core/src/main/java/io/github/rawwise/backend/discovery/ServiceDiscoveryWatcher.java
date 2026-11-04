package io.github.rawwise.backend.discovery;

import io.github.rawwise.backend.routing.NodeSlot;
import io.github.rawwise.backend.routing.RoutingCoordinator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Bridges ServiceRegistry node events to RoutingCoordinator routing updates.
 *
 * <p>When registered as a {@link ServiceRegistry.NodeEventListener}, node up/down events are
 * translated into {@link RoutingCoordinator#onExecutorUp} / {@link
 * RoutingCoordinator#onExecutorDown} calls so that shard assignments are automatically updated when
 * nodes join or leave the cluster.
 */
@Slf4j
public class ServiceDiscoveryWatcher implements ServiceRegistry.NodeEventListener {

    private final RoutingCoordinator routingCoordinator;
    private final Map<String, ServiceRegistry.NodeInfo> nodeInfoMap = new ConcurrentHashMap<>();

    public ServiceDiscoveryWatcher(RoutingCoordinator routingCoordinator) {
        this.routingCoordinator = routingCoordinator;
    }

    @Override
    public void onNodeUp(ServiceRegistry.NodeInfo nodeInfo) {
        nodeInfoMap.put(nodeInfo.executorId(), nodeInfo);
        NodeSlot nodeSlot = new NodeSlot(nodeInfo.nodeId(), nodeInfo.slotIndex());
        routingCoordinator.onExecutorUp(nodeInfo.executorId(), nodeSlot);
        if (nodeInfo.status() == ServiceRegistry.NodeStatus.READY) {
            routingCoordinator.onExecutorReady(nodeInfo.executorId());
        }
        log.info(
                "Node up: executorId={} host={}:{} shard={} status={}",
                nodeInfo.executorId(),
                nodeInfo.host(),
                nodeInfo.port(),
                nodeInfo.shardId(),
                nodeInfo.status());
    }

    @Override
    public void onNodeDown(String executorId) {
        ServiceRegistry.NodeInfo removed = nodeInfoMap.remove(executorId);
        routingCoordinator.onExecutorDown(executorId);
        log.info("Node down: executorId={} hadNodeInfo={}", executorId, removed != null);
    }

    /**
     * Returns the cached NodeInfo for the given executor, or null if not registered.
     *
     * @param executorId executor identifier
     * @return node info or null
     */
    public ServiceRegistry.NodeInfo getNodeInfo(String executorId) {
        return nodeInfoMap.get(executorId);
    }
}
