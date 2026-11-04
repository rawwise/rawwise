package io.github.rawwise.backend.discovery;

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;

/**
 * Service registration abstraction interface.
 *
 * <p>Architecture:
 *
 * <pre>
 * BE Node startup -> ServiceRegistry.register(executorId, NodeInfo)
 *   BE Server    -> ServiceRegistry.watch(listener) -> RoutingCoordinator.onExecutorUp/Down
 * BE Node stop -> ServiceRegistry.unregister(executorId) or TTL expiry
 * </pre>
 *
 * <p>Implementation:
 *
 * <ul>
 *   <li>{@link InMemoryServiceRegistry} - in-process implementation, used for development and
 *       testing
 *   <li>{@code EtcdServiceRegistry} - production environment implementation (interface, requires
 *       jetcd dependency)
 * </ul>
 *
 * <p>Semantics:
 *
 * <ul>
 *   <li>Registered NodeInfo has TTL (default 15s), renewed by heartbeat; auto-deregister on timeout
 *   <li>Watch listener receives async callbacks on node register/deregister
 *   <li>listNodes returns all current nodes
 * </ul>
 */
public interface ServiceRegistry {

    /**
     * Register or update node info (heartbeat renewal).
     *
     * @param nodeInfo node info
     * @param ttlSeconds TTL in seconds; auto-deregister if not renewed before timeout
     */
    void register(NodeInfo nodeInfo, long ttlSeconds);

    /**
     * Deregister a node.
     *
     * @param executorId executor identifier
     */
    void unregister(String executorId);

    /**
     * Query node info.
     *
     * @param executorId executor identifier
     * @return node info, or empty (not registered or already expired)
     */
    Optional<NodeInfo> lookup(String executorId);

    /**
     * Returns all nodes.
     *
     * @return set of node info
     */
    Set<NodeInfo> listNodes();

    /**
     * Register a watch listener for node register/deregister callbacks.
     *
     * @param listener listener
     */
    void watch(NodeEventListener listener);

    /** Close the registry and free resources. */
    void close();

    /**
     * Node info.
     *
     * @param executorId executor unique identifier
     * @param nodeId physical node ID (hostname or IP)
     * @param host gRPC address
     * @param port gRPC port
     * @param slotIndex slot index
     * @param shardId owned shard ID
     * @param status node status (STARTING/READY/LEAVING)
     * @param timestamp registration timestamp (seconds)
     */
    record NodeInfo(
            String executorId,
            String nodeId,
            String host,
            int port,
            int slotIndex,
            int shardId,
            NodeStatus status,
            long timestamp)
            implements Serializable {
        public NodeInfo {
            if (executorId == null || executorId.isBlank()) {
                throw new IllegalArgumentException("executorId must not be blank");
            }
            if (nodeId == null || nodeId.isBlank()) {
                throw new IllegalArgumentException("nodeId must not be blank");
            }
            if (host == null || host.isBlank()) {
                throw new IllegalArgumentException("host must not be blank");
            }
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException("port must be in range 1-65535");
            }
            if (slotIndex < 0) {
                throw new IllegalArgumentException("slotIndex must be non-negative");
            }
            if (status == null) {
                status = NodeStatus.STARTING;
            }
        }
    }

    enum NodeStatus {
        STARTING,
        READY,
        LEAVING
    }

    /** Node event listener. */
    interface NodeEventListener {
        /**
         * Callback on node register or update.
         *
         * @param nodeInfo node info
         */
        void onNodeUp(NodeInfo nodeInfo);

        /**
         * Callback on node deregister or expiry.
         *
         * @param executorId executor identifier
         */
        void onNodeDown(String executorId);
    }
}
