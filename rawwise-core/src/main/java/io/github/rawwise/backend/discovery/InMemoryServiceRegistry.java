package io.github.rawwise.backend.discovery;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * In-process ServiceRegistry implementation (for development and testing).
 *
 * <p>Semantics:
 *
 * <ul>
 *   <li>Node registration records NodeInfo + expiry timestamp
 *   <li>Background thread periodic scan (default 5s); expired nodes auto-deregistered and trigger
 *       onNodeDown
 *   <li>Watch listener receives synchronous callbacks (production can use async)
 * </ul>
 *
 * <p>Production should use the etcd implementation ({@code EtcdServiceRegistry}), which requires
 * the jetcd dependency.
 */
@Slf4j
public class InMemoryServiceRegistry implements ServiceRegistry {
    private static final long DEFAULT_SCAN_INTERVAL_SECONDS = 5L;

    private final Map<String, NodeEntry> nodes = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<NodeEventListener> listeners = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService expiryScanner;
    private final ScheduledFuture<?> scanTask;
    private final long scanIntervalSeconds;

    public InMemoryServiceRegistry() {
        this(DEFAULT_SCAN_INTERVAL_SECONDS);
    }

    public InMemoryServiceRegistry(long scanIntervalSeconds) {
        this.scanIntervalSeconds = scanIntervalSeconds;
        this.expiryScanner =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "service-registry-scanner");
                            t.setDaemon(true);
                            return t;
                        });
        this.scanTask =
                expiryScanner.scheduleAtFixedRate(
                        this::scanExpiredNodes,
                        scanIntervalSeconds,
                        scanIntervalSeconds,
                        TimeUnit.SECONDS);
        log.info("InMemoryServiceRegistry started, scan interval={}s", scanIntervalSeconds);
    }

    @Override
    public void register(NodeInfo nodeInfo, long ttlSeconds) {
        long expiryTime = System.currentTimeMillis() + ttlSeconds * 1000L;
        NodeEntry entry = new NodeEntry(nodeInfo, expiryTime);
        NodeEntry previous = nodes.put(nodeInfo.executorId(), entry);
        log.info(
                "Node registered: executorId={} host={}:{} shard={} ttl={}s status={}",
                nodeInfo.executorId(),
                nodeInfo.host(),
                nodeInfo.port(),
                nodeInfo.shardId(),
                ttlSeconds,
                nodeInfo.status());
        notifyNodeUp(nodeInfo);
    }

    @Override
    public void unregister(String executorId) {
        NodeEntry removed = nodes.remove(executorId);
        if (removed != null) {
            log.info("Node unregistered: executorId={}", executorId);
            notifyNodeDown(executorId);
        }
    }

    @Override
    public Optional<NodeInfo> lookup(String executorId) {
        NodeEntry entry = nodes.get(executorId);
        if (entry == null || entry.isExpired()) {
            if (entry != null && entry.isExpired()) {
                nodes.remove(executorId, entry);
                notifyNodeDown(executorId);
            }
            return Optional.empty();
        }
        return Optional.of(entry.nodeInfo);
    }

    @Override
    public Set<NodeInfo> listNodes() {
        return nodes.values().stream()
                .filter(e -> !e.isExpired())
                .map(e -> e.nodeInfo)
                .collect(Collectors.toSet());
    }

    @Override
    public void watch(NodeEventListener listener) {
        listeners.add(listener);
        log.info("NodeEventListener registered, total listeners={}", listeners.size());
    }

    @Override
    public void close() {
        if (scanTask != null) {
            scanTask.cancel(false);
        }
        expiryScanner.shutdownNow();
        nodes.clear();
        log.info("InMemoryServiceRegistry closed");
    }

    private void scanExpiredNodes() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, NodeEntry> e : nodes.entrySet()) {
            if (e.getValue().isExpired()) {
                String executorId = e.getKey();
                if (nodes.remove(executorId, e.getValue())) {
                    log.info("Node expired (TTL): executorId={}", executorId);
                    notifyNodeDown(executorId);
                }
            }
        }
    }

    private void notifyNodeUp(NodeInfo nodeInfo) {
        for (NodeEventListener listener : listeners) {
            try {
                listener.onNodeUp(nodeInfo);
            } catch (Exception e) {
                log.warn("NodeEventListener.onNodeUp failed: {}", e.getMessage());
            }
        }
    }

    private void notifyNodeDown(String executorId) {
        for (NodeEventListener listener : listeners) {
            try {
                listener.onNodeDown(executorId);
            } catch (Exception e) {
                log.warn("NodeEventListener.onNodeDown failed: {}", e.getMessage());
            }
        }
    }

    private static class NodeEntry {
        final NodeInfo nodeInfo;
        final long expiryTime;

        NodeEntry(NodeInfo nodeInfo, long expiryTime) {
            this.nodeInfo = nodeInfo;
            this.expiryTime = expiryTime;
        }

        boolean isExpired() {
            return System.currentTimeMillis() > expiryTime;
        }
    }
}
