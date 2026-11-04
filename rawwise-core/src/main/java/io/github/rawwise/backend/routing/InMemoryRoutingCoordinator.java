package io.github.rawwise.backend.routing;

import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class InMemoryRoutingCoordinator implements RoutingCoordinator {
    private static final Pattern EXECUTOR_ID_PATTERN = Pattern.compile("^[A-Za-z0-9_.-]+$");

    private final ShardRoutingTable routingTable;
    private final Map<ShardId, List<ShardRecoveryRecord>> shardHistory = new ConcurrentHashMap<>();
    private final RawWiseConfig config;
    private final Map<String, String> executorBasePaths = new ConcurrentHashMap<>();
    private volatile Runnable pendingRecoveryDispatcher;
    private final Map<ShardId, Set<String>> shardTableIdentifiers = new ConcurrentHashMap<>();

    public InMemoryRoutingCoordinator(ShardRoutingTable routingTable) {
        this(routingTable, null);
    }

    public InMemoryRoutingCoordinator(ShardRoutingTable routingTable, RawWiseConfig config) {
        this.routingTable = Objects.requireNonNull(routingTable, "routingTable must not be null");
        this.config = config;
    }

    @Override
    public void onExecutorUp(String executorId, NodeSlot nodeSlot) {
        if (executorId == null || executorId.isBlank()) {
            throw new IllegalArgumentException("executorId must not be blank");
        }
        if (!EXECUTOR_ID_PATTERN.matcher(executorId).matches()) {
            throw new IllegalArgumentException(
                    "executorId contains invalid characters, allowed: A-Za-z0-9_.-");
        }
        Objects.requireNonNull(nodeSlot, "nodeSlot must not be null");
        routingTable.registerExecutor(executorId, nodeSlot);
        for (ShardLookupResult routing : routingTable.routings()) {
            ShardAssignment assignment = routing.assignment();
            boolean prefersThisSlot =
                    routingTable
                            .preferredSlotOf(routing.shardId())
                            .filter(nodeSlot::equals)
                            .isPresent();
            boolean currentlyOnDifferentSlot =
                    assignment == null || !assignment.nodeSlot().equals(nodeSlot);
            if (routing.status() == RoutingStatus.REASSIGNING
                    || (prefersThisSlot
                            && currentlyOnDifferentSlot
                            && routing.status() != RoutingStatus.RECOVERING)) {
                routingTable.reassignShard(routing.shardId());
            }
        }
    }

    @Override
    public void onExecutorReady(String executorId) {
        if (executorId == null || executorId.isBlank()) {
            throw new IllegalArgumentException("executorId must not be blank");
        }
        if (!EXECUTOR_ID_PATTERN.matcher(executorId).matches()) {
            throw new IllegalArgumentException(
                    "executorId contains invalid characters, allowed: A-Za-z0-9_.-");
        }
        routingTable.markExecutorReady(executorId);
    }

    @Override
    public void onExecutorDown(String executorId) {
        if (executorId == null || executorId.isBlank()) {
            throw new IllegalArgumentException("executorId must not be blank");
        }
        if (!EXECUTOR_ID_PATTERN.matcher(executorId).matches()) {
            throw new IllegalArgumentException(
                    "executorId contains invalid characters, allowed: A-Za-z0-9_.-");
        }
        List<ShardAssignment> assignments =
                routingTable.unregisterExecutorAndCollectAssignments(executorId);
        if (assignments.isEmpty()) {
            log.warn(
                    "onExecutorDown called for executor {} but no assignments found (already unregistered or duplicate trigger)",
                    executorId);
            return;
        }
        buildShardHistory(executorId, assignments);
        executorBasePaths.remove(executorId);
        Runnable dispatcher = pendingRecoveryDispatcher;
        if (dispatcher != null) {
            try {
                dispatcher.run();
            } catch (Exception e) {
                log.error("Recovery task dispatcher failed after executor {} down", executorId, e);
            }
        }
    }

    @Override
    public ShardLookupResult lookupOwner(ShardId shardId) {
        Objects.requireNonNull(shardId, "shardId must not be null");
        ShardLookupResult result = routingTable.lookup(shardId);
        if (result.status() == RoutingStatus.UNASSIGNED) {
            routingTable.reassignShard(shardId);
            return routingTable.lookup(shardId);
        }
        return result;
    }

    @Override
    public Map<ShardId, ShardLookupResult> lookupOwners(Set<ShardId> shardIds) {
        Objects.requireNonNull(shardIds, "shardIds must not be null");
        Map<ShardId, ShardLookupResult> result = routingTable.lookupBatch(shardIds);
        for (Map.Entry<ShardId, ShardLookupResult> entry : result.entrySet()) {
            if (entry.getValue().status() == RoutingStatus.UNASSIGNED) {
                routingTable.reassignShard(entry.getKey());
            }
        }
        return routingTable.lookupBatch(shardIds);
    }

    public boolean onRecoveryComplete(ShardId shardId, RoutingEpoch epoch, boolean success) {
        if (routingTable instanceof InMemoryShardRoutingTable table) {
            if (success) {
                boolean cleared =
                        table.clearRecoveringFlag(
                                shardId, epoch, () -> shardHistory.remove(shardId));
                return cleared;
            }
        }
        return false;
    }

    public List<ShardRecoveryRecord> getShardHistory(ShardId shardId) {
        List<ShardRecoveryRecord> records = shardHistory.get(shardId);
        return records == null ? Collections.emptyList() : List.copyOf(records);
    }

    public List<ShardLookupResult> getRecoveringShards() {
        return routingTable.routings().stream()
                .filter(r -> r.status() == RoutingStatus.RECOVERING)
                .toList();
    }

    public void registerExecutorBasePath(String executorId, String basePath) {
        if (basePath != null && !basePath.isBlank()) {
            executorBasePaths.put(executorId, basePath);
        }
    }

    public void setRecoveryTaskDispatcher(Runnable dispatcher) {
        this.pendingRecoveryDispatcher = dispatcher;
    }

    public void registerShardTables(ShardId shardId, Set<String> tableIdentifiers) {
        if (tableIdentifiers != null && !tableIdentifiers.isEmpty()) {
            shardTableIdentifiers.put(shardId, Set.copyOf(tableIdentifiers));
        }
    }

    @Override
    public void assignShard(ShardAssignment assignment) {
        routingTable.assignShard(assignment);
    }

    @Override
    public void reassignShard(ShardId shardId) {
        routingTable.reassignShard(shardId);
    }

    @Override
    public void reassignShardForRecovery(ShardId shardId) {
        routingTable.reassignShardForRecovery(shardId);
    }

    /**
     * P4: 返回所有 shardId 集合（用于 range-based routing 的 lookupOwnerForKey 遍历）。
     *
     * <p>当前实现忽略 tableId 参数（InMemoryRoutingCoordinator 不按表拆分 shard 集合）。 未来按表分片时可在此过滤。
     */
    @Override
    public Set<ShardId> allShardIds(String tableId) {
        return routingTable.assignments().stream()
                .map(ShardAssignment::shardId)
                .collect(Collectors.toCollection(HashSet::new));
    }

    private void buildShardHistory(String executorId, List<ShardAssignment> assignments) {
        long lostTimestamp = System.currentTimeMillis();
        String previousBasePath = executorBasePaths.getOrDefault(executorId, "");
        String walHdfsDir = deriveWalHdfsDir(executorId);
        for (ShardAssignment assignment : assignments) {
            Set<String> tables = shardTableIdentifiers.getOrDefault(assignment.shardId(), Set.of());
            ShardRecoveryRecord record =
                    new ShardRecoveryRecord(
                            assignment.shardId(),
                            assignment.executorId(),
                            assignment.nodeSlot().toString(),
                            previousBasePath,
                            walHdfsDir,
                            tables,
                            assignment.epoch(),
                            lostTimestamp);
            shardHistory
                    .computeIfAbsent(assignment.shardId(), k -> new CopyOnWriteArrayList<>())
                    .add(record);
        }
    }

    private String deriveWalHdfsDir(String executorId) {
        if (config == null) {
            return "";
        }
        String walHdfsPath = config.get(RawWiseOptions.STORAGE_WAL_HDFS_PATH);
        if (walHdfsPath == null || walHdfsPath.isBlank()) {
            return "";
        }
        if (!EXECUTOR_ID_PATTERN.matcher(executorId).matches()) {
            log.error(
                    "deriveWalHdfsDir: executorId contains invalid characters, refusing to build path");
            return "";
        }
        String base =
                walHdfsPath.endsWith("/")
                        ? walHdfsPath.substring(0, walHdfsPath.length() - 1)
                        : walHdfsPath;
        String combined = base + "/" + executorId;
        try {
            String normalized = new URI(combined).normalize().toString();
            if (!normalized.startsWith(base)) {
                log.error(
                        "deriveWalHdfsDir: normalized path escapes base directory, refusing to build path");
                return "";
            }
        } catch (Exception e) {
            log.error("deriveWalHdfsDir: failed to normalize URI, refusing to build path", e);
            return "";
        }
        return combined;
    }

    @Override
    public ShardLookupResult lookupReplica(ShardId shardId) {
        ShardLookupResult primary = lookupOwner(shardId);
        if (primary == null || !primary.hasAssignment()) {
            return null;
        }
        String primaryExecutor = primary.assignment().executorId();
        // Find a different registered executor as replica
        for (String executorId : routingTable.registeredExecutors()) {
            if (!executorId.equals(primaryExecutor)) {
                NodeSlot slot = routingTable.nodeSlotOf(executorId).orElse(null);
                if (slot != null) {
                    return ShardLookupResult.assigned(
                            new ShardAssignment(
                                    shardId, slot, executorId, primary.assignment().epoch()));
                }
            }
        }
        return null;
    }

    @Override
    public java.util.Set<String> getRegisteredExecutorIds() {
        return routingTable.registeredExecutors();
    }

    @Override
    public long getLastHeartbeatMs(String executorId) {
        return routingTable.getLastHeartbeatMs(executorId);
    }
}
