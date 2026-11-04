package io.github.rawwise.backend.spark;

import io.github.rawwise.backend.routing.RoutingStatus;
import io.github.rawwise.backend.routing.ShardAssignment;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.routing.ShardLookupResult;
import io.github.rawwise.backend.spark.routing.SparkRoutingBridge;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkExecutorInfo;

public record RawWiseWriteTopology(Map<String, Integer> executorsPerHost) {
    public static RawWiseWriteTopology inspect(SparkContext sparkContext) {
        LinkedHashMap<String, Integer> perHost = new LinkedHashMap<>();
        for (SparkExecutorInfo executorInfo : sparkContext.statusTracker().getExecutorInfos()) {
            if (executorInfo == null
                    || executorInfo.host() == null
                    || executorInfo.host().isBlank()) {
                continue;
            }
            perHost.merge(executorInfo.host(), 1, Integer::sum);
        }
        return new RawWiseWriteTopology(Map.copyOf(perHost));
    }

    public boolean supportsPreferredLocations() {
        return !executorsPerHost.isEmpty()
                && executorsPerHost.values().stream().allMatch(count -> count == 1);
    }

    public Map<Integer, List<String>> preferredLocationsByShard(int shardCount) {
        return preferredLocationsByShard(shardCount, null);
    }

    public Map<Integer, List<String>> preferredLocationsByShard(int shardCount, String driverHost) {
        if (!supportsPreferredLocations() || shardCount <= 0) {
            return Map.of();
        }

        try {
            Set<Integer> shardIds = new LinkedHashSet<>();
            for (int shardId = 0; shardId < shardCount; shardId++) {
                shardIds.add(shardId);
            }
            Map<ShardId, ShardLookupResult> lookups = SparkRoutingBridge.lookupOwners(shardIds);
            return buildPreferredLocationsByShard(
                    new HashSet<>(executorsPerHost.keySet()), driverHost, lookups);
        } catch (Exception ignored) {
            return Map.of();
        }
    }

    static Map<Integer, List<String>> buildPreferredLocationsByShard(
            Set<String> activeHosts, String driverHost, Map<ShardId, ShardLookupResult> lookups) {
        if (activeHosts == null || activeHosts.isEmpty() || lookups == null || lookups.isEmpty()) {
            return Map.of();
        }
        String normalizedDriverHost =
                (driverHost == null || driverHost.isBlank()) ? null : driverHost.trim();

        LinkedHashMap<Integer, List<String>> preferred = new LinkedHashMap<>();
        for (Map.Entry<ShardId, ShardLookupResult> entry : lookups.entrySet()) {
            ShardLookupResult lookup = entry.getValue();
            if (lookup == null || lookup.status() != RoutingStatus.ASSIGNED) {
                continue;
            }
            ShardAssignment assignment = lookup.assignment();
            if (assignment == null || assignment.nodeSlot() == null) {
                continue;
            }
            String host = assignment.nodeSlot().nodeId();
            if (host == null || host.isBlank()) {
                continue;
            }
            // Only prefer hosts that are currently alive/active, and never prefer the driver host.
            if (!activeHosts.contains(host)) {
                continue;
            }
            if (normalizedDriverHost != null && normalizedDriverHost.equals(host)) {
                continue;
            }
            preferred.put(entry.getKey().value(), List.of(host));
        }
        return preferred.isEmpty() ? Map.of() : Map.copyOf(preferred);
    }
}
