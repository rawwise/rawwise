package io.github.rawwise.frontend.backend;

import io.github.rawwise.backend.server.txn.TxnHandle;
import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.state.TxnResult;
import io.github.rawwise.frontend.ir.PointQueryIR;
import io.github.rawwise.frontend.ir.QueryIR;
import io.github.rawwise.frontend.ir.WriteQueryIR;
import io.github.rawwise.frontend.ir.routing.RoutingTable;
import io.github.rawwise.frontend.ir.routing.RoutingTableManager;
import io.github.rawwise.server.ClusterStatus;
import io.github.rawwise.server.JobState;
import io.github.rawwise.server.QueryType;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MultiBackendClient implements BackendClient {

    private final Map<String, GrpcBackendClient> clients;
    private final GrpcBackendClient fallback;
    private final RoutingTableManager routingManager;
    private final ConcurrentHashMap<String, GrpcBackendClient> jobGroupMap =
            new ConcurrentHashMap<>();
    private static final Pattern INSERT_PATTERN =
            Pattern.compile(
                    "INSERT\\s+INTO\\s+(\\S+)\\s*\\([^)]*\\)\\s*VALUES\\s*\\(\\s*(\\d+)",
                    Pattern.CASE_INSENSITIVE);

    public MultiBackendClient(
            Map<String, GrpcBackendClient> clients, RoutingTableManager routingManager) {
        this.clients = new ConcurrentHashMap<>(clients);
        this.fallback = clients.values().iterator().next();
        this.routingManager = routingManager;
    }

    @Override
    public SubmitResult submitQuery(String sql, QueryType type, Map<String, String> options) {
        return submitQuery(sql, type, options, 0, null);
    }

    @Override
    public SubmitResult submitQuery(
            String sql,
            QueryType type,
            Map<String, String> options,
            long txnId,
            String frontendSessionId) {
        if ((type == QueryType.DML || type == QueryType.SQL) && routingManager != null) {
            GrpcBackendClient routed = resolveInsertTarget(sql);
            if (routed != null && routed != fallback) {
                SubmitResult result =
                        routed.submitQuery(sql, type, options, txnId, frontendSessionId);
                if (result != null && result.jobId() != null) {
                    jobGroupMap.put(result.jobId(), routed);
                }
                return result;
            }
        }
        SubmitResult result = fallback.submitQuery(sql, type, options, txnId, frontendSessionId);
        if (result != null && result.jobId() != null) {
            if (jobGroupMap.size() >= JOB_GROUP_MAP_MAX_SIZE) {
                jobGroupMap.clear();
            }
            jobGroupMap.put(result.jobId(), fallback);
        }
        return result;
    }

    private GrpcBackendClient resolveInsertTarget(String sql) {
        Matcher m = INSERT_PATTERN.matcher(sql);
        if (!m.find()) {
            return null;
        }
        String tableName = m.group(1);
        long key = Long.parseLong(m.group(2));
        return routeByKey(tableName, key, "INSERT");
    }

    private GrpcBackendClient routeByKey(String tableName, long key, String op) {
        RoutingTable rt = routingManager.getRoutingTable(tableName);
        if (rt == null) {
            return null;
        }
        String groupId = rt.lookup(tableName, key).groupId();
        GrpcBackendClient routed = clients.getOrDefault(groupId, fallback);
        if (routed != fallback) {
            log.info("Routed {} table={} key={} -> groupId={}", op, tableName, key, groupId);
        }
        return routed;
    }

    @Override
    public SubmitIRResult submitIR(QueryIR ir, long timeoutMs) {
        GrpcBackendClient routed = resolveIRTarget(ir);
        GrpcBackendClient target = (routed != null && routed != fallback) ? routed : fallback;
        return target.submitIR(ir, timeoutMs);
    }

    private GrpcBackendClient resolveIRTarget(QueryIR ir) {
        if (routingManager == null) return null;

        if (ir instanceof WriteQueryIR wqi && wqi.tableMeta() != null) {
            String tableId = wqi.tableMeta().tableIdentifier();
            RoutingTable rt = routingManager.getRoutingTable(tableId);
            if (rt != null && !wqi.rows().isEmpty()) {
                Map<String, Comparable<?>> firstRow = wqi.rows().get(0);
                for (var entry : firstRow.entrySet()) {
                    if (entry.getValue() instanceof Number n) {
                        long key = n.longValue();
                        String groupId = rt.lookup(tableId, key).groupId();
                        GrpcBackendClient client = clients.get(groupId);
                        if (client != null && client != fallback) {
                            log.info(
                                    "Routed submitIR WRITE table={} key={} -> groupId={}",
                                    tableId,
                                    key,
                                    groupId);
                            return client;
                        }
                        break;
                    }
                }
            }
        } else if (ir instanceof PointQueryIR pqi && pqi.tableMeta() != null) {
            String tableId = pqi.tableMeta().tableIdentifier();
            RoutingTable rt = routingManager.getRoutingTable(tableId);
            if (rt != null) {
                for (var entry : pqi.pkValues().entrySet()) {
                    if (entry.getValue() instanceof Number n) {
                        long key = n.longValue();
                        String groupId = rt.lookup(tableId, key).groupId();
                        GrpcBackendClient client = clients.get(groupId);
                        if (client != null && client != fallback) {
                            log.info(
                                    "Routed submitIR POINT_QUERY table={} key={} -> groupId={}",
                                    tableId,
                                    key,
                                    groupId);
                            return client;
                        }
                        break;
                    }
                }
            }
        }
        return null;
    }

    private static final int JOB_GROUP_MAP_MAX_SIZE = 5000;

    @Override
    public JobState getJobStatus(String jobId) {
        GrpcBackendClient target = jobGroupMap.getOrDefault(jobId, fallback);
        JobState state = target.getJobStatus(jobId);
        if (state != null) {
            String stateStr = state.toString();
            if (stateStr.equals("SUCCEEDED")
                    || stateStr.equals("FAILED")
                    || stateStr.equals("CANCELLED")
                    || stateStr.equals("TIMED_OUT")) {
                jobGroupMap.remove(jobId);
            }
        }
        return state;
    }

    @Override
    public boolean cancelJob(String jobId) {
        GrpcBackendClient target = jobGroupMap.getOrDefault(jobId, fallback);
        boolean cancelled = target.cancelJob(jobId);
        if (cancelled) {
            jobGroupMap.remove(jobId);
        }
        return cancelled;
    }

    @Override
    public ClusterStatus getClusterStatus() {
        return fallback.getClusterStatus();
    }

    @Override
    public QueryResult getQueryResult(String jobId) {
        return jobGroupMap.getOrDefault(jobId, fallback).getQueryResult(jobId);
    }

    @Override
    public QueryResult getQueryResult(String jobId, int offset, int limit, int maxBytes) {
        return jobGroupMap
                .getOrDefault(jobId, fallback)
                .getQueryResult(jobId, offset, limit, maxBytes);
    }

    @Override
    public TxnHandle beginTxn(Set<String> tableIds, IsolationLevel level) {
        return fallback.beginTxn(tableIds, level);
    }

    @Override
    public TxnResult commitTxn(long txnId) {
        return fallback.commitTxn(txnId);
    }

    @Override
    public TxnResult abortTxn(long txnId) {
        return fallback.abortTxn(txnId);
    }

    public void close() {
        for (var client : clients.values()) {
            try {
                client.close();
            } catch (Exception e) {
                log.debug("Failed to close backend client", e);
            }
        }
    }
}
