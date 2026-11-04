package io.github.rawwise.backend.spark.routing;

import io.github.rawwise.backend.routing.RoutingStatus;
import io.github.rawwise.backend.routing.ShardAssignment;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.routing.ShardLookupResult;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.rpc.RpcAddress;
import org.apache.spark.rpc.RpcEndpointRef;
import org.apache.spark.rpc.RpcTimeout;
import org.apache.spark.util.RpcUtils;

public final class RoutingWriteClient {

    private final RpcEndpointRef driverEndpoint;
    private final int maxRetries;
    private final long backoffMs;

    public RoutingWriteClient() {
        SparkConf conf = SparkEnv.get().conf();
        this.driverEndpoint =
                RpcUtils.makeDriverRef(
                        RawWiseDriverRoutingEndpoint.ENDPOINT_NAME, conf, SparkEnv.get().rpcEnv());
        this.maxRetries = conf.getInt("spark.rawwise.routing.forward.maxRetries", 3);
        this.backoffMs = conf.getLong("spark.rawwise.routing.forward.backoffMs", 100L);
    }

    public RoutingMessages.WriteAck forward(
            String basePath,
            String storeId,
            String tableIdentifier,
            int shardId,
            long epoch,
            String batchId,
            byte[] key,
            byte[] value) {
        RoutingMessages.ShardWriteRequest request =
                new RoutingMessages.ShardWriteRequest(
                        basePath,
                        storeId,
                        tableIdentifier,
                        shardId,
                        epoch,
                        batchId,
                        new byte[][] {key},
                        new byte[][] {value});
        return askWithRetry(request);
    }

    public RoutingMessages.WriteAck forwardMultiShard(
            String basePath,
            String storeId,
            String tableIdentifier,
            int[] shardIds,
            long[] epochs,
            String batchId,
            byte[][] keys,
            byte[][] values,
            String taskAttemptId) {
        RoutingMessages.MultiShardWriteRequest request =
                new RoutingMessages.MultiShardWriteRequest(
                        basePath,
                        storeId,
                        tableIdentifier,
                        shardIds,
                        epochs,
                        batchId,
                        keys,
                        values,
                        taskAttemptId);
        return askWithRetry(request);
    }

    public RoutingMessages.WriteAck forwardMultiShard(
            String basePath,
            String storeId,
            String tableIdentifier,
            int[] shardIds,
            long[] epochs,
            String batchId,
            byte[][] keys,
            byte[][] values) {
        return forwardMultiShard(
                basePath, storeId, tableIdentifier, shardIds, epochs, batchId, keys, values, null);
    }

    public RoutingMessages.ShardPrepareTxnResult prepareShardTxn(
            String basePath,
            String storeId,
            String tableIdentifier,
            int shardId,
            long epoch,
            String taskAttemptId) {
        RoutingMessages.ShardPrepareTxn prepare =
                new RoutingMessages.ShardPrepareTxn(
                        basePath, storeId, tableIdentifier, shardId, epoch, taskAttemptId);
        try {
            RpcTimeout timeout = RpcUtils.askRpcTimeout(SparkEnv.get().conf());
            Object reply =
                    driverEndpoint.askSync(
                            prepare, timeout, scala.reflect.ClassTag$.MODULE$.apply(Object.class));
            if (reply instanceof RoutingMessages.ShardPrepareTxnResult result) {
                return result;
            }
            return RoutingMessages.ShardPrepareTxnResult.error(0L, "invalid_reply");
        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            return RoutingMessages.ShardPrepareTxnResult.error(0L, "forward_failed: " + msg);
        }
    }

    public RoutingMessages.ShardCommitTxnResult commitShardTxn(
            String basePath,
            String storeId,
            String tableIdentifier,
            int shardId,
            long epoch,
            String taskAttemptId,
            long txnId) {
        RoutingMessages.ShardCommitTxn commit =
                new RoutingMessages.ShardCommitTxn(
                        basePath, storeId, tableIdentifier, shardId, epoch, taskAttemptId, txnId);
        try {
            RpcTimeout timeout = RpcUtils.askRpcTimeout(SparkEnv.get().conf());
            Object reply =
                    driverEndpoint.askSync(
                            commit, timeout, scala.reflect.ClassTag$.MODULE$.apply(Object.class));
            if (reply instanceof RoutingMessages.ShardCommitTxnResult result) {
                return result;
            }
            return RoutingMessages.ShardCommitTxnResult.error(txnId, "invalid_reply");
        } catch (Exception e) {
            String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            return RoutingMessages.ShardCommitTxnResult.error(txnId, "forward_failed: " + msg);
        }
    }

    public void abortShardTxn(
            String basePath,
            String storeId,
            String tableIdentifier,
            int shardId,
            long epoch,
            String taskAttemptId,
            long txnId) {
        RoutingMessages.ShardAbortTxn abort =
                new RoutingMessages.ShardAbortTxn(
                        basePath, storeId, tableIdentifier, shardId, epoch, taskAttemptId, txnId);
        try {
            driverEndpoint.send(abort);
        } catch (Exception e) {
            // Best-effort, abort notification may not reach driver
        }
    }

    public static String newBatchId() {
        return UUID.randomUUID().toString();
    }

    public ShardLookupResult lookupOwner(int shardId) {
        RpcTimeout timeout = RpcUtils.askRpcTimeout(SparkEnv.get().conf());
        Object reply =
                driverEndpoint.askSync(
                        new RoutingMessages.LookupShardOwnerMessage(shardId),
                        timeout,
                        scala.reflect.ClassTag$.MODULE$.apply(Object.class));
        if (reply instanceof ShardLookupResult result) {
            return result;
        }
        throw new IllegalStateException("Invalid lookup reply");
    }

    public Map<Integer, ShardLookupResult> lookupOwners(Set<Integer> shardIds) {
        RpcTimeout timeout = RpcUtils.askRpcTimeout(SparkEnv.get().conf());
        Object reply =
                driverEndpoint.askSync(
                        new RoutingMessages.LookupShardOwnersMessage(shardIds),
                        timeout,
                        scala.reflect.ClassTag$.MODULE$.apply(Object.class));
        if (reply instanceof Map<?, ?> map) {
            // Driver returns Map<ShardId, ShardLookupResult>.
            java.util.HashMap<Integer, ShardLookupResult> out = new java.util.HashMap<>();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                if (e.getKey() instanceof ShardId sid
                        && e.getValue() instanceof ShardLookupResult r) {
                    out.put(sid.value(), r);
                }
            }
            return out;
        }
        throw new IllegalStateException("Invalid lookupOwners reply");
    }

    public RoutingMessages.ExecutorEndpointInfo lookupExecutorEndpoint(String executorId) {
        RpcTimeout timeout = RpcUtils.askRpcTimeout(SparkEnv.get().conf());
        Object reply =
                driverEndpoint.askSync(
                        new RoutingMessages.LookupExecutorEndpointMessage(executorId),
                        timeout,
                        scala.reflect.ClassTag$.MODULE$.apply(Object.class));
        if (reply == null) {
            return null;
        }
        if (reply instanceof RoutingMessages.ExecutorEndpointInfo info) {
            return info;
        }
        throw new IllegalStateException("Invalid lookupExecutorEndpoint reply");
    }

    public RoutingMessages.WriteAck directForwardToExecutor(
            RoutingMessages.ExecutorEndpointInfo endpointInfo,
            RoutingMessages.ShardWriteForward forward) {
        if (endpointInfo == null) {
            return RoutingMessages.WriteAck.retry("executor_endpoint_missing");
        }
        RpcEndpointRef target =
                SparkEnv.get()
                        .rpcEnv()
                        .setupEndpointRef(
                                RpcAddress.apply(endpointInfo.host(), endpointInfo.port()),
                                endpointInfo.endpointName());
        RpcTimeout timeout = RpcUtils.askRpcTimeout(SparkEnv.get().conf());
        Object reply =
                target.askSync(
                        forward, timeout, scala.reflect.ClassTag$.MODULE$.apply(Object.class));
        if (reply instanceof RoutingMessages.WriteAck ack) {
            return ack;
        }
        return RoutingMessages.WriteAck.error("invalid_reply");
    }

    public static boolean canFastPath(ShardLookupResult result, String currentExecutorId) {
        if (result == null || currentExecutorId == null) {
            return false;
        }
        if (result.status() != RoutingStatus.ASSIGNED) {
            return false;
        }
        ShardAssignment assignment = result.assignment();
        return assignment != null && currentExecutorId.equals(assignment.executorId());
    }

    private RoutingMessages.WriteAck askWithRetry(Object request) {
        Exception last = null;
        int attempts = Math.max(1, maxRetries);
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                RpcTimeout timeout = RpcUtils.askRpcTimeout(SparkEnv.get().conf());
                Object reply =
                        driverEndpoint.askSync(
                                request,
                                timeout,
                                scala.reflect.ClassTag$.MODULE$.apply(Object.class));
                if (reply instanceof RoutingMessages.WriteAck ack) {
                    return ack;
                }
                return RoutingMessages.WriteAck.error("invalid_reply");
            } catch (Exception e) {
                last = e;
                if (attempt == attempts) {
                    break;
                }
                sleepQuietly(backoffMs * attempt);
            }
        }
        return RoutingMessages.WriteAck.error(
                "max_retries_exceeded: " + (last != null ? last.getMessage() : "unknown"));
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(Math.max(0L, millis));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while forwarding write", e);
        }
    }
}
