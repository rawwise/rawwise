package io.github.rawwise.backend.server.service;

import com.google.protobuf.Duration;
import com.google.protobuf.Empty;
import io.github.rawwise.backend.replication.CentroidSink;
import io.github.rawwise.backend.routing.NodeSlot;
import io.github.rawwise.backend.routing.RoutingCoordinator;
import io.github.rawwise.backend.routing.RoutingEpoch;
import io.github.rawwise.backend.routing.ShardAssignment;
import io.github.rawwise.backend.routing.ShardId;
import io.github.rawwise.backend.routing.ShardLookupResult;
import io.github.rawwise.backend.server.job.JobScheduler;
import io.github.rawwise.backend.server.load.BackendLoadInfo;
import io.github.rawwise.backend.server.load.BackpressureManager;
import io.github.rawwise.engine.vector.ShardCentroids;
import io.github.rawwise.server.ApplicationMetrics;
import io.github.rawwise.server.CentroidReportRequest;
import io.github.rawwise.server.CentroidReportResponse;
import io.github.rawwise.server.ClusterManagementServiceGrpc;
import io.github.rawwise.server.ClusterStatus;
import io.github.rawwise.server.EnvironmentConf;
import io.github.rawwise.server.ExecutorStatus;
import io.github.rawwise.server.HeartbeatRequest;
import io.github.rawwise.server.HeartbeatResponse;
import io.github.rawwise.server.RegisterExecutorRequest;
import io.github.rawwise.server.RegisterExecutorResponse;
import io.github.rawwise.server.ResourceRequest;
import io.github.rawwise.server.Status;
import io.grpc.stub.StreamObserver;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClusterManagementServiceImpl
        extends ClusterManagementServiceGrpc.ClusterManagementServiceImplBase {

    private final JobScheduler jobScheduler;
    private final BackpressureManager backpressureManager;
    private final String schedulerMode;
    private final RoutingCoordinator routingCoordinator;
    private CentroidSink centroidSink;
    private final ConcurrentHashMap<String, Long> heartbeatRecords = new ConcurrentHashMap<>();
    private final AtomicLong epochCounter = new AtomicLong(0);

    public ClusterManagementServiceImpl(
            JobScheduler jobScheduler,
            BackpressureManager backpressureManager,
            String schedulerMode) {
        this(jobScheduler, backpressureManager, schedulerMode, null);
    }

    public ClusterManagementServiceImpl(
            JobScheduler jobScheduler,
            BackpressureManager backpressureManager,
            String schedulerMode,
            RoutingCoordinator routingCoordinator) {
        this.jobScheduler = jobScheduler;
        this.backpressureManager = backpressureManager;
        this.schedulerMode = schedulerMode != null ? schedulerMode : "FIFO";
        this.routingCoordinator = routingCoordinator;
    }

    public void setCentroidSink(CentroidSink sink) {
        this.centroidSink = sink;
    }

    @Override
    public void registerExecutor(
            RegisterExecutorRequest request,
            StreamObserver<RegisterExecutorResponse> responseObserver) {
        try {
            if (routingCoordinator != null) {
                ShardId shardId = new ShardId(request.getShardId());
                NodeSlot slot = new NodeSlot(request.getNodeId(), request.getSlotIndex());
                routingCoordinator.onExecutorUp(request.getExecutorId(), slot);
                routingCoordinator.onExecutorReady(request.getExecutorId());
                ShardLookupResult currentLookup = routingCoordinator.lookupOwner(shardId);
                long currentEpochValue =
                        currentLookup.hasAssignment()
                                ? currentLookup.assignment().epoch().value()
                                : 0L;
                long epoch = Math.max(epochCounter.incrementAndGet(), currentEpochValue + 1);
                epochCounter.set(epoch);
                ShardAssignment assignment =
                        new ShardAssignment(
                                shardId, slot, request.getExecutorId(), new RoutingEpoch(epoch));
                routingCoordinator.assignShard(assignment);
                heartbeatRecords.put(request.getExecutorId(), System.currentTimeMillis());

                RegisterExecutorResponse response =
                        RegisterExecutorResponse.newBuilder()
                                .setStatus(Status.newBuilder().setCode(Status.Code.OK).build())
                                .setEpoch(epoch)
                                .build();
                responseObserver.onNext(response);
            } else {
                long epoch = epochCounter.incrementAndGet();
                RegisterExecutorResponse response =
                        RegisterExecutorResponse.newBuilder()
                                .setStatus(Status.newBuilder().setCode(Status.Code.OK).build())
                                .setEpoch(epoch)
                                .build();
                responseObserver.onNext(response);
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error registering executor", e);
            responseObserver.onNext(
                    RegisterExecutorResponse.newBuilder()
                            .setStatus(Status.newBuilder().setCode(Status.Code.UNKNOWN).build())
                            .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void heartbeat(
            HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        try {
            heartbeatRecords.put(request.getExecutorId(), System.currentTimeMillis());

            HeartbeatResponse response =
                    HeartbeatResponse.newBuilder()
                            .setStatus(Status.newBuilder().setCode(Status.Code.OK).build())
                            .setServerTimestampMs(System.currentTimeMillis())
                            .setNextHeartbeatIntervalMs(5000L)
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error processing heartbeat", e);
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("Heartbeat failed: " + e.getMessage())
                            .asRuntimeException());
        }
    }

    @Override
    public void reportCentroid(
            CentroidReportRequest request,
            StreamObserver<CentroidReportResponse> responseObserver) {
        try {
            if (centroidSink == null) {
                responseObserver.onNext(
                        CentroidReportResponse.newBuilder()
                                .setAccepted(false)
                                .setError("CentroidSink not configured")
                                .build());
                responseObserver.onCompleted();
                return;
            }
            if (request.getVectorCount() <= 0) {
                responseObserver.onNext(
                        CentroidReportResponse.newBuilder()
                                .setAccepted(false)
                                .setError("vector_count must be > 0")
                                .build());
                responseObserver.onCompleted();
                return;
            }

            int dim = request.getCentroidCount();
            float[] centroid = new float[dim];
            for (int i = 0; i < dim; i++) {
                centroid[i] = request.getCentroid(i);
            }
            ShardCentroids sc =
                    new ShardCentroids(
                            String.valueOf(request.getShardId()),
                            1,
                            new float[][] {centroid},
                            request.getVectorCount(),
                            1,
                            request.getTimestamp());
            centroidSink.save(sc);

            responseObserver.onNext(CentroidReportResponse.newBuilder().setAccepted(true).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error reporting centroid", e);
            responseObserver.onNext(
                    CentroidReportResponse.newBuilder()
                            .setAccepted(false)
                            .setError(e.getMessage())
                            .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getClusterStatus(Empty request, StreamObserver<ClusterStatus> responseObserver) {
        try {
            BackendLoadInfo loadInfo = backpressureManager.getCurrentLoad();

            long totalMemoryMb = Runtime.getRuntime().maxMemory() / (1024 * 1024);
            int totalCores = Runtime.getRuntime().availableProcessors();

            ClusterStatus clusterStatus =
                    ClusterStatus.newBuilder()
                            .addExecutors(
                                    ExecutorStatus.newBuilder()
                                            .setId(
                                                    "backend-"
                                                            + UUID.randomUUID()
                                                                    .toString()
                                                                    .substring(0, 8))
                                            .setState("ALIVE")
                                            .setMemoryTotalMb(totalMemoryMb)
                                            .setMemoryUsedMb(
                                                    (long)
                                                            (loadInfo.getMemoryUsage()
                                                                    * totalMemoryMb))
                                            .setCoresTotal(totalCores)
                                            .setCoresUsed(
                                                    (int) (loadInfo.getCpuUsage() * totalCores)))
                            .setApplicationId("rawwise-backend")
                            .setApplicationState("RUNNING")
                            .setNumRunningJobs(jobScheduler.getQueueLength())
                            .setNumPendingJobs(loadInfo.getQueueLength())
                            .setTotalMemoryMb(totalMemoryMb)
                            .setUsedMemoryMb((long) (loadInfo.getMemoryUsage() * totalMemoryMb))
                            .setTotalCores(totalCores)
                            .setUsedCores((int) (loadInfo.getCpuUsage() * totalCores))
                            .setSchedulerMode(schedulerMode)
                            .build();

            responseObserver.onNext(clusterStatus);
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Error getting cluster status", e);
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("Failed to get cluster status: " + e.getMessage())
                            .asRuntimeException());
        }
    }

    @Override
    public void getExecutorInfo(Empty request, StreamObserver<ExecutorStatus> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription(
                                "ClusterManagementService.getExecutorInfo not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void getApplicationMetrics(
            Empty request, StreamObserver<ApplicationMetrics> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription(
                                "ClusterManagementService.getApplicationMetrics not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void getDriverLogs(
            Empty request, StreamObserver<io.github.rawwise.server.LogMessage> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription(
                                "ClusterManagementService.getDriverLogs not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void getEnvironmentConfig(
            Empty request, StreamObserver<EnvironmentConf> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription(
                                "ClusterManagementService.getEnvironmentConfig not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void adjustResources(ResourceRequest request, StreamObserver<Status> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription(
                                "ClusterManagementService.adjustResources not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void restartExecutor(ExecutorStatus request, StreamObserver<Status> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription(
                                "ClusterManagementService.restartExecutor not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void gracefulShutdown(Duration request, StreamObserver<Status> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription(
                                "ClusterManagementService.gracefulShutdown not yet implemented")
                        .asRuntimeException());
    }

    public Long getLastHeartbeatMs(String executorId) {
        return heartbeatRecords.get(executorId);
    }

    public void removeHeartbeat(String executorId) {
        heartbeatRecords.remove(executorId);
    }

    public java.util.Set<String> getRegisteredExecutorIds() {
        return java.util.Collections.unmodifiableSet(heartbeatRecords.keySet());
    }
}
