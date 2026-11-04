package io.github.rawwise.backend.server.service;

import io.github.rawwise.server.AcquireBlockLeaseRequest;
import io.github.rawwise.server.BindCheckpointRequest;
import io.github.rawwise.server.BlockLeaseResponse;
import io.github.rawwise.server.CheckpointBindingResponse;
import io.github.rawwise.server.SplitPlanRequest;
import io.github.rawwise.server.SplitPlanResponse;
import io.github.rawwise.server.TrainingDataServiceGrpc;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TrainingDataServiceImpl extends TrainingDataServiceGrpc.TrainingDataServiceImplBase {

    public TrainingDataServiceImpl() {}

    @Override
    public void getSplitPlan(
            SplitPlanRequest request, StreamObserver<SplitPlanResponse> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("TrainingDataService.getSplitPlan not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void acquireBlockLease(
            AcquireBlockLeaseRequest request, StreamObserver<BlockLeaseResponse> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription(
                                "TrainingDataService.acquireBlockLease not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void bindCheckpoint(
            BindCheckpointRequest request,
            StreamObserver<CheckpointBindingResponse> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("TrainingDataService.bindCheckpoint not yet implemented")
                        .asRuntimeException());
    }
}
