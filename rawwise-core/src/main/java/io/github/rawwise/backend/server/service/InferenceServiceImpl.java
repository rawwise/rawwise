package io.github.rawwise.backend.server.service;

import io.github.rawwise.server.BatchInferenceRequest;
import io.github.rawwise.server.BatchInferenceResponse;
import io.github.rawwise.server.InferenceResponse;
import io.github.rawwise.server.InferenceServiceGrpc;
import io.github.rawwise.server.PointInferenceRequest;
import io.github.rawwise.server.VectorSearchRequest;
import io.github.rawwise.server.VectorSearchResponse;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InferenceServiceImpl extends InferenceServiceGrpc.InferenceServiceImplBase {

    public InferenceServiceImpl() {}

    @Override
    public void pointInference(
            PointInferenceRequest request, StreamObserver<InferenceResponse> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("InferenceService.pointInference not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void batchInference(
            BatchInferenceRequest request,
            StreamObserver<BatchInferenceResponse> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("InferenceService.batchInference not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void vectorSearch(
            VectorSearchRequest request, StreamObserver<VectorSearchResponse> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("InferenceService.vectorSearch not yet implemented")
                        .asRuntimeException());
    }
}
