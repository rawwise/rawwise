package io.github.rawwise.model.op;

import io.github.rawwise.meta.PbOperationResult;
import java.util.Optional;

public final class GrpcOpResult implements OpResult {

    private final PbOperationResult grpcResult;
    private final Throwable error;
    private final long timestamp;

    public GrpcOpResult(PbOperationResult grpcResult, Throwable error) {
        this.grpcResult = grpcResult;
        this.error = error;
        this.timestamp = System.currentTimeMillis();
    }

    public static OpResult of(PbOperationResult grpcResult) {
        return new GrpcOpResult(grpcResult, null);
    }

    public static OpResult error(Throwable error) {
        return new GrpcOpResult(null, error);
    }

    public static OpResult error(String message) {
        return new GrpcOpResult(null, new RuntimeException(message));
    }

    @Override
    public boolean success() {
        if (error != null) {
            return false;
        }
        return grpcResult != null && grpcResult.getSuccess();
    }

    @Override
    public Optional<String> message() {
        if (error != null) {
            return Optional.of(error.getMessage());
        }
        if (grpcResult != null && !grpcResult.getMessage().isEmpty()) {
            return Optional.of(grpcResult.getMessage());
        }
        return Optional.empty();
    }

    @Override
    public Optional<Throwable> error() {
        return Optional.ofNullable(error);
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    public Optional<PbOperationResult> getGrpcResult() {
        return Optional.ofNullable(grpcResult);
    }

    public boolean isNetworkError() {
        return error != null && grpcResult == null;
    }
}
