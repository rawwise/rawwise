package io.github.rawwise.backend.server.txn;

import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.state.TxnResult;
import io.github.rawwise.txn.AbortTxnRequest;
import io.github.rawwise.txn.AbortTxnResponse;
import io.github.rawwise.txn.BeginTxnRequest;
import io.github.rawwise.txn.BeginTxnResponse;
import io.github.rawwise.txn.CommitTxnRequest;
import io.github.rawwise.txn.CommitTxnResponse;
import io.github.rawwise.txn.TxnCoordinatorServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ServerTxnCoordinatorGrpcService
        extends TxnCoordinatorServiceGrpc.TxnCoordinatorServiceImplBase {

    private final ServerTxnCoordinator coordinator;

    public ServerTxnCoordinatorGrpcService(ServerTxnCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public void beginTxn(
            BeginTxnRequest request, StreamObserver<BeginTxnResponse> responseObserver) {
        Set<String> tableIds = Set.copyOf(request.getTableIdsList());
        IsolationLevel level = IsolationLevel.fromString(request.getIsolationLevel());

        log.info("ServerTxnCoordinator.beginTxn: tables={} level={}", tableIds, level);

        try {
            TxnHandle handle = coordinator.beginTxn(tableIds, level);

            BeginTxnResponse response =
                    BeginTxnResponse.newBuilder()
                            .setTxnId(handle.txnId())
                            .putAllNodeAssignments(handle.nodeAssignments())
                            .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("beginTxn failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("beginTxn failed: " + e.getMessage())
                            .asRuntimeException());
        }
    }

    @Override
    public void commitTxn(
            CommitTxnRequest request, StreamObserver<CommitTxnResponse> responseObserver) {
        long txnId = request.getTxnId();
        log.info("ServerTxnCoordinator.commitTxn: txnId={}", txnId);

        try {
            TxnResult result = coordinator.commitTxn(txnId);

            if (result.isSuccess()) {
                long commitTs = ((TxnResult.CommitOk) result).commitTs();
                responseObserver.onNext(
                        CommitTxnResponse.newBuilder()
                                .setSuccess(true)
                                .setCommitTs(commitTs)
                                .build());
            } else {
                responseObserver.onNext(
                        CommitTxnResponse.newBuilder()
                                .setSuccess(false)
                                .setConflictReason(result.message())
                                .build());
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("commitTxn failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("commitTxn failed: " + e.getMessage())
                            .asRuntimeException());
        }
    }

    @Override
    public void abortTxn(
            AbortTxnRequest request, StreamObserver<AbortTxnResponse> responseObserver) {
        long txnId = request.getTxnId();
        log.info("ServerTxnCoordinator.abortTxn: txnId={}", txnId);

        try {
            TxnResult result = coordinator.abortTxn(txnId);

            responseObserver.onNext(
                    AbortTxnResponse.newBuilder()
                            .setSuccess(result.isSuccess())
                            .setReason(result.message())
                            .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("abortTxn failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("abortTxn failed: " + e.getMessage())
                            .asRuntimeException());
        }
    }
}
