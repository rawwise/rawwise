package io.github.rawwise.backend.executor.service;

import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestrator;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnResult;
import io.github.rawwise.txn.AbortBatchRequest;
import io.github.rawwise.txn.AbortBatchResponse;
import io.github.rawwise.txn.AbortTxnRequest;
import io.github.rawwise.txn.AbortTxnResponse;
import io.github.rawwise.txn.BeginBatchRequest;
import io.github.rawwise.txn.BeginBatchResponse;
import io.github.rawwise.txn.BeginTxnRequest;
import io.github.rawwise.txn.BeginTxnResponse;
import io.github.rawwise.txn.CommitBatchRequest;
import io.github.rawwise.txn.CommitBatchResponse;
import io.github.rawwise.txn.CommitTxnRequest;
import io.github.rawwise.txn.CommitTxnResponse;
import io.github.rawwise.txn.QueryBatchStatusRequest;
import io.github.rawwise.txn.QueryBatchStatusResponse;
import io.github.rawwise.txn.TxnCoordinatorServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class TxnCoordinatorGrpcService
        extends TxnCoordinatorServiceGrpc.TxnCoordinatorServiceImplBase {

    private final TxnOrchestrator orchestrator;
    private final ConcurrentHashMap<Long, TxnContext> activeTxnContexts = new ConcurrentHashMap<>();
    private volatile long acceptedEpoch = 0;

    public TxnCoordinatorGrpcService(TxnOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    public TxnContext getActiveContext(long txnId) {
        return activeTxnContexts.get(txnId);
    }

    @Override
    public void beginTxn(
            BeginTxnRequest request, StreamObserver<BeginTxnResponse> responseObserver) {
        Set<String> tableIds = Set.copyOf(request.getTableIdsList());
        IsolationLevel level = IsolationLevel.fromString(request.getIsolationLevel());

        log.info("beginTxn: tables={} level={}", tableIds, level);

        try {
            TxnContext ctx = orchestrator.begin(tableIds, level);
            activeTxnContexts.put(ctx.txnId(), ctx);

            Map<String, String> nodeAssignments = new HashMap<>();
            for (String tableId : tableIds) {
                nodeAssignments.put(tableId, "local");
            }

            BeginTxnResponse response =
                    BeginTxnResponse.newBuilder()
                            .setTxnId(ctx.txnId())
                            .putAllNodeAssignments(nodeAssignments)
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("beginTxn: failed", e);
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("BeginTxn failed: " + e.getMessage())
                            .asRuntimeException());
        }
    }

    @Override
    public void commitTxn(
            CommitTxnRequest request, StreamObserver<CommitTxnResponse> responseObserver) {
        long txnId = request.getTxnId();
        log.info("commitTxn: txnId={}", txnId);

        try {
            TxnContext ctx = activeTxnContexts.remove(txnId);
            if (ctx == null) {
                responseObserver.onError(
                        io.grpc.Status.NOT_FOUND
                                .withDescription("Unknown txnId: " + txnId)
                                .asRuntimeException());
                return;
            }

            TxnResult result = orchestrator.commit(ctx);

            CommitTxnResponse.Builder builder = CommitTxnResponse.newBuilder();
            if (result instanceof TxnResult.CommitOk ok) {
                builder.setSuccess(true).setCommitTs(ok.commitTs());
            } else if (result instanceof TxnResult.CommitConflict conflict) {
                builder.setSuccess(false).setConflictReason(conflict.reason());
            } else if (result instanceof TxnResult.AbortResult abort) {
                builder.setSuccess(false).setConflictReason("Aborted: " + abort.reason());
            } else {
                builder.setSuccess(false).setConflictReason("Unexpected result: " + result);
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("commitTxn: txnId={} failed", txnId, e);
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("CommitTxn failed: " + e.getMessage())
                            .asRuntimeException());
        }
    }

    @Override
    public void abortTxn(
            AbortTxnRequest request, StreamObserver<AbortTxnResponse> responseObserver) {
        long txnId = request.getTxnId();
        log.info("abortTxn: txnId={}", txnId);

        try {
            TxnContext ctx = activeTxnContexts.remove(txnId);
            if (ctx == null) {
                AbortTxnResponse response =
                        AbortTxnResponse.newBuilder()
                                .setSuccess(false)
                                .setReason("Unknown txnId: " + txnId)
                                .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            orchestrator.abort(ctx);

            AbortTxnResponse response =
                    AbortTxnResponse.newBuilder().setSuccess(true).setReason("aborted").build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("abortTxn: txnId={} failed", txnId, e);
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("AbortTxn failed: " + e.getMessage())
                            .asRuntimeException());
        }
    }

    @Override
    public void beginBatch(
            BeginBatchRequest request, StreamObserver<BeginBatchResponse> responseObserver) {
        String batchId = request.getBatchId();
        String tableId = request.getTableId();
        int expectedShardCount = request.getExpectedShardCount();
        long epoch = request.getEpoch();

        log.info(
                "beginBatch: batchId={} tableId={} expectedShardCount={} epoch={}",
                batchId,
                tableId,
                expectedShardCount,
                epoch);

        if (epoch != 0) {
            if (epoch < acceptedEpoch) {
                log.warn(
                        "beginBatch: STALE_EPOCH rejected, batchId={} epoch={} acceptedEpoch={}",
                        batchId,
                        epoch,
                        acceptedEpoch);
                responseObserver.onError(
                        io.grpc.Status.FAILED_PRECONDITION
                                .withDescription(
                                        "STALE_EPOCH: request epoch "
                                                + epoch
                                                + " < accepted epoch "
                                                + acceptedEpoch)
                                .asRuntimeException());
                return;
            }
            acceptedEpoch = epoch;
        }

        try {
            BeginBatchResponse response =
                    BeginBatchResponse.newBuilder().setBatchId(batchId).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("beginBatch: batchId={} failed", batchId, e);
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("BeginBatch failed: " + e.getMessage())
                            .asRuntimeException());
        }
    }

    @Override
    public void commitBatch(
            CommitBatchRequest request, StreamObserver<CommitBatchResponse> responseObserver) {
        String batchId = request.getBatchId();
        String tableId = request.getTableId();
        String idempotencyKey = request.getIdempotencyKey();
        int expectedShardCount = request.getExpectedShardCount();

        log.info(
                "commitBatch: batchId={} tableId={} idempotencyKey={}",
                batchId,
                tableId,
                idempotencyKey);

        try {
            TxnOrchestrator.BatchCommitRequest batchRequest =
                    new TxnOrchestrator.BatchCommitRequest(
                            tableId, batchId, idempotencyKey, 0, expectedShardCount, null, null);

            TxnResult result = orchestrator.commitBatch(batchRequest);

            CommitBatchResponse.Builder builder = CommitBatchResponse.newBuilder();
            if (result instanceof TxnResult.CommitOk ok) {
                builder.setSuccess(true).setCommitTs(ok.commitTs());
            } else if (result instanceof TxnResult.CommitConflict conflict) {
                builder.setSuccess(false).setConflictReason(conflict.reason());
            } else if (result instanceof TxnResult.AbortResult abort) {
                builder.setSuccess(false).setConflictReason("Aborted: " + abort.reason());
            } else {
                builder.setSuccess(false).setConflictReason("Unexpected result: " + result);
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("commitBatch: batchId={} failed", batchId, e);
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("CommitBatch failed: " + e.getMessage())
                            .asRuntimeException());
        }
    }

    @Override
    public void abortBatch(
            AbortBatchRequest request, StreamObserver<AbortBatchResponse> responseObserver) {
        String idempotencyKey = request.getIdempotencyKey();
        log.info("abortBatch: idempotencyKey={}", idempotencyKey);

        try {
            TxnResult result = orchestrator.abortBatch(idempotencyKey);

            AbortBatchResponse.Builder builder = AbortBatchResponse.newBuilder();
            if (result instanceof TxnResult.AbortResult) {
                builder.setSuccess(true).setReason("aborted");
            } else if (result instanceof TxnResult.CommitConflict conflict) {
                builder.setSuccess(false).setReason(conflict.reason());
            } else {
                builder.setSuccess(true).setReason("aborted");
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("abortBatch: idempotencyKey={} failed", idempotencyKey, e);
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("AbortBatch failed: " + e.getMessage())
                            .asRuntimeException());
        }
    }

    @Override
    public void queryBatchStatus(
            QueryBatchStatusRequest request,
            StreamObserver<QueryBatchStatusResponse> responseObserver) {
        String idempotencyKey = request.getIdempotencyKey();
        log.info("queryBatchStatus: idempotencyKey={}", idempotencyKey);

        try {
            String recoveryDecision = orchestrator.decideBatchRecovery(idempotencyKey);

            QueryBatchStatusResponse response =
                    QueryBatchStatusResponse.newBuilder()
                            .setState(recoveryDecision)
                            .setDescription(recoveryDecision)
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("queryBatchStatus: idempotencyKey={} failed", idempotencyKey, e);
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("QueryBatchStatus failed: " + e.getMessage())
                            .asRuntimeException());
        }
    }
}
