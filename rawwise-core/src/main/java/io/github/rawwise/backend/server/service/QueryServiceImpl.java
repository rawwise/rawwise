package io.github.rawwise.backend.server.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.ByteString;
import io.github.rawwise.backend.server.auth.AuthServerInterceptor;
import io.github.rawwise.backend.server.auth.AuthorizationException;
import io.github.rawwise.backend.server.auth.AuthorizedQuery;
import io.github.rawwise.backend.server.auth.Principal;
import io.github.rawwise.backend.server.auth.QueryAuthorizer;
import io.github.rawwise.backend.server.job.ExternalResult;
import io.github.rawwise.backend.server.job.InlineResult;
import io.github.rawwise.backend.server.job.JobResultHandle;
import io.github.rawwise.backend.server.job.JobScheduler;
import io.github.rawwise.backend.server.job.JobStatus;
import io.github.rawwise.backend.server.job.JobType;
import io.github.rawwise.backend.server.job.LeasedResult;
import io.github.rawwise.backend.server.job.ManagedJob;
import io.github.rawwise.backend.server.load.BackpressureManager;
import io.github.rawwise.backend.server.session.SessionStore;
import io.github.rawwise.frontend.ir.QueryIR;
import io.github.rawwise.frontend.ir.proto.FrontendIrProto;
import io.github.rawwise.frontend.ir.serializer.IRProtobufSerializer;
import io.github.rawwise.server.JobState;
import io.github.rawwise.server.QueryRequest;
import io.github.rawwise.server.QueryResponse;
import io.github.rawwise.server.QueryServiceGrpc;
import io.github.rawwise.server.QueryType;
import io.github.rawwise.server.ResultChunk;
import io.github.rawwise.server.Status;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryServiceImpl extends QueryServiceGrpc.QueryServiceImplBase {

    private static final int CHUNK_SIZE_BYTES = 1024 * 1024;

    private final Cache<String, String> requestIdCache =
            Caffeine.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();

    private final SessionStore sessionStore;
    private final JobScheduler jobScheduler;
    private final BackpressureManager backpressureManager;
    private final QueryAuthorizer queryAuthorizer;

    public QueryServiceImpl(
            SessionStore sessionStore,
            JobScheduler jobScheduler,
            BackpressureManager backpressureManager,
            QueryAuthorizer queryAuthorizer) {
        this.sessionStore = sessionStore;
        this.jobScheduler = jobScheduler;
        this.backpressureManager = backpressureManager;
        this.queryAuthorizer = queryAuthorizer;
    }

    @Override
    public void submitQuery(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        // In no-spark mode, backpressureManager is null; skip backpressure check
        BackpressureManager.BackpressureStatus bpStatus =
                backpressureManager != null
                        ? backpressureManager.getStatus()
                        : BackpressureManager.BackpressureStatus.NORMAL;

        if (bpStatus == BackpressureManager.BackpressureStatus.REJECTING) {
            log.warn("Rejecting query due to backpressure: {}", bpStatus);
            responseObserver.onNext(
                    buildErrorResponse(
                            null,
                            Status.Code.RESOURCE_EXHAUSTED,
                            "Server is under high load, please retry later"));
            responseObserver.onCompleted();
            return;
        }

        if (backpressureManager != null && backpressureManager.shouldThrottle()) {
            responseObserver.onError(
                    io.grpc.Status.UNAVAILABLE
                            .withDescription(
                                    "Server throttled, retry after "
                                            + backpressureManager.retryAfterSeconds()
                                            + "s")
                            .asRuntimeException());
            return;
        }

        if (bpStatus == BackpressureManager.BackpressureStatus.WARNING) {
            log.warn("Processing query under backpressure warning: {}", bpStatus);
        }

        String requestId = request.getId();
        if (requestId != null && !requestId.isEmpty()) {
            String cachedJobId = requestIdCache.getIfPresent(requestId);
            if (cachedJobId != null) {
                responseObserver.onNext(
                        QueryResponse.newBuilder()
                                .setRequestId(requestId)
                                .setJobId(cachedJobId)
                                .setState(JobState.SUBMITTED)
                                .build());
                responseObserver.onCompleted();
                return;
            }
        }

        try {
            Principal principal = AuthServerInterceptor.PRINCIPAL_KEY.get(Context.current());
            String sql = request.getSql();
            Map<String, String> options = new HashMap<>();
            if (request.getParametersCount() > 0) {
                options.putAll(request.getParametersMap());
            }

            if (principal != null) {
                options.put("tenantId", principal.tenantId());
                if (queryAuthorizer != null) {
                    AuthorizedQuery authorized = queryAuthorizer.authorize(principal, sql, options);
                    sql = authorized.sql();
                    options = authorized.options();
                    options.put("tenantId", principal.tenantId());
                }
            }

            if (request.getTxnId() > 0) {
                options.put("txnId", String.valueOf(request.getTxnId()));
                log.info(
                        "Query with txnId={} frontendSessionId={}",
                        request.getTxnId(),
                        request.getFrontendSessionId());
            }

            if (!request.getFrontendSessionId().isEmpty()) {
                options.putIfAbsent("frontendSessionId", request.getFrontendSessionId());
            }

            String frontendSessionId;
            if (request.getSession() != null && !request.getSession().getSessionId().isEmpty()) {
                frontendSessionId = request.getSession().getSessionId();
            } else {
                frontendSessionId = UUID.randomUUID().toString();
            }

            JobType jobType = toJobType(request.getType());

            String sessionId = sessionStore.createSession(frontendSessionId, sql, jobType, options);
            String jobId = jobScheduler.submitJob(sessionId, sql, jobType, options);

            if (requestId != null && !requestId.isEmpty()) {
                requestIdCache.put(requestId, jobId);
            }

            QueryResponse response =
                    QueryResponse.newBuilder()
                            .setRequestId(request.getId())
                            .setJobId(jobId)
                            .setState(JobState.SUBMITTED)
                            .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (AuthorizationException e) {
            log.warn("Authorization failed for query: {}", e.getMessage());
            responseObserver.onNext(
                    buildErrorResponse(
                            request.getId(), Status.Code.PERMISSION_DENIED, e.getMessage()));
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error submitting query", e);
            responseObserver.onNext(
                    buildErrorResponse(
                            request.getId(),
                            Status.Code.INTERNAL,
                            "Failed to submit query: " + e.getMessage()));
            responseObserver.onCompleted();
        }
    }

    @Override
    public void submitIR(
            FrontendIrProto.SubmitIRRequest request,
            StreamObserver<FrontendIrProto.SubmitIRResponse> responseObserver) {
        BackpressureManager.BackpressureStatus bpStatus =
                backpressureManager != null
                        ? backpressureManager.getStatus()
                        : BackpressureManager.BackpressureStatus.NORMAL;

        if (bpStatus == BackpressureManager.BackpressureStatus.REJECTING) {
            log.warn("Rejecting IR submission due to backpressure: {}", bpStatus);
            responseObserver.onNext(
                    FrontendIrProto.SubmitIRResponse.newBuilder()
                            .setAccepted(false)
                            .setError("Server is under high load, please retry later")
                            .build());
            responseObserver.onCompleted();
            return;
        }

        if (bpStatus == BackpressureManager.BackpressureStatus.WARNING) {
            log.warn("Processing IR submission under backpressure warning: {}", bpStatus);
        }

        String irTypeName = request.getIr().getType().name();
        boolean isWriteIR =
                irTypeName.equals("INSERT")
                        || irTypeName.equals("BATCH_INSERT")
                        || irTypeName.equals("UPDATE")
                        || irTypeName.equals("DELETE");
        String irDedupeKey = null;

        if (!isWriteIR) {
            irDedupeKey =
                    "ir:"
                            + irTypeName
                            + ":"
                            + request.getIr().getTableIdentifier()
                            + ":"
                            + request.getTimeoutMs()
                            + ":"
                            + Arrays.hashCode(request.getIr().toByteArray());
            String cachedJobId = requestIdCache.getIfPresent(irDedupeKey);
            if (cachedJobId != null) {
                responseObserver.onNext(
                        FrontendIrProto.SubmitIRResponse.newBuilder()
                                .setJobId(cachedJobId)
                                .setAccepted(true)
                                .build());
                responseObserver.onCompleted();
                return;
            }
        }

        try {
            Principal principal = AuthServerInterceptor.PRINCIPAL_KEY.get(Context.current());
            if (principal == null) {
                principal =
                        new Principal(
                                "anonymous",
                                "default",
                                io.github.rawwise.backend.server.auth.Role.ADMIN,
                                java.util.Set.of(
                                        io.github.rawwise.backend.server.auth.Permission.DDL,
                                        io.github.rawwise.backend.server.auth.Permission.WRITE_PATH,
                                        io.github.rawwise.backend.server.auth.Permission
                                                .READ_CROSS_TENANT));
                log.debug(
                        "No authenticated principal found, using anonymous principal for IR submission");
            }

            QueryIR ir = IRProtobufSerializer.fromProto(request.getIr());
            long timeoutMs = request.getTimeoutMs() > 0 ? request.getTimeoutMs() : 3_600_000L;
            String tenantId = principal.tenantId();
            String jobId = jobScheduler.submitIR(ir, timeoutMs, tenantId);

            if (!isWriteIR) {
                requestIdCache.put(irDedupeKey, jobId);
            }

            responseObserver.onNext(
                    FrontendIrProto.SubmitIRResponse.newBuilder()
                            .setJobId(jobId)
                            .setAccepted(true)
                            .build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Failed to submit IR", e);
            responseObserver.onNext(
                    FrontendIrProto.SubmitIRResponse.newBuilder()
                            .setAccepted(false)
                            .setError("Failed to submit IR: " + e.getMessage())
                            .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getQueryStatus(
            QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        try {
            String jobId = request.getId();

            JobStatus jobStatus = jobScheduler.getJobStatus(jobId);
            if (jobStatus == null) {
                responseObserver.onError(
                        io.grpc.Status.NOT_FOUND
                                .withDescription("Job not found: " + jobId)
                                .asRuntimeException());
                return;
            }
            JobState jobState = toJobState(jobStatus);

            QueryResponse response =
                    QueryResponse.newBuilder()
                            .setRequestId(request.getId())
                            .setJobId(jobId)
                            .setState(jobState)
                            .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Error getting query status", e);
            responseObserver.onNext(
                    buildErrorResponse(
                            request.getId(),
                            Status.Code.INTERNAL,
                            "Failed to get query status: " + e.getMessage()));
            responseObserver.onCompleted();
        }
    }

    @Override
    public void cancelQuery(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        try {
            String jobId = request.getId();

            boolean cancelled = jobScheduler.cancelJob(jobId);

            if (!cancelled) {
                ManagedJob job =
                        jobScheduler.getActiveJobs().stream()
                                .filter(j -> j.getId().equals(jobId))
                                .findFirst()
                                .orElse(null);
                if (job != null) {
                    responseObserver.onNext(
                            QueryResponse.newBuilder()
                                    .setRequestId(request.getId())
                                    .setJobId(jobId)
                                    .setState(toJobState(job.getStatus()))
                                    .build());
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(
                            io.grpc.Status.NOT_FOUND
                                    .withDescription("Job not found")
                                    .asRuntimeException());
                }
                return;
            }

            QueryResponse response =
                    QueryResponse.newBuilder()
                            .setRequestId(request.getId())
                            .setJobId(jobId)
                            .setState(JobState.CANCELLED)
                            .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Error cancelling query", e);
            responseObserver.onNext(
                    buildErrorResponse(
                            request.getId(),
                            Status.Code.INTERNAL,
                            "Failed to cancel query: " + e.getMessage()));
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getQueryResult(QueryRequest request, StreamObserver<ResultChunk> responseObserver) {
        try {
            String jobId = request.getId();
            Principal principal = AuthServerInterceptor.PRINCIPAL_KEY.get(Context.current());

            JobResultHandle handle = jobScheduler.getJobResult(jobId);

            if (handle == null) {
                JobStatus status = jobScheduler.getJobStatus(jobId);
                if (status == null) {
                    responseObserver.onError(
                            io.grpc.Status.NOT_FOUND
                                    .withDescription("Job not found: " + jobId)
                                    .asRuntimeException());
                } else if (status == JobStatus.SUCCEEDED) {
                    responseObserver.onError(
                            io.grpc.Status.NOT_FOUND
                                    .withDescription("Job result expired")
                                    .asRuntimeException());
                } else {
                    responseObserver.onError(
                            io.grpc.Status.UNAVAILABLE
                                    .withDescription("Job " + status.name() + ", retry later")
                                    .asRuntimeException());
                }
                return;
            }

            if (principal != null) {
                String jobTenantId = jobScheduler.getJobTenantId(jobId);
                if (jobTenantId != null && !principal.tenantId().equals(jobTenantId)) {
                    responseObserver.onError(
                            io.grpc.Status.PERMISSION_DENIED
                                    .withDescription("Access denied")
                                    .asRuntimeException());
                    return;
                }
            }

            switch (handle) {
                case InlineResult inline -> sendInChunks(inline, responseObserver);
                case ExternalResult external -> sendExternalMetadata(external, responseObserver);
                case LeasedResult leased -> sendLeaseInfo(leased, responseObserver);
            }

        } catch (Exception e) {
            log.error("Error getting query result", e);
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("Failed to get query result: " + e.getMessage())
                            .asRuntimeException());
        }
    }

    private void sendInChunks(InlineResult inline, StreamObserver<ResultChunk> responseObserver) {
        byte[] data = inline.data();
        int totalChunks = (int) Math.ceil((double) data.length / CHUNK_SIZE_BYTES);
        if (totalChunks == 0) totalChunks = 1;

        ResultChunk.Metadata.Builder metadataBuilder =
                ResultChunk.Metadata.newBuilder()
                        .setTotalRows(inline.totalRows())
                        .setTotalBytes(inline.estimatedBytes());
        if (inline.columns() != null && !inline.columns().isEmpty()) {
            metadataBuilder.addAllColumns(inline.columns());
        }
        ResultChunk.Metadata metadata = metadataBuilder.build();

        for (int i = 0; i < totalChunks; i++) {
            int start = i * CHUNK_SIZE_BYTES;
            int end = Math.min(start + CHUNK_SIZE_BYTES, data.length);
            byte[] chunkData = new byte[end - start];
            System.arraycopy(data, start, chunkData, 0, end - start);

            ResultChunk chunk =
                    ResultChunk.newBuilder()
                            .setChunkId(i + 1)
                            .setIsFirst(i == 0)
                            .setIsLast(i == totalChunks - 1)
                            .setTotalChunks(totalChunks)
                            .setFormat(ResultChunk.DataFormat.JSON)
                            .setMetadata(metadata)
                            .setData(ByteString.copyFrom(chunkData))
                            .build();

            responseObserver.onNext(chunk);
        }

        responseObserver.onCompleted();
    }

    private void sendExternalMetadata(
            ExternalResult external, StreamObserver<ResultChunk> responseObserver) {
        ResultChunk.Metadata.Builder metadataBuilder =
                ResultChunk.Metadata.newBuilder().setTotalBytes(external.estimatedBytes());
        if (external.hasKnownRowCount()) {
            metadataBuilder.setTotalRows(external.totalRows());
        }
        ResultChunk.Metadata metadata = metadataBuilder.build();

        ResultChunk chunk =
                ResultChunk.newBuilder()
                        .setChunkId(1)
                        .setIsFirst(true)
                        .setIsLast(true)
                        .setTotalChunks(1)
                        .setFormat(ResultChunk.DataFormat.PARQUET)
                        .setMetadata(metadata)
                        .setData(ByteString.copyFromUtf8(external.storageUri()))
                        .build();

        responseObserver.onNext(chunk);
        responseObserver.onCompleted();
    }

    private void sendLeaseInfo(LeasedResult leased, StreamObserver<ResultChunk> responseObserver) {
        ResultChunk.Metadata metadata =
                ResultChunk.Metadata.newBuilder().setTotalBytes(leased.estimatedBytes()).build();

        ResultChunk chunk =
                ResultChunk.newBuilder()
                        .setChunkId(1)
                        .setIsFirst(true)
                        .setIsLast(true)
                        .setTotalChunks(1)
                        .setFormat(ResultChunk.DataFormat.RAW_BYTES)
                        .setMetadata(metadata)
                        .setData(ByteString.copyFromUtf8(leased.splitPlanJson()))
                        .build();

        responseObserver.onNext(chunk);
        responseObserver.onCompleted();
    }

    @Override
    public void executeQuery(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        submitQuery(request, responseObserver);
    }

    @Override
    public void explainQuery(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("QueryService.explainQuery not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void analyzeTable(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("QueryService.analyzeTable not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void validateQuery(
            QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("QueryService.validateQuery not yet implemented")
                        .asRuntimeException());
    }

    private JobType toJobType(QueryType type) {
        if (type == null) {
            return JobType.READ;
        }
        switch (type) {
            case DDL:
                return JobType.DDL;
            case DML:
                return JobType.WRITE;
            case SQL:
            case ANALYZE:
            case EXPLAIN:
            case SHOW:
            case DESCRIBE:
            default:
                return JobType.READ;
        }
    }

    private JobState toJobState(JobStatus status) {
        if (status == null) {
            return JobState.PENDING;
        }
        switch (status) {
            case PENDING:
                return JobState.PENDING;
            case RUNNING:
                return JobState.RUNNING;
            case SUCCEEDED:
                return JobState.SUCCEEDED;
            case FAILED:
                return JobState.FAILED;
            case CANCELLING:
                return JobState.CANCELLING;
            case CANCELLED:
                return JobState.CANCELLED;
            case TIMED_OUT:
                return JobState.TIMED_OUT;
            default:
                return JobState.PENDING;
        }
    }

    private QueryResponse buildErrorResponse(String requestId, Status.Code code, String message) {
        return QueryResponse.newBuilder()
                .setRequestId(requestId != null ? requestId : "")
                .setError(Status.newBuilder().setCode(code).setMessage(message).build())
                .build();
    }
}
