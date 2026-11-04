package io.github.rawwise.frontend.backend;

import com.google.protobuf.Empty;
import io.github.rawwise.backend.server.txn.TxnHandle;
import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.state.TxnResult;
import io.github.rawwise.frontend.config.FrontendOptions;
import io.github.rawwise.frontend.ir.QueryIR;
import io.github.rawwise.frontend.ir.proto.FrontendIrProto;
import io.github.rawwise.frontend.ir.serializer.IRProtobufSerializer;
import io.github.rawwise.governance.auth.InternalTokenResolver;
import io.github.rawwise.governance.telemetry.TelemetryConfig;
import io.github.rawwise.server.*;
import io.github.rawwise.txn.*;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GrpcBackendClient implements BackendClient, AutoCloseable {

    private static final long CHANNEL_SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final ManagedChannel channel;
    private final QueryServiceGrpc.QueryServiceBlockingStub queryStub;
    private final ClusterManagementServiceGrpc.ClusterManagementServiceBlockingStub clusterStub;
    private final TxnCoordinatorServiceGrpc.TxnCoordinatorServiceBlockingStub txnStub;
    private final long rpcTimeoutSeconds;
    private final int maxResultRows;
    private final int maxResultBytes;
    private final int maxTrainingResultBytes;
    private final long maxTotalResultBytes;
    private final AtomicLong totalResultBytes = new AtomicLong(0);
    private final String internalToken;

    public GrpcBackendClient(String host, int port, long rpcTimeoutSeconds) {
        this(host, port, rpcTimeoutSeconds, null);
    }

    public GrpcBackendClient(ManagedChannel channel, long rpcTimeoutSeconds) {
        this.rpcTimeoutSeconds = rpcTimeoutSeconds;
        this.channel = channel;
        this.internalToken = InternalTokenResolver.resolve();
        this.queryStub = QueryServiceGrpc.newBlockingStub(channel);
        this.clusterStub = ClusterManagementServiceGrpc.newBlockingStub(channel);
        this.txnStub = TxnCoordinatorServiceGrpc.newBlockingStub(channel);
        this.maxResultRows = 10000;
        this.maxResultBytes = 16 * 1024 * 1024;
        this.maxTrainingResultBytes = 256 * 1024 * 1024;
        this.maxTotalResultBytes = 256L * 1024 * 1024;
    }

    public static GrpcBackendClient forInProcess(String serverName, long rpcTimeoutSeconds) {
        return new GrpcBackendClient(
                io.grpc.inprocess.InProcessChannelBuilder.forName(serverName)
                        .directExecutor()
                        .build(),
                rpcTimeoutSeconds);
    }

    public GrpcBackendClient(
            String host, int port, long rpcTimeoutSeconds, FrontendOptions options) {
        this.rpcTimeoutSeconds = rpcTimeoutSeconds;
        this.internalToken = InternalTokenResolver.resolve();

        boolean tlsEnabled = options != null && options.isGrpcTlsEnabled();
        if (options != null
                && options.getDeploymentMode() == FrontendOptions.DeploymentMode.STANDALONE
                && !tlsEnabled) {
            boolean explicitlyDisabled =
                    Boolean.parseBoolean(
                            System.getProperty(
                                    "rawwise.grpc.tls.explicitly-unsafe-disable", "false"));
            if (!explicitlyDisabled) {
                log.warn(
                        "STANDALONE mode requires grpc.tls.enabled=true; overriding to true for safety");
                tlsEnabled = true;
            } else {
                log.warn("╔══════════════════════════════════════════════════════╗");
                log.warn("║  gRPC TLS EXPLICITLY DISABLED IN STANDALONE         ║");
                log.warn("║  THIS IS UNSAFE - DO NOT USE IN PRODUCTION          ║");
                log.warn("╚══════════════════════════════════════════════════════╝");
            }
        }

        NettyChannelBuilder builder = NettyChannelBuilder.forAddress(host, port);
        if (tlsEnabled) {
            builder.useTransportSecurity();
            if (options != null && options.isGrpcMtlsEnabled()) {
                builder.sslContext(buildMutualTlsSslContext(options));
            }
        } else {
            builder.usePlaintext();
        }

        builder.maxInboundMessageSize(64 * 1024 * 1024)
                .maxInboundMetadataSize(1 * 1024 * 1024)
                .flowControlWindow(8 * 1024 * 1024)
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true)
                .idleTimeout(300, TimeUnit.SECONDS);

        this.channel = builder.build();

        // Attach authorization token to all gRPC calls
        Channel authChannel =
                io.grpc.ClientInterceptors.intercept(
                        channel,
                        new ClientInterceptor() {
                            private static final Metadata.Key<String> TOKEN_KEY =
                                    Metadata.Key.of(
                                            "authorization", Metadata.ASCII_STRING_MARSHALLER);
                            private static final Metadata.Key<String> TRACE_ID_KEY =
                                    Metadata.Key.of("x-trace-id", Metadata.ASCII_STRING_MARSHALLER);

                            @Override
                            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                                    MethodDescriptor<ReqT, RespT> method,
                                    CallOptions callOptions,
                                    Channel next) {
                                ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);
                                return new ForwardingClientCall.SimpleForwardingClientCall<>(call) {
                                    @Override
                                    public void start(
                                            Listener<RespT> responseListener, Metadata headers) {
                                        headers.put(TOKEN_KEY, internalToken);
                                        String traceId = TelemetryConfig.currentTraceId();
                                        if (!traceId.isBlank()) {
                                            headers.put(TRACE_ID_KEY, traceId);
                                        }
                                        super.start(responseListener, headers);
                                    }
                                };
                            }
                        });

        this.queryStub = QueryServiceGrpc.newBlockingStub(authChannel);
        this.clusterStub = ClusterManagementServiceGrpc.newBlockingStub(authChannel);
        this.txnStub = TxnCoordinatorServiceGrpc.newBlockingStub(authChannel);

        if (options != null) {
            this.maxResultRows = options.getMaxResultRows();
            this.maxResultBytes = options.getMaxResultBytes();
            this.maxTrainingResultBytes = options.getMaxTrainingResultBytes();
            this.maxTotalResultBytes = options.getMaxTotalResultBytes();
        } else {
            this.maxResultRows = 10000;
            this.maxResultBytes = 16 * 1024 * 1024;
            this.maxTrainingResultBytes = 256 * 1024 * 1024;
            this.maxTotalResultBytes = 256L * 1024 * 1024;
        }
    }

    private SslContext buildMutualTlsSslContext(FrontendOptions options) {
        if (options.getGrpcTlsTruststorePath() == null) {
            throw new IllegalArgumentException(
                    "mTLS requires truststore configuration (grpc.tls.truststore.path)");
        }
        try {
            TrustManagerFactory tmf =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            KeyStore trustStore = KeyStore.getInstance(options.getGrpcTlsTruststoreType());
            try (InputStream is =
                    Files.newInputStream(Path.of(options.getGrpcTlsTruststorePath()))) {
                String trustPassword = resolveSecret(options.getGrpcTlsTruststorePassword());
                trustStore.load(is, trustPassword != null ? trustPassword.toCharArray() : null);
            }
            tmf.init(trustStore);

            SslContextBuilder sslBuilder =
                    GrpcSslContexts.configure(SslContextBuilder.forClient().trustManager(tmf));

            if (options.getGrpcTlsKeystorePath() != null) {
                KeyManagerFactory kmf =
                        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                KeyStore keyStore = KeyStore.getInstance(options.getGrpcTlsKeystoreType());
                try (InputStream is =
                        Files.newInputStream(Path.of(options.getGrpcTlsKeystorePath()))) {
                    String keyPassword = resolveSecret(options.getGrpcTlsKeystorePassword());
                    keyStore.load(is, keyPassword != null ? keyPassword.toCharArray() : null);
                    kmf.init(keyStore, keyPassword != null ? keyPassword.toCharArray() : null);
                }
                sslBuilder.keyManager(kmf);
            }

            sslBuilder.protocols(List.of("TLSv1.2", "TLSv1.3"));
            sslBuilder.ciphers(List.of(options.getGrpcTlsCipherSuites().split("\\s*,\\s*")));

            return sslBuilder.build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to build mTLS SSL context", e);
        }
    }

    private String resolveSecret(String value) {
        if (value != null && value.startsWith("${env:")) {
            if (!value.endsWith("}")) {
                throw new IllegalArgumentException("Invalid secret reference format: " + value);
            }
            String envVar = value.substring(6, value.length() - 1);
            if (!envVar.startsWith("RAWWISE_SECRET_")) {
                throw new IllegalArgumentException(
                        "Secret environment variable must start with 'RAWWISE_SECRET_', got: "
                                + envVar);
            }
            String resolved = System.getenv(envVar);
            if (resolved == null) {
                throw new IllegalArgumentException(
                        "Secret environment variable not found: " + envVar);
            }
            return resolved;
        }
        return value;
    }

    private int resolveMaxResultBytes(String workloadType) {
        return switch (workloadType != null ? workloadType : "interactive") {
            case "training_scan" -> maxTrainingResultBytes;
            case "online_lookup" -> 4 * 1024 * 1024;
            default -> maxResultBytes;
        };
    }

    private QueryServiceGrpc.QueryServiceBlockingStub queryStubWithDeadline() {
        return queryStub.withDeadlineAfter(rpcTimeoutSeconds, TimeUnit.SECONDS);
    }

    private ClusterManagementServiceGrpc.ClusterManagementServiceBlockingStub
            clusterStubWithDeadline() {
        return clusterStub.withDeadlineAfter(rpcTimeoutSeconds, TimeUnit.SECONDS);
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
        try {
            QueryRequest.Builder requestBuilder =
                    QueryRequest.newBuilder()
                            .setId(UUID.randomUUID().toString())
                            .setSql(sql)
                            .setType(type)
                            .putAllParameters(options);

            if (txnId > 0) {
                requestBuilder.setTxnId(txnId);
            }
            if (frontendSessionId != null && !frontendSessionId.isBlank()) {
                requestBuilder.setFrontendSessionId(frontendSessionId);
            }

            QueryResponse response = queryStubWithDeadline().submitQuery(requestBuilder.build());

            if (response.hasError()) {
                return new SubmitResult(null, false, response.getError().getMessage());
            }

            return new SubmitResult(response.getJobId(), true, null);
        } catch (Exception e) {
            log.error("Failed to submit query", e);
            return new SubmitResult(null, false, "Query submission failed");
        }
    }

    @Override
    public SubmitIRResult submitIR(QueryIR ir, long timeoutMs) {
        try {
            FrontendIrProto.SubmitIRRequest request =
                    FrontendIrProto.SubmitIRRequest.newBuilder()
                            .setIr(IRProtobufSerializer.toProto(ir))
                            .setTimeoutMs(timeoutMs)
                            .build();

            FrontendIrProto.SubmitIRResponse response = queryStubWithDeadline().submitIR(request);

            return new SubmitIRResult(
                    response.getJobId(), response.getAccepted(), response.getError());
        } catch (Exception e) {
            log.error("submitIR failed", e);
            return new SubmitIRResult(null, false, "IR submission failed");
        }
    }

    @Override
    public JobState getJobStatus(String jobId) {
        try {
            QueryRequest request = QueryRequest.newBuilder().setId(jobId).build();

            QueryResponse response = queryStubWithDeadline().getQueryStatus(request);

            if (response.hasError()) {
                log.warn(
                        "Job status query for {} returned error: {}",
                        jobId,
                        response.getError().getMessage());
            }

            return response.getState();
        } catch (Exception e) {
            log.error("Failed to get job status for {}", jobId, e);
            throw new BackendUnavailableException("Failed to get job status for " + jobId, e);
        }
    }

    @Override
    public boolean cancelJob(String jobId) {
        try {
            QueryRequest request = QueryRequest.newBuilder().setId(jobId).build();

            QueryResponse response = queryStubWithDeadline().cancelQuery(request);
            return response.getState() == JobState.CANCELLED;
        } catch (Exception e) {
            log.error("Failed to cancel job {}", jobId, e);
            return false;
        }
    }

    @Override
    public ClusterStatus getClusterStatus() {
        try {
            return clusterStubWithDeadline().getClusterStatus(Empty.getDefaultInstance());
        } catch (Exception e) {
            log.error("Failed to get cluster status", e);
            throw new BackendUnavailableException("Backend cluster status unavailable", e);
        }
    }

    @Override
    public BackendClient.QueryResult getQueryResult(String jobId) {
        return getQueryResult(jobId, 0, maxResultRows, maxResultBytes);
    }

    @Override
    public BackendClient.QueryResult getQueryResult(
            String jobId, int offset, int limit, int maxBytes) {
        try {
            QueryRequest request = QueryRequest.newBuilder().setId(jobId).build();

            java.util.Iterator<ResultChunk> chunks =
                    queryStubWithDeadline().getQueryResult(request);

            if (!chunks.hasNext()) {
                return null;
            }

            ParsedStreamResult parsed = parseResultStream(chunks);

            List<List<Object>> rows = parsed.rows();
            boolean truncated = false;

            if (offset > 0) {
                if (offset >= rows.size()) {
                    rows = List.of();
                } else {
                    rows = new ArrayList<>(rows.subList(offset, rows.size()));
                }
            }

            if (rows.size() > limit) {
                rows = new ArrayList<>(rows.subList(0, limit));
                truncated = true;
            }

            if (parsed.totalBytes() > maxBytes) {
                truncated = true;
            }

            String nextPageToken = truncated ? String.valueOf(offset + rows.size()) : null;
            return new BackendClient.QueryResult(
                    parsed.columns(), rows, rows.size(), truncated, nextPageToken);
        } catch (ResultBudgetExceededException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to get query result for {}", jobId, e);
            return null;
        }
    }

    private record ParsedStreamResult(
            List<BackendClient.ColumnInfo> columns,
            List<List<Object>> rows,
            int totalRows,
            long totalBytes) {}

    private ParsedStreamResult parseResultStream(java.util.Iterator<ResultChunk> chunks) {
        List<BackendClient.ColumnInfo> columns = List.of();
        StringBuilder allData = new StringBuilder();
        int totalRows = 0;
        long totalBytes = 0;
        boolean isParquet = false;

        while (chunks.hasNext()) {
            ResultChunk chunk = chunks.next();

            if (chunk.hasMetadata()) {
                columns =
                        chunk.getMetadata().getColumnsList().stream()
                                .map(
                                        col ->
                                                new BackendClient.ColumnInfo(
                                                        col.getName(), col.getType()))
                                .toList();
                totalRows = (int) chunk.getMetadata().getTotalRows();
            }

            if (chunk.getFormat() == ResultChunk.DataFormat.PARQUET) {
                isParquet = true;
            }

            if (chunk.getData() != null && !chunk.getData().isEmpty()) {
                totalBytes += chunk.getData().size();
                allData.append(chunk.getData().toStringUtf8());
            }
        }

        long currentTotal = totalResultBytes.get();
        while (true) {
            long newTotal = currentTotal + totalBytes;
            if (newTotal > maxTotalResultBytes) {
                throw new ResultBudgetExceededException("Global result memory budget exceeded");
            }
            if (totalResultBytes.compareAndSet(currentTotal, newTotal)) {
                break;
            }
            currentTotal = totalResultBytes.get();
        }

        List<List<Object>> rows = List.of();
        if (!allData.isEmpty()) {
            if (isParquet) {
                log.warn(
                        "External Parquet result received ({} bytes) but Frontend cannot read Parquet files; returning empty rows",
                        totalBytes);
            } else {
                rows = parseJsonRows(allData.toString(), columns);
            }
        }

        int rowCount = totalRows > 0 ? totalRows : rows.size();
        return new ParsedStreamResult(columns, rows, rowCount, totalBytes);
    }

    @SuppressWarnings("unchecked")
    private List<List<Object>> parseJsonRows(
            String jsonData, List<BackendClient.ColumnInfo> columns) {
        try {
            List<Object> rawList = JSON_MAPPER.readValue(jsonData, List.class);
            List<List<Object>> rows = new ArrayList<>();
            for (Object item : rawList) {
                if (item instanceof java.util.Map) {
                    java.util.Map<String, Object> map = (java.util.Map<String, Object>) item;
                    List<Object> row = new ArrayList<>();
                    for (BackendClient.ColumnInfo col : columns) {
                        row.add(map.get(col.name()));
                    }
                    rows.add(row);
                }
            }
            return rows;
        } catch (Exception e) {
            log.warn("Failed to parse JSON rows", e);
            return List.of();
        }
    }

    private static final com.fasterxml.jackson.databind.ObjectMapper JSON_MAPPER =
            new com.fasterxml.jackson.databind.ObjectMapper();

    @Override
    public TxnHandle beginTxn(Set<String> tableIds, IsolationLevel level) {
        try {
            BeginTxnRequest request =
                    BeginTxnRequest.newBuilder()
                            .addAllTableIds(tableIds)
                            .setIsolationLevel(level.name())
                            .build();

            BeginTxnResponse response =
                    txnStub.withDeadlineAfter(rpcTimeoutSeconds, TimeUnit.SECONDS)
                            .beginTxn(request);

            long txnId = response.getTxnId();
            if (txnId <= 0) {
                throw new RuntimeException("Backend returned invalid txnId: " + txnId);
            }

            Map<String, String> nodeAssignments = new HashMap<>(response.getNodeAssignmentsMap());
            log.info(
                    "beginTxn: txnId={} tableIds={} level={} nodeAssignments={}",
                    txnId,
                    tableIds,
                    level,
                    nodeAssignments);

            return new TxnHandle(
                    txnId, level, tableIds, nodeAssignments, System.currentTimeMillis());
        } catch (Exception e) {
            log.error("beginTxn failed", e);
            throw new RuntimeException("Failed to begin transaction: " + e.getMessage(), e);
        }
    }

    @Override
    public TxnResult commitTxn(long txnId) {
        try {
            CommitTxnRequest request = CommitTxnRequest.newBuilder().setTxnId(txnId).build();

            CommitTxnResponse response =
                    txnStub.withDeadlineAfter(rpcTimeoutSeconds, TimeUnit.SECONDS)
                            .commitTxn(request);

            if (response.getSuccess()) {
                log.info("commitTxn: txnId={} committed at ts={}", txnId, response.getCommitTs());
                return new TxnResult.CommitOk(response.getCommitTs());
            } else {
                log.warn("commitTxn: txnId={} conflict: {}", txnId, response.getConflictReason());
                return new TxnResult.CommitConflict(response.getConflictReason());
            }
        } catch (Exception e) {
            log.error("commitTxn failed for txnId={}", txnId, e);
            return new TxnResult.CommitConflict("gRPC error: " + e.getMessage());
        }
    }

    @Override
    public TxnResult abortTxn(long txnId) {
        try {
            AbortTxnRequest request = AbortTxnRequest.newBuilder().setTxnId(txnId).build();

            AbortTxnResponse response =
                    txnStub.withDeadlineAfter(rpcTimeoutSeconds, TimeUnit.SECONDS)
                            .abortTxn(request);

            log.info(
                    "abortTxn: txnId={} success={} reason={}",
                    txnId,
                    response.getSuccess(),
                    response.getReason());
            return new TxnResult.AbortResult(txnId, response.getReason());
        } catch (Exception e) {
            log.error("abortTxn failed for txnId={}", txnId, e);
            return new TxnResult.AbortResult(txnId, "gRPC error: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        if (channel == null || channel.isShutdown()) {
            return;
        }
        channel.shutdown();
        try {
            if (!channel.awaitTermination(CHANNEL_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("gRPC channel did not terminate gracefully, forcing shutdown");
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("gRPC channel shutdown interrupted, forcing shutdown");
            channel.shutdownNow();
        }
    }
}
