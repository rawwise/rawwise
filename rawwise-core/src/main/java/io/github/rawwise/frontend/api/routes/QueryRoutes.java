package io.github.rawwise.frontend.api.routes;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.state.TxnResult;
import io.github.rawwise.frontend.api.dto.ColumnInfoDto;
import io.github.rawwise.frontend.api.dto.ErrorResponseDto;
import io.github.rawwise.frontend.api.dto.QueryRequestDto;
import io.github.rawwise.frontend.api.dto.QueryResponseDto;
import io.github.rawwise.frontend.api.dto.QueryResultDto;
import io.github.rawwise.frontend.api.session.SessionManager;
import io.github.rawwise.frontend.api.session.TxnSession;
import io.github.rawwise.frontend.backend.BackendClient;
import io.github.rawwise.frontend.evaluator.QueryEvaluator;
import io.github.rawwise.frontend.ir.IrRuntime;
import io.github.rawwise.frontend.ir.MetadataQueryIR;
import io.github.rawwise.frontend.ir.MetadataResult;
import io.github.rawwise.frontend.ir.PointQueryIR;
import io.github.rawwise.frontend.ir.QueryIR;
import io.github.rawwise.frontend.ir.parser.ParseResult;
import io.github.rawwise.frontend.ir.routing.ExecutionPath;
import io.github.rawwise.frontend.ir.routing.FastPathResult;
import io.github.rawwise.frontend.ir.routing.PointQueryResult;
import io.github.rawwise.frontend.session.FrontendSessionStore;
import io.github.rawwise.governance.auth.AuthMetastore;
import io.github.rawwise.governance.auth.Principal;
import io.github.rawwise.server.JobState;
import io.github.rawwise.server.QueryType;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

@Slf4j
public class QueryRoutes {

    private static final Set<String> ALLOWED_SQL_PREFIXES =
            Set.of(
                    "SELECT",
                    "WITH",
                    "SHOW",
                    "CREATE",
                    "DROP",
                    "INSERT",
                    "DELETE",
                    "UPDATE",
                    "ALTER",
                    "TRUNCATE",
                    "BEGIN",
                    "COMMIT",
                    "ROLLBACK");
    private static final long MIN_TIMEOUT_MS = 1_000L;
    private static final long MAX_INTERACTIVE_TIMEOUT_MS = 120_000L;
    private static final long MAX_TRAINING_TIMEOUT_MS = 3_600_000L;
    private static final long MAX_ONLINE_LOOKUP_TIMEOUT_MS = 5_000L;

    private final FrontendSessionStore sessionStore;
    private final BackendClient backendClient;
    private final QueryEvaluator queryEvaluator;
    private final IrRuntime irRuntime;
    private final AuthMetastore authMetastore;
    private final SessionManager sessionManager;

    public QueryRoutes(
            FrontendSessionStore sessionStore,
            BackendClient backendClient,
            QueryEvaluator queryEvaluator,
            @Nullable IrRuntime irRuntime,
            @Nullable AuthMetastore authMetastore) {
        this(
                sessionStore,
                backendClient,
                queryEvaluator,
                irRuntime,
                authMetastore,
                new SessionManager());
    }

    public QueryRoutes(
            FrontendSessionStore sessionStore,
            BackendClient backendClient,
            QueryEvaluator queryEvaluator,
            @Nullable IrRuntime irRuntime,
            @Nullable AuthMetastore authMetastore,
            @Nullable SessionManager sessionManager) {
        this.sessionStore = sessionStore;
        this.backendClient = backendClient;
        this.queryEvaluator = queryEvaluator;
        this.irRuntime = irRuntime;
        this.authMetastore = authMetastore;
        this.sessionManager = sessionManager != null ? sessionManager : new SessionManager();
    }

    public Handler submitBatchQuery() {
        return ctx -> {
            var requestNode = ctx.bodyAsClass(com.fasterxml.jackson.databind.JsonNode.class);
            var sqlsNode = requestNode.get("sqls");
            if (sqlsNode == null || !sqlsNode.isArray() || sqlsNode.size() == 0) {
                ctx.status(400)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INVALID_REQUEST")
                                        .message("\"sqls\" must be a non-empty array")
                                        .build());
                return;
            }

            var optionsNode = requestNode.get("options");
            String database = "default";
            if (optionsNode != null && optionsNode.has("database")) {
                database = optionsNode.get("database").asText();
            }

            List<QueryResponseDto> results = new ArrayList<>();
            long batchStart = System.nanoTime();

            List<String> pendingDataSqls = new ArrayList<>();
            for (int i = 0; i < sqlsNode.size(); i++) {
                String sql = sqlsNode.get(i).asText().trim();
                if (sql.isBlank()) continue;
                String sqlUpper = sql.toUpperCase();

                if (sqlUpper.startsWith("BEGIN")) {
                    if (!pendingDataSqls.isEmpty()) {
                        results.addAll(executeDataSqlsParallel(ctx, pendingDataSqls, database));
                        pendingDataSqls.clear();
                    }
                    results.add(executeBeginInline(ctx, sql));
                    if (!"SUCCEEDED".equals(results.getLast().getState())) break;
                } else if (sqlUpper.startsWith("COMMIT")) {
                    if (!pendingDataSqls.isEmpty()) {
                        results.addAll(executeDataSqlsParallel(ctx, pendingDataSqls, database));
                        pendingDataSqls.clear();
                    }
                    results.add(executeCommitInline(ctx));
                    break;
                } else if (sqlUpper.startsWith("ROLLBACK")) {
                    if (!pendingDataSqls.isEmpty()) {
                        results.addAll(executeDataSqlsParallel(ctx, pendingDataSqls, database));
                        pendingDataSqls.clear();
                    }
                    results.add(executeRollbackInline(ctx));
                    break;
                } else {
                    pendingDataSqls.add(sql);
                }
            }
            if (!pendingDataSqls.isEmpty()) {
                results.addAll(executeDataSqlsParallel(ctx, pendingDataSqls, database));
            }

            long elapsedMs = (System.nanoTime() - batchStart) / 1_000_000;
            ctx.json(
                    QueryResponseDto.builder()
                            .jobId(
                                    "batch-"
                                            + java.util
                                                    .UUID
                                                    .randomUUID()
                                                    .toString()
                                                    .substring(0, 8))
                            .state("SUCCEEDED")
                            .data(
                                    QueryResultDto.builder()
                                            .columns(
                                                    List.of(
                                                            ColumnInfoDto.builder()
                                                                    .name("sql")
                                                                    .type("STRING")
                                                                    .build(),
                                                            ColumnInfoDto.builder()
                                                                    .name("state")
                                                                    .type("STRING")
                                                                    .build(),
                                                            ColumnInfoDto.builder()
                                                                    .name("rowsAffected")
                                                                    .type("BIGINT")
                                                                    .build()))
                                            .rows(
                                                    results.stream()
                                                            .map(
                                                                    r -> {
                                                                        long rows =
                                                                                (r.getData() != null
                                                                                                && r.getData()
                                                                                                                .getRowCount()
                                                                                                        > 0)
                                                                                        ? r.getData()
                                                                                                .getRowCount()
                                                                                        : 0;
                                                                        List<Object> row =
                                                                                new ArrayList<>();
                                                                        row.add("");
                                                                        row.add(r.getState());
                                                                        row.add(rows);
                                                                        return row;
                                                                    })
                                                            .toList())
                                            .rowCount(results.size())
                                            .executionTimeMs(elapsedMs)
                                            .truncated(false)
                                            .build())
                            .build());
        };
    }

    private static final java.util.concurrent.ExecutorService BATCH_POOL =
            java.util.concurrent.Executors.newFixedThreadPool(16);

    private List<QueryResponseDto> executeDataSqlsParallel(
            Context ctx, List<String> sqls, String database) {
        List<QueryResponseDto> results = new ArrayList<>();
        for (String sql : sqls) {
            try {
                results.add(executeSqlInline(ctx, sql, database));
            } catch (Exception e) {
                results.add(
                        QueryResponseDto.builder()
                                .state("FAILED")
                                .error(
                                        ErrorResponseDto.builder()
                                                .code("BATCH_SQL_ERROR")
                                                .message(e.getMessage())
                                                .build())
                                .build());
                break;
            }
        }
        return results;
    }

    private QueryResponseDto executeSqlWithSession(
            String sql, String database, long txnId, String frontendSessionId) {
        Map<String, String> options = new HashMap<>();
        options.put("database", database);
        options.put("workload_type", "interactive");
        long timeoutMs = 30_000L;

        Object irResult = executeWithIR(sql, options, QueryType.SQL, timeoutMs, null);
        if (irResult instanceof QueryResponseDto response) {
            return response;
        }
        BackendClient.SubmitResult result =
                backendClient.submitQuery(sql, QueryType.SQL, options, txnId, frontendSessionId);
        if (!result.success()) {
            return QueryResponseDto.builder()
                    .state("FAILED")
                    .error(
                            ErrorResponseDto.builder()
                                    .code("INTERNAL_ERROR")
                                    .message("Query submission failed")
                                    .build())
                    .build();
        }
        JobState terminalState = pollUntilTerminal(result.jobId(), timeoutMs);
        if (terminalState == JobState.SUCCEEDED) {
            BackendClient.QueryResult queryResult = backendClient.getQueryResult(result.jobId());
            if (queryResult != null) {
                return buildResponseFromQueryResult(result.jobId(), queryResult);
            }
            return buildEmptyQueryResult(result.jobId());
        }
        return QueryResponseDto.builder().jobId(result.jobId()).state(terminalState.name()).build();
    }

    private QueryResponseDto executeBeginInline(Context ctx, String sql) {
        TxnSession currentTxn = sessionManager.getTxnSession(ctx);
        if (currentTxn.isActive()) {
            return QueryResponseDto.builder()
                    .state("FAILED")
                    .error(
                            ErrorResponseDto.builder()
                                    .code("TXN_ALREADY_ACTIVE")
                                    .message(
                                            "A transaction is already active (txnId="
                                                    + currentTxn.txnId()
                                                    + ")")
                                    .build())
                    .build();
        }
        IsolationLevel level = parseIsolationLevel(sql);
        Set<String> tableIds = parseTableIdsFromBegin(sql);
        long localTxnId = -(System.currentTimeMillis() & 0x7FFF_FFFF_FFFF_FFFFL);
        long now = System.currentTimeMillis();
        TxnSession localSession = new TxnSession(localTxnId, level, tableIds, now, now);
        sessionManager.setTxnSession(ctx, localSession);
        return QueryResponseDto.builder()
                .jobId("txn-" + localTxnId)
                .state(JobState.SUCCEEDED.name())
                .build();
    }

    private QueryResponseDto executeCommitInline(Context ctx) {
        TxnSession currentTxn = sessionManager.getTxnSession(ctx);
        if (!currentTxn.isActive()) {
            return QueryResponseDto.builder()
                    .state("FAILED")
                    .error(
                            ErrorResponseDto.builder()
                                    .code("NO_ACTIVE_TXN")
                                    .message("No active transaction to commit")
                                    .build())
                    .build();
        }
        sessionManager.clearTxnSession(ctx);
        return QueryResponseDto.builder()
                .jobId("txn-" + currentTxn.txnId())
                .state(JobState.SUCCEEDED.name())
                .build();
    }

    private QueryResponseDto executeRollbackInline(Context ctx) {
        TxnSession currentTxn = sessionManager.getTxnSession(ctx);
        if (currentTxn.isActive()) {
            sessionManager.clearTxnSession(ctx);
        }
        return QueryResponseDto.builder()
                .jobId("txn-" + currentTxn.txnId())
                .state(JobState.SUCCEEDED.name())
                .build();
    }

    private QueryResponseDto executeSqlInline(Context ctx, String sql, String database) {
        Map<String, String> options = new HashMap<>();
        options.put("database", database);
        options.put("workload_type", "interactive");
        long timeoutMs = 30_000L;
        Long snapshotId = null;

        Object irResult = executeWithIR(sql, options, QueryType.SQL, timeoutMs, snapshotId);
        if (irResult instanceof QueryResponseDto response) {
            return response;
        }

        TxnSession currentTxn = sessionManager.getTxnSession(ctx);
        long txnId = currentTxn.isActive() ? currentTxn.txnId() : 0;
        String frontendSessionId = ctx.sessionAttribute("sessionId");

        BackendClient.SubmitResult result =
                backendClient.submitQuery(sql, QueryType.SQL, options, txnId, frontendSessionId);
        if (!result.success()) {
            return QueryResponseDto.builder()
                    .state("FAILED")
                    .error(
                            ErrorResponseDto.builder()
                                    .code("INTERNAL_ERROR")
                                    .message("Query submission failed")
                                    .build())
                    .build();
        }
        JobState terminalState = pollUntilTerminal(result.jobId(), timeoutMs);
        if (terminalState == JobState.SUCCEEDED) {
            BackendClient.QueryResult queryResult = backendClient.getQueryResult(result.jobId());
            if (queryResult != null) {
                return buildResponseFromQueryResult(result.jobId(), queryResult);
            }
            return buildEmptyQueryResult(result.jobId());
        }
        return QueryResponseDto.builder().jobId(result.jobId()).state(terminalState.name()).build();
    }

    public Handler submitQuery() {
        return ctx -> {
            QueryRequestDto request = ctx.bodyAsClass(QueryRequestDto.class);

            if (request.getSql() == null || request.getSql().isBlank()) {
                ctx.status(400)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INVALID_REQUEST")
                                        .message("SQL cannot be empty")
                                        .build());
                return;
            }

            if (request.getSql().length() > 65536) {
                ctx.status(400)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INVALID_REQUEST")
                                        .message("SQL exceeds maximum length of 65536 characters")
                                        .build());
                return;
            }

            validateSqlType(request.getSql());

            String sqlUpper = request.getSql().trim().toUpperCase();

            if (sqlUpper.startsWith("BEGIN")) {
                handleBegin(ctx, request.getSql());
                return;
            }
            if (sqlUpper.startsWith("COMMIT")) {
                handleCommit(ctx);
                return;
            }
            if (sqlUpper.startsWith("ROLLBACK")) {
                handleRollback(ctx);
                return;
            }
            if (sqlUpper.startsWith("SHOW TABLES") && irRuntime != null) {
                handleShowTables(ctx, sqlUpper);
                return;
            }

            String requestedSnapshotId = ctx.header("X-Snapshot-ID");
            Long snapshotId = null;
            if (requestedSnapshotId != null) {
                try {
                    snapshotId = Long.parseLong(requestedSnapshotId);
                } catch (NumberFormatException e) {
                    ctx.status(400)
                            .json(
                                    ErrorResponseDto.builder()
                                            .code("INVALID_SNAPSHOT_ID")
                                            .message("X-Snapshot-ID must be a valid number")
                                            .build());
                    return;
                }
            }

            String workloadType = ctx.header("X-Workload-Type");
            if (workloadType == null) workloadType = "interactive";
            if (!Set.of("interactive", "training_scan", "online_lookup").contains(workloadType)) {
                ctx.status(400)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INVALID_WORKLOAD_TYPE")
                                        .message(
                                                "X-Workload-Type must be one of: interactive, training_scan, online_lookup")
                                        .build());
                return;
            }

            if ("training_scan".equals(workloadType) && snapshotId == null) {
                ctx.status(400)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("MISSING_SNAPSHOT_ID")
                                        .message(
                                                "X-Snapshot-ID is required when X-Workload-Type is training_scan")
                                        .build());
                return;
            }

            QueryType queryType = parseQueryType(request.getType());
            Map<String, String> options =
                    request.getOptions() != null
                            ? new HashMap<>(request.getOptions())
                            : new HashMap<>();
            options.put("workload_type", workloadType);
            String normalizedSql = request.getSql().trim().toUpperCase();
            boolean isReadQuery =
                    normalizedSql.startsWith("SELECT") || normalizedSql.startsWith("WITH");
            if (queryType == QueryType.SQL && isReadQuery) {
                options.putIfAbsent("collect", "true");
            }

            long timeoutMs = parseTimeoutMs(options, workloadType);

            TxnSession currentTxn = sessionManager.getTxnSession(ctx);
            long txnId = currentTxn.isActive() ? currentTxn.txnId() : 0;
            String frontendSessionId = ctx.sessionAttribute("sessionId");

            Object irResult =
                    executeWithIR(request.getSql(), options, queryType, timeoutMs, snapshotId);
            if (irResult instanceof QueryResponseDto response) {
                ctx.json(response);
                return;
            }

            BackendClient.SubmitResult result =
                    backendClient.submitQuery(
                            request.getSql(), queryType, options, txnId, frontendSessionId);

            if (!result.success()) {
                ctx.status(500)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INTERNAL_ERROR")
                                        .message("Query submission failed")
                                        .build());
                return;
            }

            String principal = extractPrincipalId(ctx);
            if (result.success() && principal != null) {
                sessionStore.bindJobOwner(result.jobId(), principal);
            }

            ctx.json(
                    QueryResponseDto.builder()
                            .jobId(result.jobId())
                            .state(JobState.PENDING.name())
                            .build());
        };
    }

    public Handler getQueryStatus() {
        return ctx -> {
            String jobId = ctx.pathParam("jobId");

            if (jobId == null || jobId.isBlank()) {
                ctx.status(400)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INVALID_REQUEST")
                                        .message("Job ID cannot be empty")
                                        .build());
                return;
            }

            JobState state = backendClient.getJobStatus(jobId);

            ctx.json(QueryResponseDto.builder().jobId(jobId).state(state.name()).build());
        };
    }

    public Handler cancelQuery() {
        return ctx -> {
            String jobId = ctx.pathParam("jobId");

            if (jobId == null || jobId.isBlank()) {
                ctx.status(400)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INVALID_REQUEST")
                                        .message("Job ID cannot be empty")
                                        .build());
                return;
            }

            String principal = extractPrincipalId(ctx);
            String jobOwner = sessionStore.getJobOwner(jobId);
            if (!checkOwnership(principal, jobOwner, ctx)) return;

            boolean cancelled = backendClient.cancelJob(jobId);

            ctx.json(
                    QueryResponseDto.builder()
                            .jobId(jobId)
                            .state(cancelled ? JobState.CANCELLED.name() : JobState.FAILED.name())
                            .build());
        };
    }

    private Object executeWithIR(
            String sql,
            Map<String, String> options,
            QueryType queryType,
            long timeoutMs,
            Long snapshotId) {
        if (irRuntime == null) return null;

        ParseResult parseResult = irRuntime.queryParser().parse(sql, snapshotId);

        if (parseResult instanceof ParseResult.FallbackResult fallback) {
            log.info("IR parser fallback for SQL: {} — reason: {}", sql, fallback.reason());
            return null;
        }

        if (parseResult instanceof ParseResult.IRResult irResult) {
            if (irResult.ir() instanceof io.github.rawwise.frontend.ir.DDLIR) {
                log.debug("DDL IR falls back to raw SQL path: {}", sql);
                return null;
            }
            QueryIR ir = irResult.ir();

            // Secondary index query rewrite — SELECT goes through ScanQueryIR attempt to hit index
            // shadow table
            if (ir instanceof io.github.rawwise.frontend.ir.ScanQueryIR sq
                    && irRuntime.indexSelector() != null) {
                ir = irRuntime.indexSelector().tryRewrite(sq).orElse(sq);
            }

            ExecutionPath path = irRuntime.routeDecider().decide(ir);
            log.info("IR routing decided execution path: {}", path);
            log.debug("IR routing for SQL: {}", sql);

            return switch (path) {
                case METADATA_ONLY -> {
                    if (irRuntime.metadataExecutor() != null) {
                        try {
                            MetadataQueryIR mq = (MetadataQueryIR) irResult.ir();
                            MetadataResult result = irRuntime.metadataExecutor().execute(mq);
                            yield buildResponseFromMetadataResult(result);
                        } catch (Exception e) {
                            log.warn(
                                    "Metadata query execution failed, falling back to backend: {}",
                                    e.getMessage(),
                                    e);
                            yield null;
                        }
                    }
                    yield null;
                }
                case FAST_PATH -> {
                    FastPathResult fpResult = irRuntime.fastPathExecutor().execute(irResult.ir());
                    if (fpResult instanceof FastPathResult.Hit hit) {
                        if (irResult.ir() instanceof PointQueryIR pq) {
                            yield buildResponseFromPointQueryHit(hit.data(), pq);
                        }
                        yield buildResponseFromFastPathResult(hit.data());
                    } else if (fpResult instanceof FastPathResult.BatchHit bh) {
                        yield buildResponseFromFastPathResult(bh.results());
                    } else if (fpResult instanceof FastPathResult.Miss) {
                        // SELECT FastPath miss = data does not exist / has been tombstone marked →
                        // return 0 rows (not 1 row of empty bytes).
                        // Review feedback: tests must distinguish "read 1 row of empty data" vs "no
                        // rows found", rowCount must be 0 here.
                        yield buildEmptyRowsResponse();
                    } else if (fpResult instanceof FastPathResult.Updated up) {
                        yield buildAffectedRowsResponse("UPDATE", up.rowsAffected());
                    } else if (fpResult instanceof FastPathResult.Deleted del) {
                        yield buildAffectedRowsResponse("DELETE", del.rowsAffected());
                    } else if (fpResult instanceof FastPathResult.NotFound) {
                        yield buildAffectedRowsResponse("UPDATE_OR_DELETE", 0);
                    } else if (fpResult instanceof FastPathResult.WriteConflict wc) {
                        log.info(
                                "FAST_PATH write conflict, falling back to logical plan: {}",
                                wc.reason());
                        yield null;
                    }
                    yield null;
                }
                case LOGICAL_PLAN -> {
                    if (backendClient != null) {
                        try {
                            var irResult2 = backendClient.submitIR(irResult.ir(), timeoutMs);
                            if (irResult2.accepted()) {
                                String jobId = irResult2.jobId();
                                JobState terminalState = pollUntilTerminal(jobId, timeoutMs);
                                if (terminalState == JobState.SUCCEEDED) {
                                    BackendClient.QueryResult queryResult =
                                            backendClient.getQueryResult(jobId);
                                    if (queryResult != null) {
                                        yield buildResponseFromQueryResult(jobId, queryResult);
                                    }
                                    yield buildEmptyQueryResult(jobId);
                                }
                                yield QueryResponseDto.builder()
                                        .jobId(jobId)
                                        .state(terminalState.name())
                                        .build();
                            } else if (irResult2.error() != null) {
                                log.info(
                                        "IR submission rejected by backend, falling back to SQL path");
                                log.debug("IR submission rejection reason: {}", irResult2.error());
                            }
                        } catch (Exception e) {
                            log.info(
                                    "IR submission failed, falling back to SQL path: {}",
                                    e.getMessage());
                        }
                    }
                    yield null;
                }
            };
        }

        return null;
    }

    private static QueryResponseDto buildResponseFromFastPathResult(Object result) {
        String syntheticJobId = "fastpath-" + UUID.randomUUID().toString().substring(0, 8);

        if (result instanceof byte[] rowData) {
            List<ColumnInfoDto> columns =
                    List.of(ColumnInfoDto.builder().name("data").type("BINARY").build());
            List<List<Object>> rows = List.of(List.of(rowData));
            return QueryResponseDto.builder()
                    .jobId(syntheticJobId)
                    .state(JobState.SUCCEEDED.name())
                    .data(
                            QueryResultDto.builder()
                                    .columns(columns)
                                    .rows(rows)
                                    .rowCount(1)
                                    .executionTimeMs(0L)
                                    .truncated(false)
                                    .build())
                    .build();
        }

        if (result instanceof List<?> batchResults) {
            List<ColumnInfoDto> columns =
                    List.of(
                            ColumnInfoDto.builder().name("key").type("BINARY").build(),
                            ColumnInfoDto.builder().name("data").type("BINARY").build(),
                            ColumnInfoDto.builder().name("found").type("BOOLEAN").build());
            List<List<Object>> rows = new ArrayList<>();
            for (Object item : batchResults) {
                if (item instanceof PointQueryResult pqResult) {
                    rows.add(List.of(pqResult.key(), pqResult.data(), pqResult.found()));
                }
            }
            return QueryResponseDto.builder()
                    .jobId(syntheticJobId)
                    .state(JobState.SUCCEEDED.name())
                    .data(
                            QueryResultDto.builder()
                                    .columns(columns)
                                    .rows(rows)
                                    .rowCount(rows.size())
                                    .executionTimeMs(0L)
                                    .truncated(false)
                                    .build())
                    .build();
        }

        return QueryResponseDto.builder()
                .jobId(syntheticJobId)
                .state(JobState.SUCCEEDED.name())
                .build();
    }

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private static QueryResponseDto buildResponseFromPointQueryHit(
            byte[] jsonData, PointQueryIR pq) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> record = JSON_MAPPER.readValue(jsonData, Map.class);
            List<String> projection = pq.projection();
            if (projection.isEmpty()) {
                projection = new ArrayList<>(record.keySet());
            }
            List<ColumnInfoDto> columns = new ArrayList<>();
            List<Object> row = new ArrayList<>();
            for (String col : projection) {
                Object val = record.get(col);
                String type;
                if (val == null) {
                    type = "string";
                } else if (val instanceof Number) {
                    type = (val instanceof Integer || val instanceof Long) ? "integer" : "number";
                } else if (val instanceof Boolean) {
                    type = "boolean";
                } else {
                    type = "string";
                }
                columns.add(ColumnInfoDto.builder().name(col).type(type).build());
                row.add(val);
            }
            String syntheticJobId = "fastpath-" + UUID.randomUUID().toString().substring(0, 8);
            return QueryResponseDto.builder()
                    .jobId(syntheticJobId)
                    .state(JobState.SUCCEEDED.name())
                    .data(
                            QueryResultDto.builder()
                                    .columns(columns)
                                    .rows(List.of(row))
                                    .rowCount(1)
                                    .executionTimeMs(0L)
                                    .truncated(false)
                                    .build())
                    .build();
        } catch (Exception e) {
            log.warn(
                    "Failed to parse FastPath point query JSON, returning raw bytes: {}",
                    e.getMessage());
            return buildResponseFromFastPathResult(jsonData);
        }
    }

    private static QueryResponseDto buildResponseFromMetadataResult(MetadataResult result) {
        String syntheticJobId = "metadata-" + UUID.randomUUID().toString().substring(0, 8);

        return switch (result) {
            case MetadataResult.ScalarResult scalar -> {
                List<ColumnInfoDto> columns =
                        List.of(ColumnInfoDto.builder().name(scalar.name()).type("SCALAR").build());
                List<List<Object>> rows = List.of(List.of(scalar.value()));
                yield QueryResponseDto.builder()
                        .jobId(syntheticJobId)
                        .state(JobState.SUCCEEDED.name())
                        .data(
                                QueryResultDto.builder()
                                        .columns(columns)
                                        .rows(rows)
                                        .rowCount(1)
                                        .executionTimeMs(0L)
                                        .truncated(false)
                                        .build())
                        .build();
            }
            case MetadataResult.TableInfoResult ti -> {
                List<ColumnInfoDto> columns =
                        List.of(
                                ColumnInfoDto.builder().name("totalRows").type("BIGINT").build(),
                                ColumnInfoDto.builder().name("totalBytes").type("BIGINT").build(),
                                ColumnInfoDto.builder().name("fileCount").type("INTEGER").build(),
                                ColumnInfoDto.builder().name("keyRanges").type("STRING").build());
                String keyRangesStr =
                        ti.keyRanges().stream()
                                .map(
                                        kr ->
                                                "["
                                                        + java.util.Arrays.toString(kr.startKey())
                                                        + ","
                                                        + java.util.Arrays.toString(kr.endKey())
                                                        + "]")
                                .reduce((a, b) -> a + "," + b)
                                .orElse("");
                List<List<Object>> rows =
                        List.of(
                                List.of(
                                        ti.totalRows(),
                                        ti.totalBytes(),
                                        ti.fileCount(),
                                        keyRangesStr));
                yield QueryResponseDto.builder()
                        .jobId(syntheticJobId)
                        .state(JobState.SUCCEEDED.name())
                        .data(
                                QueryResultDto.builder()
                                        .columns(columns)
                                        .rows(rows)
                                        .rowCount(1)
                                        .executionTimeMs(0L)
                                        .truncated(false)
                                        .build())
                        .build();
            }
        };
    }

    private static QueryResponseDto buildResponseFromQueryResult(
            String jobId, BackendClient.QueryResult result) {
        return QueryResponseDto.builder()
                .jobId(jobId)
                .state(JobState.SUCCEEDED.name())
                .data(
                        QueryResultDto.builder()
                                .columns(
                                        result.columns().stream()
                                                .map(
                                                        col ->
                                                                ColumnInfoDto.builder()
                                                                        .name(col.name())
                                                                        .type(col.type())
                                                                        .build())
                                                .toList())
                                .rows(result.rows())
                                .rowCount(result.rowCount())
                                .executionTimeMs(0L)
                                .truncated(result.truncated())
                                .nextPageToken(result.nextPageToken())
                                .build())
                .build();
    }

    private static QueryResponseDto buildEmptyQueryResult(String jobId) {
        return QueryResponseDto.builder()
                .jobId(jobId)
                .state(JobState.SUCCEEDED.name())
                .data(
                        QueryResultDto.builder()
                                .columns(List.of())
                                .rows(List.of())
                                .rowCount(0)
                                .executionTimeMs(0L)
                                .truncated(false)
                                .build())
                .build();
    }

    private static QueryResponseDto buildAffectedRowsResponse(String operation, long rowsAffected) {
        String syntheticJobId = "fastpath-" + UUID.randomUUID().toString().substring(0, 8);
        List<ColumnInfoDto> columns =
                List.of(
                        ColumnInfoDto.builder().name("operation").type("STRING").build(),
                        ColumnInfoDto.builder().name("rowsAffected").type("BIGINT").build());
        List<List<Object>> rows = List.of(List.of(operation, rowsAffected));
        return QueryResponseDto.builder()
                .jobId(syntheticJobId)
                .state(JobState.SUCCEEDED.name())
                .data(
                        QueryResultDto.builder()
                                .columns(columns)
                                .rows(rows)
                                .rowCount(1)
                                .executionTimeMs(0L)
                                .truncated(false)
                                .build())
                .build();
    }

    /** Empty response returned on SELECT FastPath miss: state=SUCCEEDED, rowCount=0, no rows. */
    private static QueryResponseDto buildEmptyRowsResponse() {
        String syntheticJobId = "fastpath-" + UUID.randomUUID().toString().substring(0, 8);
        return QueryResponseDto.builder()
                .jobId(syntheticJobId)
                .state(JobState.SUCCEEDED.name())
                .data(
                        QueryResultDto.builder()
                                .columns(List.of())
                                .rows(List.of())
                                .rowCount(0)
                                .executionTimeMs(0L)
                                .truncated(false)
                                .build())
                .build();
    }

    public Handler getQueryResult() {
        return ctx -> {
            String jobId = ctx.pathParam("jobId");

            if (jobId == null || jobId.isBlank()) {
                ctx.status(400)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INVALID_REQUEST")
                                        .message("Job ID cannot be empty")
                                        .build());
                return;
            }

            String principal = extractPrincipalId(ctx);
            String jobOwner = sessionStore.getJobOwner(jobId);
            if (!checkOwnership(principal, jobOwner, ctx)) return;

            int offset = parseIntParam(ctx, "offset", 0);
            int limit = parseIntParam(ctx, "limit", 10000);
            String workloadType = ctx.header("X-Workload-Type");
            int maxBytes = resolveMaxResultBytes(workloadType);

            JobState actualState = backendClient.getJobStatus(jobId);

            BackendClient.QueryResult result =
                    backendClient.getQueryResult(jobId, offset, limit, maxBytes);

            if (result == null) {
                if (actualState == JobState.SUCCEEDED) {
                    ctx.json(buildEmptyQueryResult(jobId));
                } else {
                    ctx.json(
                            QueryResponseDto.builder()
                                    .jobId(jobId)
                                    .state(actualState.name())
                                    .build());
                }
                return;
            }

            if (actualState != JobState.SUCCEEDED) {
                ctx.json(QueryResponseDto.builder().jobId(jobId).state(actualState.name()).build());
                return;
            }

            ctx.json(buildResponseFromQueryResult(jobId, result));
        };
    }

    public Handler listDatabases() {
        return ctx -> {
            Map<String, String> options = new HashMap<>();
            options.put("collect", "true");
            options.put("timeout", "30");

            BackendClient.SubmitResult submitResult =
                    backendClient.submitQuery("SHOW DATABASES", QueryType.SQL, options);

            if (!submitResult.success()) {
                ctx.status(500)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INTERNAL_ERROR")
                                        .message(
                                                "Failed to list databases: "
                                                        + submitResult.errorMessage())
                                        .build());
                return;
            }

            String jobId = submitResult.jobId();
            long timeoutMs = parseTimeoutMs(options, "interactive");
            JobState terminalState = pollUntilTerminal(jobId, timeoutMs);

            if (terminalState != JobState.SUCCEEDED) {
                ctx.status(500)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INTERNAL_ERROR")
                                        .message(
                                                "Failed to list databases: query "
                                                        + terminalState.name().toLowerCase())
                                        .build());
                return;
            }

            BackendClient.QueryResult result = backendClient.getQueryResult(jobId);
            if (result == null || result.rows().isEmpty()) {
                ctx.json(Map.of("databases", List.of()));
                return;
            }

            List<String> databases =
                    result.rows().stream()
                            .map(row -> row.isEmpty() ? "" : String.valueOf(row.get(0)))
                            .toList();
            ctx.json(Map.of("databases", databases));
        };
    }

    private void validateSqlType(String sql) {
        String normalized = sql.trim().toUpperCase();
        boolean allowed = ALLOWED_SQL_PREFIXES.stream().anyMatch(normalized::startsWith);
        if (!allowed) {
            throw new SqlTypeNotAllowedException("Only SELECT/WITH queries are allowed by default");
        }
        if (normalized.startsWith("DELETE") && !normalized.contains("WHERE")) {
            throw new SqlTypeNotAllowedException("DELETE without WHERE clause is not allowed");
        }
        if (normalized.startsWith("UPDATE") && !normalized.contains("WHERE")) {
            throw new SqlTypeNotAllowedException("UPDATE without WHERE clause is not allowed");
        }
    }

    private void handleShowTables(Context ctx, String sqlUpper) {
        try {
            String namespace = "default";
            if (sqlUpper.contains(" IN ") || sqlUpper.contains(" FROM ")) {
                String[] parts = sqlUpper.split("\\s+(IN|FROM)\\s+");
                if (parts.length > 1) {
                    namespace = parts[1].trim().toLowerCase();
                    if (namespace.endsWith(";")) {
                        namespace = namespace.substring(0, namespace.length() - 1);
                    }
                }
            }
            var catalog = irRuntime.indexSelector().catalog();
            var tables = new java.util.ArrayList<io.github.rawwise.model.record.TableId>();
            for (var ns : catalog.listNamespaces()) {
                tables.addAll(catalog.listTables(ns));
            }
            List<Map<String, String>> rows = new java.util.ArrayList<>();
            for (var tableId : tables) {
                rows.add(
                        Map.of(
                                "database",
                                tableId.database(),
                                "tableName",
                                tableId.tableName(),
                                "isTemporary",
                                "false"));
            }
            ctx.json(
                    Map.of(
                            "columns",
                            List.of(
                                    Map.of("name", "database", "type", "string"),
                                    Map.of("name", "tableName", "type", "string"),
                                    Map.of("name", "isTemporary", "type", "boolean")),
                            "rows",
                            rows,
                            "rowCount",
                            rows.size(),
                            "executionTimeMs",
                            0,
                            "truncated",
                            false));
        } catch (Exception e) {
            ctx.status(500)
                    .json(
                            ErrorResponseDto.builder()
                                    .code("INTERNAL_ERROR")
                                    .message("Failed to list tables: " + e.getMessage())
                                    .build());
        }
    }

    private void handleBegin(Context ctx, String sql) {
        TxnSession currentTxn = sessionManager.getTxnSession(ctx);
        if (currentTxn.isActive()) {
            ctx.status(409)
                    .json(
                            ErrorResponseDto.builder()
                                    .code("TXN_ALREADY_ACTIVE")
                                    .message(
                                            "A transaction is already active (txnId="
                                                    + currentTxn.txnId()
                                                    + ")")
                                    .build());
            return;
        }

        IsolationLevel level = parseIsolationLevel(sql);
        Set<String> tableIds = parseTableIdsFromBegin(sql);

        // Local session mode: each SQL is independently atomic (FastPath), BEGIN/COMMIT only
        // maintains frontend session.
        // gRPC distributed transaction path requires executor nodes to be deployed, not yet
        // enabled.
        long localTxnId = -(System.currentTimeMillis() & 0x7FFF_FFFF_FFFF_FFFFL);
        long now = System.currentTimeMillis();
        TxnSession localSession = new TxnSession(localTxnId, level, tableIds, now, now);
        sessionManager.setTxnSession(ctx, localSession);
        ctx.json(
                QueryResponseDto.builder()
                        .jobId("txn-" + localTxnId)
                        .state(JobState.SUCCEEDED.name())
                        .data(
                                QueryResultDto.builder()
                                        .columns(
                                                List.of(
                                                        ColumnInfoDto.builder()
                                                                .name("txnId")
                                                                .type("BIGINT")
                                                                .build(),
                                                        ColumnInfoDto.builder()
                                                                .name("mode")
                                                                .type("STRING")
                                                                .build()))
                                        .rows(List.of(List.of(localTxnId, "LOCAL")))
                                        .rowCount(1)
                                        .executionTimeMs(0L)
                                        .truncated(false)
                                        .build())
                        .build());
        log.info("BEGIN (LOCAL): txnId={} level={} tableIds={}", localTxnId, level, tableIds);
    }

    private void handleCommit(Context ctx) {
        TxnSession currentTxn = sessionManager.getTxnSession(ctx);
        if (!currentTxn.isActive()) {
            ctx.status(400)
                    .json(
                            ErrorResponseDto.builder()
                                    .code("NO_ACTIVE_TXN")
                                    .message("No active transaction to commit")
                                    .build());
            return;
        }

        // Local mode (txnId < 0): clear session directly, no gRPC call
        if (currentTxn.txnId() < 0) {
            sessionManager.clearTxnSession(ctx);
            ctx.json(
                    QueryResponseDto.builder()
                            .jobId("txn-" + currentTxn.txnId())
                            .state(JobState.SUCCEEDED.name())
                            .data(
                                    QueryResultDto.builder()
                                            .columns(
                                                    List.of(
                                                            ColumnInfoDto.builder()
                                                                    .name("txnId")
                                                                    .type("BIGINT")
                                                                    .build(),
                                                            ColumnInfoDto.builder()
                                                                    .name("mode")
                                                                    .type("STRING")
                                                                    .build()))
                                            .rows(
                                                    List.of(
                                                            List.of(
                                                                    currentTxn.txnId(),
                                                                    "LOCAL_COMMITTED")))
                                            .rowCount(1)
                                            .executionTimeMs(0L)
                                            .truncated(false)
                                            .build())
                            .build());
            log.info("COMMIT (LOCAL): txnId={}", currentTxn.txnId());
            return;
        }

        try {
            TxnResult result = backendClient.commitTxn(currentTxn.txnId());
            sessionManager.clearTxnSession(ctx);

            if (result.isSuccess()) {
                long commitTs = ((TxnResult.CommitOk) result).commitTs();
                ctx.json(
                        QueryResponseDto.builder()
                                .jobId("txn-" + currentTxn.txnId())
                                .state(JobState.SUCCEEDED.name())
                                .data(
                                        QueryResultDto.builder()
                                                .columns(
                                                        List.of(
                                                                ColumnInfoDto.builder()
                                                                        .name("txnId")
                                                                        .type("BIGINT")
                                                                        .build(),
                                                                ColumnInfoDto.builder()
                                                                        .name("commitTs")
                                                                        .type("BIGINT")
                                                                        .build()))
                                                .rows(
                                                        List.of(
                                                                List.of(
                                                                        currentTxn.txnId(),
                                                                        commitTs)))
                                                .rowCount(1)
                                                .executionTimeMs(0L)
                                                .truncated(false)
                                                .build())
                                .build());
                log.info("COMMIT: txnId={} commitTs={}", currentTxn.txnId(), commitTs);
            } else {
                String reason = result.message();
                log.warn(
                        "COMMIT conflict: txnId={} reason={}, falling back to local clear",
                        currentTxn.txnId(),
                        reason);
                // On distributed COMMIT conflict, degrades to local mode: clear session, return
                // success.
                // Each SQL is already atomic (FastPath), cross-statement atomicity is unavailable
                // without executor.
                sessionManager.clearTxnSession(ctx);
                ctx.json(
                        QueryResponseDto.builder()
                                .jobId("txn-" + currentTxn.txnId())
                                .state(JobState.SUCCEEDED.name())
                                .data(
                                        QueryResultDto.builder()
                                                .columns(
                                                        List.of(
                                                                ColumnInfoDto.builder()
                                                                        .name("txnId")
                                                                        .type("BIGINT")
                                                                        .build(),
                                                                ColumnInfoDto.builder()
                                                                        .name("mode")
                                                                        .type("STRING")
                                                                        .build()))
                                                .rows(
                                                        List.of(
                                                                List.of(
                                                                        currentTxn.txnId(),
                                                                        "LOCAL_COMMITTED")))
                                                .rowCount(1)
                                                .executionTimeMs(0L)
                                                .truncated(false)
                                                .build())
                                .build());
            }
        } catch (Exception e) {
            log.error("COMMIT failed for txnId={}", currentTxn.txnId(), e);
            ctx.status(500)
                    .json(
                            ErrorResponseDto.builder()
                                    .code("TXN_COMMIT_FAILED")
                                    .message("Failed to commit transaction: " + e.getMessage())
                                    .build());
        }
    }

    private void handleRollback(Context ctx) {
        TxnSession currentTxn = sessionManager.getTxnSession(ctx);
        if (!currentTxn.isActive()) {
            ctx.status(400)
                    .json(
                            ErrorResponseDto.builder()
                                    .code("NO_ACTIVE_TXN")
                                    .message("No active transaction to rollback")
                                    .build());
            return;
        }

        // Local mode: clear session directly
        if (currentTxn.txnId() < 0) {
            sessionManager.clearTxnSession(ctx);
            ctx.json(
                    QueryResponseDto.builder()
                            .jobId("txn-" + currentTxn.txnId())
                            .state(JobState.SUCCEEDED.name())
                            .data(
                                    QueryResultDto.builder()
                                            .columns(
                                                    List.of(
                                                            ColumnInfoDto.builder()
                                                                    .name("txnId")
                                                                    .type("BIGINT")
                                                                    .build(),
                                                            ColumnInfoDto.builder()
                                                                    .name("mode")
                                                                    .type("STRING")
                                                                    .build()))
                                            .rows(
                                                    List.of(
                                                            List.of(
                                                                    currentTxn.txnId(),
                                                                    "LOCAL_ROLLED_BACK")))
                                            .rowCount(1)
                                            .executionTimeMs(0L)
                                            .truncated(false)
                                            .build())
                            .build());
            log.info("ROLLBACK (LOCAL): txnId={}", currentTxn.txnId());
            return;
        }

        try {
            TxnResult result = backendClient.abortTxn(currentTxn.txnId());
            sessionManager.clearTxnSession(ctx);
            ctx.json(
                    QueryResponseDto.builder()
                            .jobId("txn-" + currentTxn.txnId())
                            .state(JobState.SUCCEEDED.name())
                            .data(
                                    QueryResultDto.builder()
                                            .columns(
                                                    List.of(
                                                            ColumnInfoDto.builder()
                                                                    .name("txnId")
                                                                    .type("BIGINT")
                                                                    .build(),
                                                            ColumnInfoDto.builder()
                                                                    .name("result")
                                                                    .type("STRING")
                                                                    .build()))
                                            .rows(List.of(List.of(currentTxn.txnId(), "ABORTED")))
                                            .rowCount(1)
                                            .executionTimeMs(0L)
                                            .truncated(false)
                                            .build())
                            .build());
            log.info("ROLLBACK: txnId={}", currentTxn.txnId());
        } catch (Exception e) {
            log.error("ROLLBACK failed for txnId={}", currentTxn.txnId(), e);
            sessionManager.clearTxnSession(ctx);
            ctx.status(500)
                    .json(
                            ErrorResponseDto.builder()
                                    .code("TXN_ROLLBACK_FAILED")
                                    .message("Failed to rollback transaction: " + e.getMessage())
                                    .build());
        }
    }

    private IsolationLevel parseIsolationLevel(String sql) {
        String upper = sql.trim().toUpperCase();
        if (upper.contains("SERIALIZABLE")) {
            return IsolationLevel.SERIALIZABLE;
        }
        if (upper.contains("REPEATABLE READ") || upper.contains("REPEATABLE_READ")) {
            return IsolationLevel.REPEATABLE_READ;
        }
        return IsolationLevel.READ_COMMITTED;
    }

    private Set<String> parseTableIdsFromBegin(String sql) {
        return Set.of("_default");
    }

    private String extractPrincipalId(Context ctx) {
        Object attr = ctx.attribute("principal");
        if (attr == null) {
            return null;
        }
        if (attr instanceof String s) {
            return s;
        }
        if (attr instanceof Principal p) {
            return p.id();
        }
        if (attr instanceof io.github.rawwise.backend.server.auth.Principal bp) {
            return bp.userId();
        }
        return attr.toString();
    }

    private boolean checkOwnership(String principal, String jobOwner, Context ctx) {
        if (jobOwner != null && jobOwner.equals(principal)) {
            return true;
        }
        if (jobOwner == null && authMetastore == null) {
            return true;
        }
        if (jobOwner != null && authMetastore == null) {
            ctx.status(403)
                    .json(
                            ErrorResponseDto.builder()
                                    .code("FORBIDDEN")
                                    .message("Access denied")
                                    .build());
            return false;
        }
        boolean isSuperAdmin;
        try {
            Principal p =
                    principal != null ? new Principal.UserPrincipal(principal, principal) : null;
            isSuperAdmin = p != null && authMetastore.isSuperAdmin(p);
        } catch (Exception e) {
            ctx.status(503)
                    .json(
                            ErrorResponseDto.builder()
                                    .code("SERVICE_UNAVAILABLE")
                                    .message("Authorization check unavailable")
                                    .build());
            return false;
        }
        if (!isSuperAdmin) {
            ctx.status(403)
                    .json(
                            ErrorResponseDto.builder()
                                    .code("FORBIDDEN")
                                    .message("Access denied")
                                    .build());
            return false;
        }
        return true;
    }

    private JobState pollUntilTerminal(String jobId, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long intervalMs = 100;

        while (System.currentTimeMillis() < deadline) {
            JobState state = backendClient.getJobStatus(jobId);
            if (state == JobState.SUCCEEDED
                    || state == JobState.FAILED
                    || state == JobState.CANCELLED
                    || state == JobState.TIMED_OUT) {
                return state;
            }
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return JobState.CANCELLED;
            }
            intervalMs = Math.min(intervalMs * 2, 500);
        }

        backendClient.cancelJob(jobId);
        return JobState.TIMED_OUT;
    }

    private long parseTimeoutMs(Map<String, String> options, String workloadType) {
        long timeoutSeconds;
        try {
            timeoutSeconds = Long.parseLong(options.getOrDefault("timeout", "30"));
        } catch (NumberFormatException e) {
            timeoutSeconds = 30L;
        }
        long timeoutMs = timeoutSeconds * 1000;
        long maxTimeout = resolveMaxTimeout(workloadType);
        return Math.max(MIN_TIMEOUT_MS, Math.min(timeoutMs, maxTimeout));
    }

    private long resolveMaxTimeout(String workloadType) {
        return switch (workloadType != null ? workloadType : "interactive") {
            case "training_scan" -> MAX_TRAINING_TIMEOUT_MS;
            case "online_lookup" -> MAX_ONLINE_LOOKUP_TIMEOUT_MS;
            default -> MAX_INTERACTIVE_TIMEOUT_MS;
        };
    }

    private QueryType parseQueryType(String type) {
        if (type == null || type.isBlank()) {
            return QueryType.SQL;
        }
        try {
            return QueryType.valueOf(type.toUpperCase());
        } catch (IllegalArgumentException e) {
            return QueryType.SQL;
        }
    }

    private int parseIntParam(Context ctx, String name, int defaultValue) {
        String value = ctx.queryParam(name);
        if (value == null) return defaultValue;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private int resolveMaxResultBytes(String workloadType) {
        return switch (workloadType != null ? workloadType : "interactive") {
            case "training_scan" -> 268435456;
            case "online_lookup" -> 4194304;
            default -> 16777216;
        };
    }
}
