package io.github.rawwise.frontend.api;

import com.fasterxml.jackson.core.JsonParseException;
import io.github.rawwise.axiom.memory.exception.AxiomException;
import io.github.rawwise.frontend.api.dto.ErrorResponseDto;
import io.github.rawwise.frontend.api.routes.AdminRoutes;
import io.github.rawwise.frontend.api.routes.AxiomRouteGroup;
import io.github.rawwise.frontend.api.routes.ContextRoutes;
import io.github.rawwise.frontend.api.routes.ExecutionRoutes;
import io.github.rawwise.frontend.api.routes.QueryRoutes;
import io.github.rawwise.frontend.api.routes.RoutingRoutes;
import io.github.rawwise.frontend.api.routes.SqlTypeNotAllowedException;
import io.github.rawwise.frontend.api.routes.TableRoutes;
import io.github.rawwise.frontend.api.session.SessionManager;
import io.github.rawwise.frontend.backend.BackendClient;
import io.github.rawwise.frontend.backend.BackendUnavailableException;
import io.github.rawwise.frontend.backend.ResultBudgetExceededException;
import io.github.rawwise.frontend.backend.ResultTruncatedException;
import io.github.rawwise.frontend.config.FrontendOptions;
import io.github.rawwise.frontend.evaluator.QueryEvaluator;
import io.github.rawwise.frontend.ir.IrRuntime;
import io.github.rawwise.frontend.ir.adaptor.IRAdaptException;
import io.github.rawwise.frontend.ir.executor.MetadataExecutionException;
import io.github.rawwise.frontend.mcp.RawWiseMcpServer;
import io.github.rawwise.frontend.session.FrontendSessionStore;
import io.github.rawwise.governance.auth.AuthFilter;
import io.github.rawwise.governance.auth.AuthMetastore;
import io.github.rawwise.governance.auth.AuthRoutes;
import io.github.rawwise.governance.telemetry.TelemetryConfig;
import io.github.rawwise.metastore.backup.MetastoreBackupManager;
import io.javalin.Javalin;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

@Slf4j
public class FrontendServer implements AutoCloseable {

    private final Javalin app;
    private final FrontendOptions options;
    private final IrRuntime irRuntime;
    private final MetastoreBackupManager backupManager;
    private final SessionManager sessionManager;

    public FrontendServer(
            FrontendOptions options,
            FrontendSessionStore sessionStore,
            BackendClient backendClient,
            QueryEvaluator queryEvaluator,
            @Nullable IrRuntime irRuntime,
            AuthFilter authFilter,
            AuthRoutes authRoutes,
            ContextRoutes contextRoutes,
            ExecutionRoutes executionRoutes,
            AxiomRouteGroup axiomRouteGroup,
            @Nullable AuthMetastore authMetastore,
            @Nullable RoutingRoutes routingRoutes,
            @Nullable RateLimitFilter rateLimitFilter,
            @Nullable MetastoreBackupManager backupManager,
            @Nullable SessionManager sessionManager) {
        this(
                options,
                sessionStore,
                backendClient,
                queryEvaluator,
                irRuntime,
                authFilter,
                authRoutes,
                contextRoutes,
                executionRoutes,
                axiomRouteGroup,
                authMetastore,
                routingRoutes,
                rateLimitFilter,
                backupManager,
                sessionManager,
                null);
    }

    public FrontendServer(
            FrontendOptions options,
            FrontendSessionStore sessionStore,
            BackendClient backendClient,
            QueryEvaluator queryEvaluator,
            @Nullable IrRuntime irRuntime,
            AuthFilter authFilter,
            AuthRoutes authRoutes,
            ContextRoutes contextRoutes,
            ExecutionRoutes executionRoutes,
            AxiomRouteGroup axiomRouteGroup,
            @Nullable AuthMetastore authMetastore,
            @Nullable RoutingRoutes routingRoutes,
            @Nullable RateLimitFilter rateLimitFilter,
            @Nullable MetastoreBackupManager backupManager,
            @Nullable SessionManager sessionManager,
            @Nullable RawWiseMcpServer mcpServer) {
        this.options = options;
        this.irRuntime = irRuntime;
        this.backupManager = backupManager;
        this.sessionManager = sessionManager != null ? sessionManager : new SessionManager();

        QueryRoutes queryRoutes =
                new QueryRoutes(
                        sessionStore,
                        backendClient,
                        queryEvaluator,
                        irRuntime,
                        authMetastore,
                        this.sessionManager);
        TableRoutes tableRoutes = new TableRoutes(backendClient);

        var appBuilder =
                Javalin.create(
                                cfg -> {
                                    cfg.jetty.threadPool =
                                            new org.eclipse.jetty.util.thread.QueuedThreadPool(
                                                    256, 8, 60_000);
                                    cfg.jetty.modifyServer(
                                            server -> {
                                                org.eclipse.jetty.server.Server jetty =
                                                        (org.eclipse.jetty.server.Server) server;
                                                jetty.setAttribute(
                                                        "org.eclipse.jetty.server.Request.maxFormContentSize",
                                                        -1);
                                                for (org.eclipse.jetty.server.Connector c :
                                                        jetty.getConnectors()) {
                                                    if (c
                                                            instanceof
                                                            org.eclipse.jetty.server.ServerConnector
                                                                            sc) {
                                                        sc.setIdleTimeout(
                                                                java.time.Duration.ofSeconds(120)
                                                                        .toMillis());
                                                        sc.setAcceptQueueSize(1024);
                                                    }
                                                }
                                            });
                                })
                        .before(
                                ctx -> {
                                    String traceId = ctx.header("X-Trace-Id");
                                    if (traceId == null || traceId.isBlank()) {
                                        traceId = TelemetryConfig.newTraceId();
                                    }
                                    TelemetryConfig.setTraceId(traceId);
                                    ctx.header("X-Trace-Id", traceId);
                                })
                        .after(ctx -> TelemetryConfig.clearTraceId())
                        .get(
                                "/health",
                                ctx -> {
                                    var runtime =
                                            java.lang.management.ManagementFactory
                                                    .getRuntimeMXBean();
                                    var memoryBean =
                                            java.lang.management.ManagementFactory
                                                    .getMemoryMXBean();
                                    var threadBean =
                                            java.lang.management.ManagementFactory
                                                    .getThreadMXBean();
                                    long heapUsed = memoryBean.getHeapMemoryUsage().getUsed();
                                    long heapMax = memoryBean.getHeapMemoryUsage().getMax();
                                    double heapUsage =
                                            heapMax > 0 ? (double) heapUsed / heapMax : 0;
                                    ctx.json(
                                            Map.of(
                                                    "status",
                                                    "UP",
                                                    "heapUsedMb",
                                                    heapUsed / (1024 * 1024),
                                                    "heapMaxMb",
                                                    heapMax / (1024 * 1024),
                                                    "heapUsage",
                                                    String.format("%.2f", heapUsage),
                                                    "threadCount",
                                                    threadBean.getThreadCount(),
                                                    "uptimeMs",
                                                    runtime.getUptime()));
                                })
                        .get(
                                "/ready",
                                ctx -> {
                                    var checks = new java.util.LinkedHashMap<String, Object>();
                                    boolean allReady = true;
                                    try {
                                        var clusterStatus = backendClient.getClusterStatus();
                                        checks.put("backend", "UP");
                                    } catch (Exception e) {
                                        checks.put(
                                                "backend",
                                                Map.of("status", "DOWN", "reason", e.getMessage()));
                                        allReady = false;
                                    }
                                    checks.put(
                                            "irParser",
                                            irRuntime != null ? "UP" : "NOT_CONFIGURED");
                                    if (!allReady) {
                                        ctx.status(503)
                                                .json(Map.of("status", "DOWN", "checks", checks));
                                    } else {
                                        ctx.json(Map.of("status", "UP", "checks", checks));
                                    }
                                });

        if (options.isAuthEnabled() && authFilter != null) {
            appBuilder.before(authFilter.authenticate).before(authFilter.authorize);
        } else if (!options.isAuthEnabled()) {
            appBuilder.before(
                    ctx -> {
                        ctx.attribute(
                                "principal",
                                new io.github.rawwise.governance.auth.Principal.UserPrincipal(
                                        "anonymous", "Anonymous"));
                    });
        }

        if (rateLimitFilter != null) {
            appBuilder.before(rateLimitFilter);
        }

        this.app =
                appBuilder
                        .get(
                                "/api/admin/security-status",
                                ctx -> {
                                    boolean explicitlyUnsafeDisable =
                                            Boolean.parseBoolean(
                                                    System.getProperty(
                                                            "rawwise.auth.explicitly-unsafe-disable",
                                                            "false"));
                                    String envMode = System.getProperty("rawwise.env", "dev");
                                    boolean secure =
                                            !"production".equalsIgnoreCase(envMode)
                                                    || !explicitlyUnsafeDisable;
                                    ctx.json(
                                            Map.of(
                                                    "secure",
                                                    secure,
                                                    "env",
                                                    envMode,
                                                    "explicitlyUnsafeDisable",
                                                    explicitlyUnsafeDisable));
                                })
                        .post(
                                "/api/admin/metastore/backup",
                                ctx -> {
                                    if (backupManager == null) {
                                        ctx.status(503)
                                                .json(Map.of("error", "backup not configured"));
                                        return;
                                    }
                                    backupManager.performBackup();
                                    ctx.json(Map.of("status", "backup_triggered"));
                                })
                        .get("/api/query/{jobId}/status", queryRoutes.getQueryStatus())
                        .post("/api/query", queryRoutes.submitQuery())
                        .post("/api/query/batch", queryRoutes.submitBatchQuery())
                        .get("/api/query/{jobId}/result", queryRoutes.getQueryResult())
                        .post("/api/query/{jobId}/cancel", queryRoutes.cancelQuery())
                        .get("/api/databases", queryRoutes.listDatabases())
                        .get("/api/tables", tableRoutes.listTables())
                        .get("/api/tables/{table}", tableRoutes.getTable())
                        .get("/api/tables/{table}/partitions", tableRoutes.getTablePartitions());

        if (routingRoutes != null) {
            this.app
                    .post("/api/admin/routing/{tableId}/initialize", routingRoutes.initialize())
                    .post("/api/admin/routing/{tableId}/entry", routingRoutes.addEntry())
                    .put("/api/admin/routing/{tableId}/entry/{seq}", routingRoutes.updateEntry())
                    .delete("/api/admin/routing/{tableId}/entry/{seq}", routingRoutes.removeEntry())
                    .put("/api/admin/routing/{tableId}/default", routingRoutes.updateDefault())
                    .get("/api/admin/routing/{tableId}", routingRoutes.getRoutingTable());
        }

        if (options.isAuthEnabled() && authRoutes != null) {
            this.app
                    .post("/api/v1/auth/roles", authRoutes.createRole)
                    .delete("/api/v1/auth/roles/{roleId}", authRoutes.deleteRole)
                    .get("/api/v1/auth/roles", authRoutes.listRoles)
                    .get("/api/v1/auth/roles/{roleId}", authRoutes.getRole)
                    .post("/api/v1/auth/grant", authRoutes.grantRole)
                    .post("/api/v1/auth/revoke", authRoutes.revokeRole)
                    .get("/api/v1/auth/permissions", authRoutes.getPermissions)
                    .post("/api/v1/auth/tokens", authRoutes.createToken)
                    .post("/api/v1/auth/agents", authRoutes.createAgent)
                    .delete("/api/v1/auth/tokens/{principalId}", authRoutes.revokeToken);
        }

        if (options.isContextEnabled() && contextRoutes != null) {
            this.app
                    .post("/api/v1/context/sessions", contextRoutes.createSession)
                    .get("/api/v1/context/sessions/{sessionId}", contextRoutes.getSession)
                    .delete("/api/v1/context/sessions/{sessionId}", contextRoutes.terminateSession)
                    .post(
                            "/api/v1/context/sessions/{sessionId}/entries",
                            contextRoutes.putSessionEntry)
                    .get(
                            "/api/v1/context/sessions/{sessionId}/entries/{key}",
                            contextRoutes.getSessionEntry)
                    .get(
                            "/api/v1/context/sessions/{sessionId}/entries",
                            contextRoutes.listSessionEntries)
                    .delete(
                            "/api/v1/context/sessions/{sessionId}/entries/{key}",
                            contextRoutes.removeSessionEntry)
                    .get(
                            "/api/v1/context/sessions/{sessionId}/entries/{key}/history",
                            contextRoutes.getEntryHistory)
                    .get(
                            "/api/v1/context/sessions/{sessionId}/entries/{key}/tier/{tier}",
                            contextRoutes.getEntryAtTier)
                    .get("/api/v1/context/sessions/{sessionId}/scan/l0", contextRoutes.scanL0)
                    .post("/api/v1/context/knowledge", contextRoutes.createKnowledge)
                    .get("/api/v1/context/knowledge/{knowledgeId}", contextRoutes.getKnowledge)
                    .get("/api/v1/context/knowledge", contextRoutes.listKnowledge)
                    .delete(
                            "/api/v1/context/knowledge/{knowledgeId}",
                            contextRoutes.deleteKnowledge)
                    .post(
                            "/api/v1/context/knowledge/{knowledgeId}/entries",
                            contextRoutes.putKnowledgeEntry)
                    .get(
                            "/api/v1/context/knowledge/{knowledgeId}/entries/{key}",
                            contextRoutes.getKnowledgeEntry)
                    .get(
                            "/api/v1/context/knowledge/{knowledgeId}/entries",
                            contextRoutes.listKnowledgeEntries)
                    .delete(
                            "/api/v1/context/knowledge/{knowledgeId}/entries/{key}",
                            contextRoutes.removeKnowledgeEntry)
                    .get(
                            "/api/v1/context/knowledge/{knowledgeId}/entries/{key}/history",
                            contextRoutes.getKnowledgeEntryHistory)
                    .get(
                            "/api/v1/context/knowledge/{knowledgeId}/entries/{key}/tier/{tier}",
                            contextRoutes.getKnowledgeEntryAtTier)
                    .get(
                            "/api/v1/context/knowledge/{knowledgeId}/scan/l0",
                            contextRoutes.scanKnowledgeL0);
        }

        if (options.isExecutionEnabled() && executionRoutes != null) {
            this.app
                    .post("/api/v1/execution/plans", executionRoutes.createPlan)
                    .get("/api/v1/execution/plans/{planId}", executionRoutes.getPlan)
                    .get("/api/v1/execution/plans", executionRoutes.listPlans)
                    .delete("/api/v1/execution/plans/{planId}", executionRoutes.cancelPlan)
                    .post(
                            "/api/v1/execution/plans/{planId}/steps/{stepIndex}/start",
                            executionRoutes.startStep)
                    .post(
                            "/api/v1/execution/plans/{planId}/steps/{stepIndex}/complete",
                            executionRoutes.completeStep)
                    .post(
                            "/api/v1/execution/plans/{planId}/steps/{stepIndex}/fail",
                            executionRoutes.failStep)
                    .post(
                            "/api/v1/execution/plans/{planId}/steps/{stepIndex}/skip",
                            executionRoutes.skipStep)
                    .post(
                            "/api/v1/execution/plans/{planId}/steps/{stepIndex}/retry",
                            executionRoutes.retryStep)
                    .post(
                            "/api/v1/execution/plans/{planId}/compensate",
                            executionRoutes.compensatePlan)
                    .post(
                            "/api/v1/execution/plans/{planId}/steps/{stepIndex}/compensate",
                            executionRoutes.compensateStep)
                    .post("/api/v1/execution/plans/{planId}/resume", executionRoutes.resumePlan)
                    .get(
                            "/api/v1/execution/plans/{planId}/steps/{stepIndex}/logs",
                            executionRoutes.getStepLogs);
        }

        if (options.isAxiomEnabled() && axiomRouteGroup != null) {
            axiomRouteGroup.register(this.app, options);
        }

        AdminRoutes.register(this.app);

        if (mcpServer != null) {
            mcpServer.register(this.app);
            log.info("MCP server registered");
        }

        this.app.exception(
                AxiomException.class,
                (e, ctx) -> {
                    log.error("Axiom error: {}", e.getErrorCode(), e);
                    ctx.status(e.getHttpStatus())
                            .json(
                                    Map.of(
                                            "error",
                                            Map.of(
                                                    "code",
                                                    e.getErrorCode(),
                                                    "message",
                                                    e.getMessage())));
                });

        this.app.exception(
                JsonParseException.class,
                (e, ctx) -> {
                    ctx.status(400)
                            .json(
                                    ErrorResponseDto.builder()
                                            .code("BAD_REQUEST")
                                            .message("Invalid request body")
                                            .build());
                });

        this.app.exception(
                SqlTypeNotAllowedException.class,
                (e, ctx) -> {
                    ctx.status(400)
                            .json(
                                    ErrorResponseDto.builder()
                                            .code("SQL_TYPE_NOT_ALLOWED")
                                            .message(e.getMessage())
                                            .build());
                });

        this.app.exception(
                IRAdaptException.class,
                (e, ctx) -> {
                    log.warn("IR adaptation failed: {}", e.getMessage());
                    ctx.status(422)
                            .json(
                                    ErrorResponseDto.builder()
                                            .code("IR_ADAPTATION_FAILED")
                                            .message("Query cannot be processed through IR path")
                                            .build());
                });

        this.app.exception(
                MetadataExecutionException.class,
                (e, ctx) -> {
                    log.error("Metadata execution failed", e);
                    ctx.status(500)
                            .json(
                                    ErrorResponseDto.builder()
                                            .code("METADATA_EXECUTION_FAILED")
                                            .message("Metadata query execution failed")
                                            .build());
                });

        this.app.exception(
                BackendUnavailableException.class,
                (e, ctx) -> {
                    ctx.status(503)
                            .json(
                                    ErrorResponseDto.builder()
                                            .code("SERVICE_UNAVAILABLE")
                                            .message("Backend service unavailable")
                                            .build());
                });

        this.app.exception(
                ResultBudgetExceededException.class,
                (e, ctx) -> {
                    ctx.status(507)
                            .json(
                                    ErrorResponseDto.builder()
                                            .code("RESULT_BUDGET_EXCEEDED")
                                            .message("Global result memory budget exceeded")
                                            .build());
                });

        this.app.exception(
                ResultTruncatedException.class,
                (e, ctx) -> {
                    ctx.status(413)
                            .json(
                                    ErrorResponseDto.builder()
                                            .code("RESULT_TRUNCATED")
                                            .message(e.getMessage())
                                            .build());
                });

        this.app.exception(
                Exception.class,
                (e, ctx) -> {
                    log.error("Unhandled exception", e);
                    ctx.status(500)
                            .json(
                                    ErrorResponseDto.builder()
                                            .code("INTERNAL_ERROR")
                                            .message("An internal error occurred")
                                            .build());
                });
    }

    public void start() {
        app.start(options.getPort());
        log.info("Frontend server started on port {}", app.port());
    }

    public int getPort() {
        return app.port();
    }

    @Override
    public void close() {
        if (irRuntime != null) {
            irRuntime.close();
        }
        if (sessionManager != null) {
            try {
                sessionManager.close();
            } catch (Exception e) {
                log.warn("Failed to close SessionManager", e);
            }
        }
        app.stop();
        log.info("Frontend server stopped");
    }

    public void stop() {
        close();
    }
}
