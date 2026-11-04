package io.github.rawwise.governance.auth;

import io.github.rawwise.frontend.ir.IRType;
import io.github.rawwise.frontend.ir.QueryIR;
import io.github.rawwise.frontend.ir.parser.ParseResult;
import io.github.rawwise.frontend.ir.parser.QueryParser;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AuthFilter {

    private static final long UNSAFE_DISABLE_WARN_INTERVAL_MS = 60_000;

    private final AuthMetastore authMetastore;
    private final PermissionCache permissionCache;
    private final TokenCache tokenCache;
    private final QueryParser queryParser;
    private final AuthAuditLogger auditLogger;
    private final boolean effectivelyDenyUnauthenticated;
    private final boolean explicitlyUnsafeDisabled;
    private volatile long lastUnsafeDisableWarnTs = 0;

    public final Handler authenticate;
    public final Handler authorize;

    public AuthFilter(
            AuthMetastore authMetastore,
            PermissionCache permissionCache,
            TokenCache tokenCache,
            QueryParser queryParser,
            AuthAuditLogger auditLogger,
            boolean unauthenticatedDeny) {
        this.authMetastore = authMetastore;
        this.permissionCache = permissionCache;
        this.tokenCache = tokenCache;
        this.queryParser = queryParser;
        this.auditLogger = auditLogger;

        String envMode = System.getProperty("rawwise.env", "dev");
        boolean explicitlyUnsafeDisable =
                Boolean.parseBoolean(
                        System.getProperty("rawwise.auth.explicitly-unsafe-disable", "false"));
        this.explicitlyUnsafeDisabled = explicitlyUnsafeDisable;
        if ("production".equalsIgnoreCase(envMode) && explicitlyUnsafeDisable) {
            log.warn(
                    "Production mode: ignoring rawwise.auth.explicitly-unsafe-disable=true, enforcing auth");
            this.effectivelyDenyUnauthenticated = true;
        } else if (explicitlyUnsafeDisable) {
            this.effectivelyDenyUnauthenticated = false;
        } else {
            this.effectivelyDenyUnauthenticated = unauthenticatedDeny;
        }

        this.authenticate =
                ctx -> {
                    if (isExcludedPath(ctx.path())) {
                        return;
                    }

                    boolean unsafeDisabled = explicitlyUnsafeDisabled;
                    if (unsafeDisabled && !effectivelyDenyUnauthenticated) {
                        long now = System.currentTimeMillis();
                        if (now - lastUnsafeDisableWarnTs > UNSAFE_DISABLE_WARN_INTERVAL_MS) {
                            lastUnsafeDisableWarnTs = now;
                            log.warn("╔══════════════════════════════════════════════════════╗");
                            log.warn("║  AUTH EXPLICITLY UNSAFE DISABLED                    ║");
                            log.warn("║  THIS IS UNSAFE - DO NOT USE IN PRODUCTION          ║");
                            log.warn("╚══════════════════════════════════════════════════════╝");
                        }
                    }

                    String token = extractToken(ctx);
                    if (token == null) {
                        if (AuthFilter.this.effectivelyDenyUnauthenticated) {
                            ctx.status(401).json(Map.of("error", "Missing Authorization header"));
                            ctx.skipRemainingHandlers();
                        } else {
                            ctx.attribute(
                                    "principal",
                                    new io.github.rawwise.governance.auth.Principal.UserPrincipal(
                                            "anonymous", "Anonymous"));
                        }
                        return;
                    }

                    try {
                        Principal principal =
                                AuthFilter.this.tokenCache.get(
                                        token, AuthFilter.this.authMetastore);
                        if (principal == null) {
                            ctx.status(401).json(Map.of("error", "Invalid token"));
                            ctx.skipRemainingHandlers();
                            return;
                        }
                        ctx.attribute("principal", principal);
                    } catch (IOException e) {
                        log.error("Authentication failed due to IO error", e);
                        ctx.status(503).json(Map.of("error", "Authentication service unavailable"));
                        ctx.skipRemainingHandlers();
                    }
                };

        this.authorize =
                ctx -> {
                    if (isExcludedPath(ctx.path())) {
                        return;
                    }

                    // In unsafe-disabled mode, all requests are allowed directly (only for local
                    // development/benchmark)
                    if (explicitlyUnsafeDisabled && !effectivelyDenyUnauthenticated) {
                        return;
                    }

                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        if (AuthFilter.this.effectivelyDenyUnauthenticated) {
                            ctx.status(401).json(Map.of("error", "Not authenticated"));
                            ctx.skipRemainingHandlers();
                        }
                        return;
                    }

                    if (ctx.path().startsWith("/api/admin/")) {
                        try {
                            if (!AuthFilter.this.authMetastore.isSuperAdmin(principal)) {
                                AuthFilter.this.auditLogger.logAccess(
                                        principal, Set.of(), Set.of(), false, ctx.path());
                                ctx.status(403).json(Map.of("error", "Admin access required"));
                                ctx.skipRemainingHandlers();
                                return;
                            }
                        } catch (IOException e) {
                            log.error(
                                    "Admin check failed for principal={}, denying access",
                                    principal.id(),
                                    e);
                            ctx.status(503)
                                    .json(Map.of("error", "Authorization service unavailable"));
                            ctx.skipRemainingHandlers();
                            return;
                        }
                    }

                    try {
                        if (AuthFilter.this.authMetastore.isSuperAdmin(principal)) {
                            AuthFilter.this.auditLogger.logAccess(
                                    principal, Set.of(), Set.of(), true, ctx.path());
                            return;
                        }
                    } catch (IOException e) {
                        log.error(
                                "Super admin check failed for principal={}, denying access",
                                principal.id(),
                                e);
                        ctx.status(503).json(Map.of("error", "Authorization service unavailable"));
                        ctx.skipRemainingHandlers();
                        return;
                    }

                    AuthContext authCtx = resolveAuthContext(ctx);
                    if (authCtx.requiredPermissions().isEmpty()) {
                        return;
                    }

                    try {
                        ResolvedPermissions perms =
                                AuthFilter.this.permissionCache.get(
                                        principal.id(), AuthFilter.this.authMetastore);

                        for (var entry : authCtx.requiredPermissions().entrySet()) {
                            String tableId = entry.getKey();
                            Set<Permission.TableOp> ops = entry.getValue();
                            for (Permission.TableOp op : ops) {
                                if (!perms.hasPermission(tableId, op)) {
                                    AuthFilter.this.auditLogger.logAccess(
                                            principal,
                                            authCtx.requiredPermissions().keySet(),
                                            ops,
                                            false,
                                            ctx.path());
                                    ctx.status(403)
                                            .json(
                                                    Map.of(
                                                            "error",
                                                            "Permission denied",
                                                            "required",
                                                            op.name(),
                                                            "tableId",
                                                            tableId));
                                    ctx.skipRemainingHandlers();
                                    return;
                                }
                            }
                        }

                        Set<Permission.TableOp> allOps =
                                authCtx.requiredPermissions().values().stream()
                                        .flatMap(Set::stream)
                                        .collect(Collectors.toSet());
                        AuthFilter.this.auditLogger.logAccess(
                                principal,
                                authCtx.requiredPermissions().keySet(),
                                allOps,
                                true,
                                ctx.path());
                    } catch (IOException e) {
                        log.error("Authorization failed due to IO error", e);
                        ctx.status(503).json(Map.of("error", "Authorization service unavailable"));
                        ctx.skipRemainingHandlers();
                    }
                };
    }

    private static boolean isExcludedPath(String path) {
        return "/health".equals(path) || "/ready".equals(path);
    }

    private AuthContext resolveAuthContext(Context ctx) {
        String path = ctx.path();
        String method = ctx.method().name();

        if (path.startsWith("/api/tables/")) {
            String table = path.substring("/api/tables/".length());
            int slashIdx = table.indexOf('/');
            if (slashIdx > 0) {
                table = table.substring(0, slashIdx);
            }
            if (table.isEmpty()) {
                return new AuthContext(Map.of());
            }
            Permission.TableOp op =
                    switch (method) {
                        case "GET" -> Permission.TableOp.SELECT;
                        case "POST" -> Permission.TableOp.CREATE;
                        case "PUT" -> Permission.TableOp.ALTER;
                        case "DELETE" -> Permission.TableOp.DROP;
                        default -> Permission.TableOp.SELECT;
                    };
            return new AuthContext(Map.of(table, Set.of(op)));
        }

        if ("/api/query".equals(path) && "POST".equals(method)) {
            if (queryParser == null) {
                return new AuthContext(Map.of("__DENY_ALL__", Set.of(Permission.TableOp.SELECT)));
            }
            try {
                String body = ctx.body();
                var node = AuthJson.MAPPER.readTree(body);
                String sql = node.has("sql") ? node.get("sql").asText() : null;
                if (sql == null || sql.isBlank()) {
                    return new AuthContext(Map.of());
                }
                ParseResult parseResult = queryParser.parse(sql);
                if (parseResult instanceof ParseResult.IRResult irResult) {
                    QueryIR ir = irResult.ir();
                    Set<String> tableIds = ir.tableIdentifiers();
                    Permission.TableOp op = inferOpFromIR(ir.type());
                    Map<String, Set<Permission.TableOp>> perms = new HashMap<>();
                    for (String tableId : tableIds) {
                        perms.put(tableId, Set.of(op));
                    }
                    return new AuthContext(perms);
                }
                return new AuthContext(Map.of("__DENY_ALL__", Set.of(Permission.TableOp.SELECT)));
            } catch (Exception e) {
                log.warn(
                        "Failed to parse SQL for auth context, denying by default: {}",
                        e.getMessage());
                return new AuthContext(Map.of("__DENY_ALL__", Set.of(Permission.TableOp.SELECT)));
            }
        }

        return new AuthContext(Map.of());
    }

    private Permission.TableOp inferOpFromIR(IRType irType) {
        return switch (irType) {
            case POINT_QUERY,
                            BATCH_POINT_QUERY,
                            SCAN,
                            AGGREGATE,
                            METADATA_COUNT_ALL,
                            METADATA_COUNT_COLUMN,
                            METADATA_MIN,
                            METADATA_MAX,
                            METADATA_TABLE_INFO,
                            JOIN,
                            REFRESH ->
                    Permission.TableOp.SELECT;
            case INSERT, BATCH_INSERT -> Permission.TableOp.INSERT;
            case UPDATE_RANGE, POINT_UPDATE -> Permission.TableOp.UPDATE;
            case DELETE_RANGE, POINT_DELETE -> Permission.TableOp.DELETE;
            case CREATE_TABLE -> Permission.TableOp.CREATE;
            case DROP_TABLE -> Permission.TableOp.DROP;
            case ALTER_TABLE, TRUNCATE -> Permission.TableOp.ALTER;
            case CREATE_INDEX, DROP_INDEX -> Permission.TableOp.ALTER;
        };
    }

    private String extractToken(Context ctx) {
        String header = ctx.header("Authorization");
        if (header != null && header.startsWith("Bearer ")) {
            return header.substring(7);
        }
        return null;
    }
}
