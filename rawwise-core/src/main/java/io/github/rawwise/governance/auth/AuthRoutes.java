package io.github.rawwise.governance.auth;

import io.javalin.http.Context;
import io.javalin.http.Handler;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AuthRoutes {

    private final AuthMetastore authMetastore;
    private final PermissionCache permissionCache;
    private final TokenCache tokenCache;
    private final AuthAuditLogger auditLogger;

    public final Handler createRole;
    public final Handler deleteRole;
    public final Handler listRoles;
    public final Handler getRole;
    public final Handler grantRole;
    public final Handler revokeRole;
    public final Handler getPermissions;
    public final Handler createToken;
    public final Handler createAgent;
    public final Handler revokeToken;

    public AuthRoutes(
            AuthMetastore authMetastore,
            PermissionCache permissionCache,
            TokenCache tokenCache,
            AuthAuditLogger auditLogger) {
        this.authMetastore = authMetastore;
        this.permissionCache = permissionCache;
        this.tokenCache = tokenCache;
        this.auditLogger = auditLogger;

        this.createRole =
                ctx -> {
                    Principal caller = requireAdmin(ctx);
                    if (caller == null) return;

                    try {
                        var body = ctx.bodyAsClass(CreateRoleRequest.class);
                        validateId(body.roleId(), "roleId");
                        Role role =
                                new Role(
                                        body.roleId(),
                                        body.description(),
                                        body.permissions(),
                                        System.currentTimeMillis(),
                                        System.currentTimeMillis());
                        AuthRoutes.this.authMetastore.createRole(role);
                        AuthRoutes.this.auditLogger.logRoleCreate(caller, role.roleId());
                        ctx.status(201).json(Map.of("roleId", role.roleId(), "status", "created"));
                    } catch (IOException e) {
                        log.error("Failed to create role", e);
                        ctx.status(500).json(Map.of("error", "Failed to create role"));
                        ctx.skipRemainingHandlers();
                    } catch (IllegalArgumentException e) {
                        ctx.status(400).json(Map.of("error", e.getMessage()));
                        ctx.skipRemainingHandlers();
                    }
                };

        this.deleteRole =
                ctx -> {
                    Principal caller = requireAdmin(ctx);
                    if (caller == null) return;

                    try {
                        String roleId = ctx.pathParam("roleId");
                        validateId(roleId, "roleId");
                        AuthRoutes.this.authMetastore.deleteRole(roleId);
                        AuthRoutes.this.permissionCache.invalidateAll();
                        AuthRoutes.this.auditLogger.logRoleDelete(caller, roleId);
                        ctx.json(Map.of("roleId", roleId, "status", "deleted"));
                    } catch (IOException e) {
                        log.error("Failed to delete role", e);
                        ctx.status(500).json(Map.of("error", "Failed to delete role"));
                        ctx.skipRemainingHandlers();
                    } catch (IllegalArgumentException e) {
                        ctx.status(400).json(Map.of("error", e.getMessage()));
                        ctx.skipRemainingHandlers();
                    }
                };

        this.listRoles =
                ctx -> {
                    Principal caller = requireAdmin(ctx);
                    if (caller == null) return;

                    try {
                        List<Role> roles = AuthRoutes.this.authMetastore.listRoles();
                        ctx.json(Map.of("roles", roles));
                    } catch (IOException e) {
                        log.error("Failed to list roles", e);
                        ctx.status(500).json(Map.of("error", "Failed to list roles"));
                        ctx.skipRemainingHandlers();
                    }
                };

        this.getRole =
                ctx -> {
                    Principal caller = requireAdmin(ctx);
                    if (caller == null) return;

                    try {
                        String roleId = ctx.pathParam("roleId");
                        validateId(roleId, "roleId");
                        var role = AuthRoutes.this.authMetastore.getRole(roleId);
                        if (role.isEmpty()) {
                            ctx.status(404).json(Map.of("error", "Role not found"));
                            return;
                        }
                        ctx.json(role.get());
                    } catch (IOException e) {
                        log.error("Failed to get role", e);
                        ctx.status(500).json(Map.of("error", "Failed to get role"));
                        ctx.skipRemainingHandlers();
                    } catch (IllegalArgumentException e) {
                        ctx.status(400).json(Map.of("error", e.getMessage()));
                        ctx.skipRemainingHandlers();
                    }
                };

        this.grantRole =
                ctx -> {
                    Principal caller = requireAdmin(ctx);
                    if (caller == null) return;

                    try {
                        var body = ctx.bodyAsClass(GrantRevokeRequest.class);
                        validateId(body.principalId(), "principalId");
                        validateId(body.roleId(), "roleId");
                        if (AuthRoutes.this.authMetastore.getRole(body.roleId()).isEmpty()) {
                            throw new IllegalArgumentException("Role not found: " + body.roleId());
                        }
                        Principal target = resolvePrincipalFromRequest(body, caller);
                        AuthRoutes.this.authMetastore.grantRole(target, body.roleId(), caller.id());
                        AuthRoutes.this.permissionCache.invalidate(target.id());
                        AuthRoutes.this.auditLogger.logGrant(target, body.roleId(), caller.id());
                        ctx.json(
                                Map.of(
                                        "principalId",
                                        target.id(),
                                        "roleId",
                                        body.roleId(),
                                        "status",
                                        "granted"));
                    } catch (IOException e) {
                        log.error("Failed to grant role", e);
                        ctx.status(500).json(Map.of("error", "Failed to grant role"));
                        ctx.skipRemainingHandlers();
                    } catch (IllegalArgumentException e) {
                        ctx.status(400).json(Map.of("error", e.getMessage()));
                        ctx.skipRemainingHandlers();
                    }
                };

        this.revokeRole =
                ctx -> {
                    Principal caller = requireAdmin(ctx);
                    if (caller == null) return;

                    try {
                        var body = ctx.bodyAsClass(GrantRevokeRequest.class);
                        validateId(body.principalId(), "principalId");
                        validateId(body.roleId(), "roleId");
                        Principal target = resolvePrincipalFromRequest(body, caller);
                        AuthRoutes.this.authMetastore.revokeRole(target, body.roleId());
                        AuthRoutes.this.permissionCache.invalidate(target.id());
                        AuthRoutes.this.auditLogger.logRevoke(target, body.roleId(), caller.id());
                        ctx.json(
                                Map.of(
                                        "principalId",
                                        target.id(),
                                        "roleId",
                                        body.roleId(),
                                        "status",
                                        "revoked"));
                    } catch (IOException e) {
                        log.error("Failed to revoke role", e);
                        ctx.status(500).json(Map.of("error", "Failed to revoke role"));
                        ctx.skipRemainingHandlers();
                    } catch (IllegalArgumentException e) {
                        ctx.status(400).json(Map.of("error", e.getMessage()));
                        ctx.skipRemainingHandlers();
                    }
                };

        this.getPermissions =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401).json(Map.of("error", "Not authenticated"));
                        ctx.skipRemainingHandlers();
                        return;
                    }

                    try {
                        ResolvedPermissions perms =
                                AuthRoutes.this.permissionCache.get(
                                        principal.id(), AuthRoutes.this.authMetastore);
                        ctx.json(
                                Map.of(
                                        "principalId",
                                        principal.id(),
                                        "permissions",
                                        perms.tablePermissions()));
                    } catch (IOException e) {
                        log.error("Failed to get permissions", e);
                        ctx.status(500).json(Map.of("error", "Failed to get permissions"));
                        ctx.skipRemainingHandlers();
                    }
                };

        this.createToken =
                ctx -> {
                    Principal caller = requireAdmin(ctx);
                    if (caller == null) return;

                    try {
                        var body = ctx.bodyAsClass(CreateTokenRequest.class);
                        validateId(body.principalId(), "principalId");
                        String token = AuthCrypto.generateSecureToken();
                        Principal principal =
                                new Principal.UserPrincipal(body.principalId(), body.displayName());
                        AuthRoutes.this.authMetastore.createToken(token, principal);
                        AuthRoutes.this.auditLogger.logTokenCreate(caller, body.principalId());
                        ctx.json(
                                Map.of(
                                        "token",
                                        token,
                                        "principalId",
                                        body.principalId(),
                                        "message",
                                        "Please save this token securely. It will not be shown again."));
                    } catch (IOException e) {
                        log.error("Failed to create token", e);
                        ctx.status(500).json(Map.of("error", "Failed to create token"));
                        ctx.skipRemainingHandlers();
                    } catch (IllegalArgumentException e) {
                        ctx.status(400).json(Map.of("error", e.getMessage()));
                        ctx.skipRemainingHandlers();
                    }
                };

        this.createAgent =
                ctx -> {
                    Principal caller = requireAdmin(ctx);
                    if (caller == null) return;

                    try {
                        var body = ctx.bodyAsClass(CreateAgentRequest.class);
                        validateId(body.agentId(), "agentId");
                        if (body.userId() != null) {
                            validateId(body.userId(), "userId");
                        }
                        String token = AuthCrypto.generateSecureToken();
                        Principal agent =
                                new Principal.AgentPrincipal(
                                        body.agentId(), body.userId(), caller.id());
                        AuthRoutes.this.authMetastore.createToken(token, agent);

                        if (body.initialRoles() != null) {
                            for (String roleId : body.initialRoles()) {
                                validateId(roleId, "roleId in initialRoles");
                                if (AuthRoutes.this.authMetastore.getRole(roleId).isEmpty()) {
                                    throw new IllegalArgumentException("Role not found: " + roleId);
                                }
                                AuthRoutes.this.authMetastore.grantRole(agent, roleId, caller.id());
                            }
                        }

                        AuthRoutes.this.auditLogger.logAgentCreate(caller, body.agentId());
                        ctx.json(
                                Map.of(
                                        "agentId",
                                        body.agentId(),
                                        "token",
                                        token,
                                        "message",
                                        "Please save this token securely. It will not be shown again."));
                    } catch (IOException e) {
                        log.error("Failed to create agent", e);
                        ctx.status(500).json(Map.of("error", "Failed to create agent"));
                        ctx.skipRemainingHandlers();
                    } catch (IllegalArgumentException e) {
                        ctx.status(400).json(Map.of("error", e.getMessage()));
                        ctx.skipRemainingHandlers();
                    }
                };

        this.revokeToken =
                ctx -> {
                    Principal caller = requireAdmin(ctx);
                    if (caller == null) return;

                    try {
                        String principalId = ctx.pathParam("principalId");
                        validateId(principalId, "principalId");
                        String tokenHash = AuthRoutes.this.authMetastore.revokeToken(principalId);
                        AuthRoutes.this.permissionCache.invalidate(principalId);
                        if (tokenHash != null) {
                            AuthRoutes.this.tokenCache.invalidateByHash(tokenHash);
                        }
                        AuthRoutes.this.auditLogger.logTokenRevoke(caller, principalId);
                        ctx.json(Map.of("principalId", principalId, "status", "revoked"));
                    } catch (IOException e) {
                        log.error("Failed to revoke token", e);
                        ctx.status(500).json(Map.of("error", "Failed to revoke token"));
                        ctx.skipRemainingHandlers();
                    } catch (IllegalArgumentException e) {
                        ctx.status(400).json(Map.of("error", e.getMessage()));
                        ctx.skipRemainingHandlers();
                    }
                };
    }

    private Principal requireAdmin(Context ctx) {
        Principal caller = ctx.attribute("principal");
        if (caller == null) {
            ctx.status(401).json(Map.of("error", "Not authenticated"));
            ctx.skipRemainingHandlers();
            return null;
        }
        try {
            if (!authMetastore.isSuperAdmin(caller)) {
                auditLogger.logAccess(caller, Set.of(), Set.of(), false, ctx.path());
                ctx.status(403).json(Map.of("error", "Admin permission required"));
                ctx.skipRemainingHandlers();
                return null;
            }
        } catch (IOException e) {
            log.error("Admin check failed", e);
            ctx.status(503).json(Map.of("error", "Authorization service unavailable"));
            ctx.skipRemainingHandlers();
            return null;
        }
        return caller;
    }

    private Principal resolvePrincipalFromRequest(GrantRevokeRequest body, Principal caller) {
        if ("agent".equals(body.principalType())) {
            if (body.userId() != null) {
                validateId(body.userId(), "userId");
            }
            return new Principal.AgentPrincipal(body.principalId(), body.userId(), caller.id());
        }
        return new Principal.UserPrincipal(body.principalId(), body.principalId());
    }

    private static void validateId(String id, String fieldName) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be empty");
        }
        if (id.length() > 128) {
            throw new IllegalArgumentException(fieldName + " too long (max 128)");
        }
        if (!id.matches("^[a-zA-Z0-9_.\\-]+$")) {
            throw new IllegalArgumentException(fieldName + " contains invalid characters");
        }
    }

    record CreateRoleRequest(String roleId, String description, Set<Permission> permissions) {}

    record GrantRevokeRequest(
            String principalId,
            String principalType,
            String userId,
            String delegatedBy,
            String roleId) {}

    record CreateTokenRequest(String principalId, String displayName) {}

    record CreateAgentRequest(
            String agentId, String userId, String delegatedBy, List<String> initialRoles) {}
}
