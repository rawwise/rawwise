package io.github.rawwise.governance.auth;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProvenanceAuditor extends AuthAuditLogger {

    private final ProvenanceStore provenanceStore;
    private final boolean provenanceEnabled;

    public ProvenanceAuditor(
            boolean auditEnabled, ProvenanceStore provenanceStore, boolean provenanceEnabled) {
        super(auditEnabled);
        this.provenanceStore = provenanceStore;
        this.provenanceEnabled = provenanceEnabled;
    }

    @Override
    public void logAccess(
            Principal principal,
            Set<String> tables,
            Set<Permission.TableOp> ops,
            boolean granted,
            String path) {
        super.logAccess(principal, tables, ops, granted, path);
        if (!provenanceEnabled) return;
        String opsJson;
        try {
            opsJson = AuthJson.MAPPER.writeValueAsString(ops);
        } catch (Exception e) {
            opsJson = ops.toString();
        }
        for (String table : tables) {
            recordEvent(
                    buildEvent(
                            principal,
                            ProvenanceEvent.ActionType.ACCESS.name(),
                            table,
                            ProvenanceEvent.TargetType.TABLE.name(),
                            granted,
                            Map.of("ops", opsJson, "path", path != null ? path : "")));
        }
    }

    @Override
    public void logGrant(Principal principal, String roleId, String grantedBy) {
        super.logGrant(principal, roleId, grantedBy);
        if (!provenanceEnabled) return;
        recordEvent(
                buildEvent(
                        principal,
                        ProvenanceEvent.ActionType.GRANT.name(),
                        roleId,
                        ProvenanceEvent.TargetType.ROLE.name(),
                        true,
                        Map.of("grantedBy", grantedBy != null ? grantedBy : "")));
    }

    @Override
    public void logRevoke(Principal principal, String roleId, String revokedBy) {
        super.logRevoke(principal, roleId, revokedBy);
        if (!provenanceEnabled) return;
        recordEvent(
                buildEvent(
                        principal,
                        ProvenanceEvent.ActionType.REVOKE.name(),
                        roleId,
                        ProvenanceEvent.TargetType.ROLE.name(),
                        true,
                        Map.of("revokedBy", revokedBy != null ? revokedBy : "")));
    }

    @Override
    public void logRoleCreate(Principal caller, String roleId) {
        super.logRoleCreate(caller, roleId);
        if (!provenanceEnabled) return;
        recordEvent(
                buildEvent(
                        caller,
                        ProvenanceEvent.ActionType.ROLE_CREATE.name(),
                        roleId,
                        ProvenanceEvent.TargetType.ROLE.name(),
                        true,
                        Map.of()));
    }

    @Override
    public void logRoleDelete(Principal caller, String roleId) {
        super.logRoleDelete(caller, roleId);
        if (!provenanceEnabled) return;
        recordEvent(
                buildEvent(
                        caller,
                        ProvenanceEvent.ActionType.ROLE_DELETE.name(),
                        roleId,
                        ProvenanceEvent.TargetType.ROLE.name(),
                        true,
                        Map.of()));
    }

    @Override
    public void logTokenCreate(Principal caller, String principalId) {
        super.logTokenCreate(caller, principalId);
        if (!provenanceEnabled) return;
        recordEvent(
                buildEvent(
                        caller,
                        ProvenanceEvent.ActionType.TOKEN_CREATE.name(),
                        principalId,
                        ProvenanceEvent.TargetType.TOKEN.name(),
                        true,
                        Map.of()));
    }

    @Override
    public void logTokenRevoke(Principal caller, String principalId) {
        super.logTokenRevoke(caller, principalId);
        if (!provenanceEnabled) return;
        recordEvent(
                buildEvent(
                        caller,
                        ProvenanceEvent.ActionType.TOKEN_REVOKE.name(),
                        principalId,
                        ProvenanceEvent.TargetType.TOKEN.name(),
                        true,
                        Map.of()));
    }

    @Override
    public void logAgentCreate(Principal caller, String agentId) {
        super.logAgentCreate(caller, agentId);
        if (!provenanceEnabled) return;
        recordEvent(
                buildEvent(
                        caller,
                        ProvenanceEvent.ActionType.AGENT_CREATE.name(),
                        agentId,
                        ProvenanceEvent.TargetType.AGENT.name(),
                        true,
                        Map.of()));
    }

    private ProvenanceEvent buildEvent(
            Principal principal,
            String action,
            String targetId,
            String targetType,
            boolean granted,
            Map<String, String> context) {
        String principalType = principal instanceof Principal.AgentPrincipal ? "agent" : "user";
        return new ProvenanceEvent(
                UUID.randomUUID().toString(),
                principal.id(),
                principalType,
                action,
                targetId,
                targetType,
                granted,
                context,
                System.currentTimeMillis());
    }

    private void recordEvent(ProvenanceEvent event) {
        try {
            provenanceStore.record(event);
        } catch (IOException e) {
            log.error(
                    "Failed to record provenance event: action={}, principal={}",
                    event.action(),
                    event.principalId(),
                    e);
        }
    }
}
