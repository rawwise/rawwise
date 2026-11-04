package io.github.rawwise.governance.auth;

import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AuthAuditLogger {

    private final boolean enabled;

    public AuthAuditLogger(boolean enabled) {
        this.enabled = enabled;
    }

    public void logAccess(
            Principal principal,
            Set<String> tables,
            Set<Permission.TableOp> ops,
            boolean granted,
            String path) {
        if (!enabled) {
            return;
        }
        log.info(
                "AUTH_AUDIT|principal={}|tables={}|op={}|result={}|path={}",
                principal.id(),
                tables,
                ops,
                granted ? "ALLOW" : "DENY",
                path);
    }

    public void logGrant(Principal principal, String roleId, String grantedBy) {
        if (!enabled) {
            return;
        }
        log.info(
                "AUTH_AUDIT|action=GRANT|principal={}|roleId={}|grantedBy={}",
                principal.id(),
                roleId,
                grantedBy);
    }

    public void logRevoke(Principal principal, String roleId, String revokedBy) {
        if (!enabled) {
            return;
        }
        log.info(
                "AUTH_AUDIT|action=REVOKE|principal={}|roleId={}|revokedBy={}",
                principal.id(),
                roleId,
                revokedBy);
    }

    public void logRoleCreate(Principal caller, String roleId) {
        if (!enabled) {
            return;
        }
        log.info("AUTH_AUDIT|action=ROLE_CREATE|roleId={}|caller={}", roleId, caller.id());
    }

    public void logRoleDelete(Principal caller, String roleId) {
        if (!enabled) {
            return;
        }
        log.info("AUTH_AUDIT|action=ROLE_DELETE|roleId={}|caller={}", roleId, caller.id());
    }

    public void logTokenCreate(Principal caller, String principalId) {
        if (!enabled) {
            return;
        }
        log.info(
                "AUTH_AUDIT|action=TOKEN_CREATE|principalId={}|caller={}",
                principalId,
                caller.id());
    }

    public void logTokenRevoke(Principal caller, String principalId) {
        if (!enabled) {
            return;
        }
        log.info(
                "AUTH_AUDIT|action=TOKEN_REVOKE|principalId={}|caller={}",
                principalId,
                caller.id());
    }

    public void logAgentCreate(Principal caller, String agentId) {
        if (!enabled) {
            return;
        }
        log.info("AUTH_AUDIT|action=AGENT_CREATE|agentId={}|caller={}", agentId, caller.id());
    }
}
