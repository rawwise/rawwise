package io.github.rawwise.governance.auth;

import java.util.Map;
import java.util.Set;

public record AuthContext(Map<String, Set<Permission.TableOp>> requiredPermissions) {
    public AuthContext {
        requiredPermissions = Map.copyOf(requiredPermissions);
    }
}
