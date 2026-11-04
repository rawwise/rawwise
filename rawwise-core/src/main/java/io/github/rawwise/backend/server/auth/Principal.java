package io.github.rawwise.backend.server.auth;

import java.util.Set;

public record Principal(String userId, String tenantId, Role role, Set<Permission> permissions) {
    public boolean hasPermission(Permission permission) {
        return permissions.contains(permission);
    }
}
