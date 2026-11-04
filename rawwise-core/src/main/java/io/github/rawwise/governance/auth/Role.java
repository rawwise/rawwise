package io.github.rawwise.governance.auth;

import java.util.Set;

public record Role(
        String roleId,
        String description,
        Set<Permission> permissions,
        long createdAt,
        long updatedAt) {
    public Role {
        permissions = Set.copyOf(permissions);
    }
}
