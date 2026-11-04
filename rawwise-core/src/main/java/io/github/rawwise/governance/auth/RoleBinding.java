package io.github.rawwise.governance.auth;

public record RoleBinding(Principal principal, String roleId, long grantedAt, String grantedBy) {}
