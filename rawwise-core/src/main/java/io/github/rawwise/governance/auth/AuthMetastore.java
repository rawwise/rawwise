package io.github.rawwise.governance.auth;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface AuthMetastore {

    Principal resolveToken(String token) throws IOException;

    void createToken(String token, Principal principal) throws IOException;

    String revokeToken(String principalId) throws IOException;

    void createRole(Role role) throws IOException;

    Optional<Role> getRole(String roleId) throws IOException;

    List<Role> listRoles() throws IOException;

    void deleteRole(String roleId) throws IOException;

    void grantRole(Principal principal, String roleId, String grantedBy) throws IOException;

    void revokeRole(Principal principal, String roleId) throws IOException;

    List<RoleBinding> listBindings(Principal principal) throws IOException;

    ResolvedPermissions resolvePermissions(String principalId) throws IOException;

    boolean isSuperAdmin(Principal principal) throws IOException;

    void setSuperAdmin(Principal principal) throws IOException;
}
