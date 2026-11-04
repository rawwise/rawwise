package io.github.rawwise.governance.auth;

import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultAuthMetastore implements AuthMetastore {

    private static final String PREFIX_TOKEN = "auth:token:";
    private static final String PREFIX_TOKEN_REV = "auth:token-rev:";
    private static final String PREFIX_ROLE = "auth:role:";
    private static final String PREFIX_BINDING = "auth:binding:";
    private static final String PREFIX_BINDING_BY_ROLE = "auth:binding-role:";
    private static final String PREFIX_PRINCIPAL = "auth:principal:";
    private static final String PREFIX_SUPERADMIN = "auth:superadmin:";

    private final Metastore metastore;

    public DefaultAuthMetastore(Metastore metastore) {
        this.metastore = metastore;
    }

    @Override
    public Principal resolveToken(String token) throws IOException {
        String tokenHash = AuthCrypto.sha256Hex(token);
        byte[] value =
                metastore
                        .get((PREFIX_TOKEN + tokenHash).getBytes(StandardCharsets.UTF_8))
                        .orElse(null);
        if (value == null) {
            return null;
        }
        return AuthJson.MAPPER.readValue(value, Principal.class);
    }

    @Override
    public void createToken(String token, Principal principal) throws IOException {
        String tokenHash = AuthCrypto.sha256Hex(token);
        byte[] value = AuthJson.MAPPER.writeValueAsBytes(principal);
        boolean success =
                metastore.compareAndSwap(
                        List.of(
                                new Precondition.NotExists(
                                        (PREFIX_TOKEN + tokenHash)
                                                .getBytes(StandardCharsets.UTF_8))),
                        List.of(
                                new Mutation.Put(
                                        (PREFIX_TOKEN + tokenHash).getBytes(StandardCharsets.UTF_8),
                                        value),
                                new Mutation.Put(
                                        (PREFIX_PRINCIPAL + encode(principal.id()))
                                                .getBytes(StandardCharsets.UTF_8),
                                        value),
                                new Mutation.Put(
                                        (PREFIX_TOKEN_REV + encode(principal.id()))
                                                .getBytes(StandardCharsets.UTF_8),
                                        tokenHash.getBytes(StandardCharsets.UTF_8))));
        if (!success) {
            throw new IOException("Token already exists");
        }
    }

    @Override
    public String revokeToken(String principalId) throws IOException {
        byte[] revValue =
                metastore
                        .get(
                                (PREFIX_TOKEN_REV + encode(principalId))
                                        .getBytes(StandardCharsets.UTF_8))
                        .orElse(null);
        String tokenHash = null;
        if (revValue != null) {
            tokenHash = new String(revValue, StandardCharsets.UTF_8);
            metastore.delete((PREFIX_TOKEN + tokenHash).getBytes(StandardCharsets.UTF_8));
            metastore.delete(
                    (PREFIX_TOKEN_REV + encode(principalId)).getBytes(StandardCharsets.UTF_8));
        }
        metastore.delete((PREFIX_PRINCIPAL + encode(principalId)).getBytes(StandardCharsets.UTF_8));
        return tokenHash;
    }

    @Override
    public void createRole(Role role) throws IOException {
        byte[] roleKey = (PREFIX_ROLE + encode(role.roleId())).getBytes(StandardCharsets.UTF_8);
        byte[] value = AuthJson.MAPPER.writeValueAsBytes(role);
        boolean success =
                metastore.compareAndSwap(
                        List.of(new Precondition.NotExists(roleKey)),
                        List.of(new Mutation.Put(roleKey, value)));
        if (!success) {
            throw new IOException("Role already exists: " + role.roleId());
        }
    }

    @Override
    public Optional<Role> getRole(String roleId) throws IOException {
        byte[] value =
                metastore
                        .get((PREFIX_ROLE + encode(roleId)).getBytes(StandardCharsets.UTF_8))
                        .orElse(null);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(AuthJson.MAPPER.readValue(value, Role.class));
    }

    @Override
    public List<Role> listRoles() throws IOException {
        List<Role> roles = new ArrayList<>();
        byte[] prefix = PREFIX_ROLE.getBytes(StandardCharsets.UTF_8);
        try (var iter = metastore.scan(prefix)) {
            while (iter.hasNext()) {
                var kv = iter.next();
                roles.add(AuthJson.MAPPER.readValue(kv.value(), Role.class));
            }
        }
        return roles;
    }

    @Override
    public void deleteRole(String roleId) throws IOException {
        byte[] roleKey = (PREFIX_ROLE + encode(roleId)).getBytes(StandardCharsets.UTF_8);
        List<Mutation> mutations = new ArrayList<>();
        mutations.add(new Mutation.Delete(roleKey));
        byte[] roleBindingPrefix =
                (PREFIX_BINDING_BY_ROLE + encode(roleId) + ":").getBytes(StandardCharsets.UTF_8);
        try (var iter = metastore.scan(roleBindingPrefix)) {
            while (iter.hasNext()) {
                var kv = iter.next();
                mutations.add(new Mutation.Delete(kv.key()));
                String keyStr = new String(kv.key(), StandardCharsets.UTF_8);
                int lastColon = keyStr.lastIndexOf(':');
                if (lastColon > 0) {
                    String principalIdEncoded =
                            keyStr.substring(
                                    PREFIX_BINDING_BY_ROLE.length() + encode(roleId).length() + 1,
                                    lastColon);
                    String bindingKey = PREFIX_BINDING + principalIdEncoded + ":" + encode(roleId);
                    mutations.add(new Mutation.Delete(bindingKey.getBytes(StandardCharsets.UTF_8)));
                }
            }
        }
        metastore.compareAndSwap(List.of(new Precondition.Exists(roleKey)), mutations);
    }

    @Override
    public void grantRole(Principal principal, String roleId, String grantedBy) throws IOException {
        byte[] roleKey = (PREFIX_ROLE + encode(roleId)).getBytes(StandardCharsets.UTF_8);
        if (metastore.get(roleKey).isEmpty()) {
            throw new IOException("Role does not exist: " + roleId);
        }
        String bindingKey = PREFIX_BINDING + encode(principal.id()) + ":" + encode(roleId);
        RoleBinding binding =
                new RoleBinding(principal, roleId, System.currentTimeMillis(), grantedBy);
        byte[] value = AuthJson.MAPPER.writeValueAsBytes(binding);
        String roleBindingKey =
                PREFIX_BINDING_BY_ROLE
                        + encode(roleId)
                        + ":"
                        + encode(principal.id())
                        + ":"
                        + encode(grantedBy);
        metastore.compareAndSwap(
                List.of(),
                List.of(
                        new Mutation.Put(bindingKey.getBytes(StandardCharsets.UTF_8), value),
                        new Mutation.Put(roleBindingKey.getBytes(StandardCharsets.UTF_8), value)));
    }

    @Override
    public void revokeRole(Principal principal, String roleId) throws IOException {
        String bindingKey = PREFIX_BINDING + encode(principal.id()) + ":" + encode(roleId);
        metastore.delete(bindingKey.getBytes(StandardCharsets.UTF_8));
        byte[] roleBindingPrefix =
                (PREFIX_BINDING_BY_ROLE + encode(roleId) + ":" + encode(principal.id()) + ":")
                        .getBytes(StandardCharsets.UTF_8);
        try (var iter = metastore.scan(roleBindingPrefix)) {
            while (iter.hasNext()) {
                var kv = iter.next();
                metastore.delete(kv.key());
            }
        }
    }

    @Override
    public List<RoleBinding> listBindings(Principal principal) throws IOException {
        List<RoleBinding> bindings = new ArrayList<>();
        String prefix = PREFIX_BINDING + encode(principal.id()) + ":";
        try (var iter = metastore.scan(prefix.getBytes(StandardCharsets.UTF_8))) {
            while (iter.hasNext()) {
                var kv = iter.next();
                RoleBinding binding = AuthJson.MAPPER.readValue(kv.value(), RoleBinding.class);
                if (getRole(binding.roleId()).isPresent()) {
                    bindings.add(binding);
                }
            }
        }
        return bindings;
    }

    @Override
    public ResolvedPermissions resolvePermissions(String principalId) throws IOException {
        Map<String, Set<Permission.TableOp>> tablePerms = new HashMap<>();
        Principal principal = getPrincipalById(principalId);
        if (principal == null) {
            return new ResolvedPermissions(principalId, Map.of());
        }

        List<RoleBinding> bindings = listBindings(principal);
        for (RoleBinding binding : bindings) {
            Optional<Role> roleOpt = getRole(binding.roleId());
            if (roleOpt.isEmpty()) {
                continue;
            }
            Role role = roleOpt.get();
            for (Permission perm : role.permissions()) {
                if (perm instanceof Permission.TablePermission tp) {
                    tablePerms.merge(
                            tp.tableId(),
                            tp.allowedOps(),
                            (existing, newOps) -> {
                                Set<Permission.TableOp> merged = EnumSet.copyOf(existing);
                                merged.addAll(newOps);
                                return merged;
                            });
                }
            }
        }
        return new ResolvedPermissions(principalId, tablePerms);
    }

    @Override
    public boolean isSuperAdmin(Principal principal) throws IOException {
        return metastore.exists(
                (PREFIX_SUPERADMIN + encode(principal.id())).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void setSuperAdmin(Principal principal) throws IOException {
        metastore.put(
                (PREFIX_SUPERADMIN + encode(principal.id())).getBytes(StandardCharsets.UTF_8),
                "1".getBytes(StandardCharsets.UTF_8));
    }

    private Principal getPrincipalById(String principalId) throws IOException {
        byte[] value =
                metastore
                        .get(
                                (PREFIX_PRINCIPAL + encode(principalId))
                                        .getBytes(StandardCharsets.UTF_8))
                        .orElse(null);
        if (value == null) {
            return null;
        }
        return AuthJson.MAPPER.readValue(value, Principal.class);
    }

    private static String encode(String raw) {
        return URLEncoder.encode(raw, StandardCharsets.UTF_8);
    }
}
