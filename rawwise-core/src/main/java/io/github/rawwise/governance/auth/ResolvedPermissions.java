package io.github.rawwise.governance.auth;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public record ResolvedPermissions(
        String principalId, Map<String, Set<Permission.TableOp>> tablePermissions) {
    public ResolvedPermissions {
        tablePermissions =
                Map.copyOf(
                        tablePermissions.entrySet().stream()
                                .collect(
                                        Collectors.toMap(
                                                Map.Entry::getKey, e -> Set.copyOf(e.getValue()))));
    }

    public boolean hasPermission(String tableId, Permission.TableOp op) {
        Set<Permission.TableOp> ops = tablePermissions.get(tableId);
        if (ops == null) {
            return false;
        }
        if (ops.contains(Permission.TableOp.OWNERSHIP)) {
            return true;
        }
        return ops.contains(op);
    }

    public Set<Permission.TableOp> getPermissions(String tableId) {
        return tablePermissions.getOrDefault(tableId, Set.of());
    }
}
