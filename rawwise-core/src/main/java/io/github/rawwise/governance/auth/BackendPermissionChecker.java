package io.github.rawwise.governance.auth;

public interface BackendPermissionChecker {

    BackendPermissionChecker NO_OP = (principalId, tableId, op) -> true;

    boolean checkPermission(String principalId, String tableId, Permission.TableOp op);
}
