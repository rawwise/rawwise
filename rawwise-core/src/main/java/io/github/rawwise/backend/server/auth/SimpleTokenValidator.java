package io.github.rawwise.backend.server.auth;

import java.util.Set;

public class SimpleTokenValidator implements TokenValidator {

    private static final Principal DEFAULT_ADMIN =
            new Principal(
                    "admin",
                    "default",
                    Role.ADMIN,
                    Set.of(Permission.DDL, Permission.WRITE_PATH, Permission.READ_CROSS_TENANT));

    @Override
    public boolean validate(String token) {
        return token != null && !token.isBlank();
    }

    @Override
    public Principal extractPrincipal(String token) {
        return DEFAULT_ADMIN;
    }
}
