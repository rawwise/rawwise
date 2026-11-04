package io.github.rawwise.backend.server.auth;

public interface TokenValidator {
    boolean validate(String token);

    Principal extractPrincipal(String token);
}
