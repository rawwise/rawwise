package io.github.rawwise.frontend.api.routes;

public class SqlTypeNotAllowedException extends RuntimeException {
    public SqlTypeNotAllowedException(String message) {
        super(message);
    }
}
