package io.github.rawwise.governance.config;

public class ConfigException extends RuntimeException {

    public ConfigException(String message) {
        super(message);
    }

    public ConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public static class ParseError extends ConfigException {
        public ParseError(String message) {
            super(message);
        }

        public ParseError(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class TypeCoercionError extends ConfigException {
        public TypeCoercionError(String message) {
            super(message);
        }

        public TypeCoercionError(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class ValidationError extends ConfigException {
        public ValidationError(String message) {
            super(message);
        }
    }

    public static class UnknownKeyError extends ConfigException {
        public UnknownKeyError(String message) {
            super(message);
        }
    }
}
