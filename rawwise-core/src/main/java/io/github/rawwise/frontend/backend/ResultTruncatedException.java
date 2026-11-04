package io.github.rawwise.frontend.backend;

public class ResultTruncatedException extends RuntimeException {
    public ResultTruncatedException(String message) {
        super(message);
    }
}
