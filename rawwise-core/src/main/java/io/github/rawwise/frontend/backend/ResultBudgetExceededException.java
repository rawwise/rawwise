package io.github.rawwise.frontend.backend;

public class ResultBudgetExceededException extends RuntimeException {
    public ResultBudgetExceededException(String message) {
        super(message);
    }
}
