package io.github.rawwise.model.op;

import java.util.Optional;

public record Failure(String errorMsg, Throwable errorCause) implements OpResult {

    @Override
    public boolean success() {
        return false;
    }

    @Override
    public Optional<String> message() {
        return Optional.ofNullable(errorMsg);
    }

    @Override
    public Optional<Throwable> error() {
        return Optional.ofNullable(errorCause);
    }

    @Override
    public long timestamp() {
        return System.currentTimeMillis();
    }
}
