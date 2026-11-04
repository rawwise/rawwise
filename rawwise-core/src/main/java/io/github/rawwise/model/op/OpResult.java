package io.github.rawwise.model.op;

import java.util.Optional;

public interface OpResult {

    boolean success();

    default boolean isSuccess() {
        return success();
    }

    Optional<String> message();

    Optional<Throwable> error();

    long timestamp();

    static <T extends OpResult> T success(T result) {
        return result;
    }

    static Failure failure(String msg) {
        return new Failure(msg, null);
    }

    static Failure failure(Throwable cause) {
        return new Failure(cause.getMessage(), cause);
    }

    static Failure failure(String msg, Throwable cause) {
        return new Failure(msg, cause);
    }
}
