package io.github.rawwise.engine.txn.clock;

public final class FatalClockException extends RuntimeException {

    public FatalClockException(String message) {
        super(message);
    }

    public FatalClockException(String message, Throwable cause) {
        super(message, cause);
    }
}
