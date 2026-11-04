package io.github.rawwise.backend.wal;

public class WALRecoveryException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public WALRecoveryException(String message) {
        super(message);
    }

    public WALRecoveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
