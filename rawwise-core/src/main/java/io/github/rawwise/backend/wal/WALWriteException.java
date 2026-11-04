package io.github.rawwise.backend.wal;

public class WALWriteException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public WALWriteException(String message) {
        super(message);
    }

    public WALWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}
