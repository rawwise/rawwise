package io.github.rawwise.backend.wal;

public class WALDurabilityException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final boolean degraded;

    public WALDurabilityException(String message) {
        super(message);
        this.degraded = false;
    }

    public WALDurabilityException(String message, boolean degraded) {
        super(message);
        this.degraded = degraded;
    }

    public WALDurabilityException(String message, Throwable cause) {
        super(message, cause);
        this.degraded = false;
    }

    public WALDurabilityException(String message, Throwable cause, boolean degraded) {
        super(message, cause);
        this.degraded = degraded;
    }

    public boolean isDegraded() {
        return degraded;
    }
}
