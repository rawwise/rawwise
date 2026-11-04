package io.github.rawwise.engine.storage.writer;

public record WriteResult(long recordsWritten, long recordsFailed) {
    public static final WriteResult SUCCESS = new WriteResult(1, 0);
    public static final WriteResult FAILURE = new WriteResult(0, 1);

    public WriteResult {
        if (recordsWritten < 0) throw new IllegalArgumentException("recordsWritten must be >= 0");
        if (recordsFailed < 0) throw new IllegalArgumentException("recordsFailed must be >= 0");
    }

    public boolean isFullSuccess() {
        return recordsFailed == 0;
    }

    public WriteResult add(WriteResult other) {
        return new WriteResult(
                this.recordsWritten + other.recordsWritten,
                this.recordsFailed + other.recordsFailed);
    }
}
