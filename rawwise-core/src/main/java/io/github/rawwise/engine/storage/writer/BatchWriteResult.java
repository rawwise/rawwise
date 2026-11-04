package io.github.rawwise.engine.storage.writer;

public record BatchWriteResult(long recordsWritten, long recordsFailed, long bytesWritten) {
    public BatchWriteResult {
        if (recordsWritten < 0) throw new IllegalArgumentException("recordsWritten must be >= 0");
        if (recordsFailed < 0) throw new IllegalArgumentException("recordsFailed must be >= 0");
        if (bytesWritten < 0) throw new IllegalArgumentException("bytesWritten must be >= 0");
    }

    public boolean isFullSuccess() {
        return recordsFailed == 0;
    }

    public BatchWriteResult add(BatchWriteResult other) {
        return new BatchWriteResult(
                this.recordsWritten + other.recordsWritten,
                this.recordsFailed + other.recordsFailed,
                this.bytesWritten + other.bytesWritten);
    }
}
