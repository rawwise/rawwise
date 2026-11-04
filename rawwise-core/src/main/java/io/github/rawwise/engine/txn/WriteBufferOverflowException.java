package io.github.rawwise.engine.txn;

public class WriteBufferOverflowException extends RuntimeException {

    private final long currentBytes;
    private final long maxBytes;
    private final long attemptedBytes;

    public WriteBufferOverflowException(long currentBytes, long maxBytes, long attemptedBytes) {
        super(
                String.format(
                        "WriteBuffer overflow: current=%d, max=%d, attempted=%d",
                        currentBytes, maxBytes, attemptedBytes));
        this.currentBytes = currentBytes;
        this.maxBytes = maxBytes;
        this.attemptedBytes = attemptedBytes;
    }

    public long currentBytes() {
        return currentBytes;
    }

    public long maxBytes() {
        return maxBytes;
    }

    public long attemptedBytes() {
        return attemptedBytes;
    }
}
