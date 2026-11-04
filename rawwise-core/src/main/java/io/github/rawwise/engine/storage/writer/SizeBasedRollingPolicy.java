package io.github.rawwise.engine.storage.writer;

public final class SizeBasedRollingPolicy implements RollingPolicy {
    private final long maxFileSize;

    public SizeBasedRollingPolicy(long maxFileSize) {
        if (maxFileSize <= 0) {
            throw new IllegalArgumentException("maxFileSize must be positive, got: " + maxFileSize);
        }
        this.maxFileSize = maxFileSize;
    }

    @Override
    public boolean shouldRoll(long currentFileSize, long incomingBytes) {
        return currentFileSize + incomingBytes > maxFileSize && currentFileSize > 0;
    }

    @Override
    public void onRollStart() {}

    @Override
    public void onRollComplete() {}
}
