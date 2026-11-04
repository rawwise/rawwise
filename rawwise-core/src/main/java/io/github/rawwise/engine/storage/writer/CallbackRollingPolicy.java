package io.github.rawwise.engine.storage.writer;

import java.io.IOException;
import java.util.Objects;

public final class CallbackRollingPolicy implements RollingPolicy {
    private final RollingPolicy delegate;
    private final ThrowingRunnable beforeRoll;
    private final ThrowingRunnable afterRoll;

    public CallbackRollingPolicy(
            RollingPolicy delegate, ThrowingRunnable beforeRoll, ThrowingRunnable afterRoll) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.beforeRoll = Objects.requireNonNull(beforeRoll, "beforeRoll must not be null");
        this.afterRoll = Objects.requireNonNull(afterRoll, "afterRoll must not be null");
    }

    @Override
    public boolean shouldRoll(long currentFileSize, long incomingBytes) {
        return delegate.shouldRoll(currentFileSize, incomingBytes);
    }

    @Override
    public void onRollStart() throws IOException {
        delegate.onRollStart();
        beforeRoll.run();
    }

    @Override
    public void onRollComplete() throws IOException {
        afterRoll.run();
        delegate.onRollComplete();
    }

    @Override
    public void onRollFailed(IOException cause) {
        delegate.onRollFailed(cause);
    }
}
