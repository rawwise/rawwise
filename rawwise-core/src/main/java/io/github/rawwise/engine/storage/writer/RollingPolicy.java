package io.github.rawwise.engine.storage.writer;

import java.io.IOException;

public interface RollingPolicy {
    boolean shouldRoll(long currentFileSize, long incomingBytes);

    void onRollStart() throws IOException;

    void onRollComplete() throws IOException;

    default void onRollFailed(IOException cause) {}
}
