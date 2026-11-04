package io.github.rawwise.engine.storage.writer;

import java.io.IOException;

@FunctionalInterface
public interface ThrowingRunnable {
    void run() throws IOException;
}
