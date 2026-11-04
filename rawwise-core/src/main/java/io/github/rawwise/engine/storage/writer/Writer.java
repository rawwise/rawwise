package io.github.rawwise.engine.storage.writer;

import java.io.IOException;

public interface Writer extends AutoCloseable {
    void flush() throws IOException;

    boolean isClosed();

    @Override
    void close() throws IOException;
}
