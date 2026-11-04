package io.github.rawwise.engine.storage.reader;

import java.io.IOException;

public interface Reader extends AutoCloseable {
    boolean isClosed();

    @Override
    void close() throws IOException;
}
