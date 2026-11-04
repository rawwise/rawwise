package io.github.rawwise.engine.storage.reader;

import java.io.IOException;

public interface CursorReader extends Reader {
    boolean hasNext() throws IOException;

    void advance() throws IOException;

    long recordsRead();
}
