package io.github.rawwise.engine.storage.reader;

import java.io.IOException;

public interface IteratorReader<T> extends Reader {
    boolean hasNext() throws IOException;

    T next() throws IOException;

    long recordsRead();
}
