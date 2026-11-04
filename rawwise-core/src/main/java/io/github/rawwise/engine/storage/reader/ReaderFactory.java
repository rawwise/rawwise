package io.github.rawwise.engine.storage.reader;

import java.io.IOException;

public interface ReaderFactory<R extends Reader> {
    ReaderType readerType();

    Class<R> readerClass();

    R create(ReaderConfig config, ReaderContext context) throws IOException;
}
