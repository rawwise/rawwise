package io.github.rawwise.engine.storage.writer;

import java.io.IOException;

public interface WriterFactory<W extends Writer> {
    String writerType();

    Class<W> writerClass();

    W create(WriterConfig config, WriterContext context) throws IOException;
}
