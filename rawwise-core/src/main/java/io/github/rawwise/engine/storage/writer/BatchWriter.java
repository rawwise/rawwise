package io.github.rawwise.engine.storage.writer;

import java.io.IOException;
import java.util.Collection;

public interface BatchWriter<T> extends Writer {
    BatchWriteResult batchWrite(Collection<T> records) throws IOException;
}
