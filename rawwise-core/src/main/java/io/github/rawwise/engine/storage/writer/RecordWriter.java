package io.github.rawwise.engine.storage.writer;

import java.io.IOException;

public interface RecordWriter<T> extends Writer {
    WriteResult write(T record) throws IOException;

    long recordsWritten();

    long recordsFailed();
}
