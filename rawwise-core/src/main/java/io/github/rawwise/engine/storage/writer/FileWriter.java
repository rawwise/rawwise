package io.github.rawwise.engine.storage.writer;

import io.github.rawwise.engine.storage.spi.Storage;

public interface FileWriter<T> extends RecordWriter<T> {
    Storage.StoragePath filePath();

    long length();
}
