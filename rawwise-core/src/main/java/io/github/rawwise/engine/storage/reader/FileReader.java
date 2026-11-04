package io.github.rawwise.engine.storage.reader;

import io.github.rawwise.engine.storage.spi.Storage;

public interface FileReader<T> extends IteratorReader<T> {
    Storage.StoragePath filePath();

    long length();
}
