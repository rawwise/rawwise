package io.github.rawwise.backend.wal;

import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.engine.storage.writer.RecordWriter;
import java.io.IOException;
import java.util.Optional;

public interface WALWriter extends RecordWriter<WALRecord> {
    void forceSync() throws IOException;

    long writtenBytes();

    long committedLSN();

    Optional<Storage.StoragePath> currentFilePath();

    void roll() throws IOException;
}
