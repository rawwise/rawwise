package io.github.rawwise.backend.wal;

import io.github.rawwise.engine.storage.reader.BulkReader;
import io.github.rawwise.engine.storage.spi.Storage;

public interface WALReader extends BulkReader<WALRecord> {
    Storage.StoragePath walBasePath();
}
