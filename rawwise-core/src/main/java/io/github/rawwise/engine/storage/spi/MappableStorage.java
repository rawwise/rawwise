package io.github.rawwise.engine.storage.spi;

import java.io.IOException;

public interface MappableStorage extends Storage {

    MappedRegion map(StoragePath path, long offset, long length, AccessMode mode)
            throws IOException;
}
