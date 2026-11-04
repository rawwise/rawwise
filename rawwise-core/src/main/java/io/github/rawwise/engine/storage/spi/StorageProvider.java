package io.github.rawwise.engine.storage.spi;

import io.github.rawwise.governance.config.RawWiseConfig;
import java.io.IOException;

public interface StorageProvider {
    String scheme();

    Storage create(Storage.StoragePath path, RawWiseConfig config) throws IOException;
}
