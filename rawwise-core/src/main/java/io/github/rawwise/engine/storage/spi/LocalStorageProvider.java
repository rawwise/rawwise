package io.github.rawwise.engine.storage.spi;

import io.github.rawwise.governance.config.RawWiseConfig;
import java.io.IOException;

public class LocalStorageProvider implements StorageProvider {

    @Override
    public String scheme() {
        return "file";
    }

    @Override
    public Storage create(Storage.StoragePath path, RawWiseConfig config) throws IOException {
        return new LocalStorage(path.location());
    }
}
