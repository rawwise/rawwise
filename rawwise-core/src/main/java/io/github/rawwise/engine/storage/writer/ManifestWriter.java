package io.github.rawwise.engine.storage.writer;

import io.github.rawwise.engine.storage.spi.Storage;
import java.io.IOException;

public interface ManifestWriter extends Writer {
    void writeManifest(Object manifest, Storage.StoragePath path) throws IOException;
}
