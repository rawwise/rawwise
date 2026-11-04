package io.github.rawwise.engine.storage.manifest;

import io.github.rawwise.engine.storage.reader.ReaderConfig;
import io.github.rawwise.engine.storage.reader.ReaderContext;
import io.github.rawwise.engine.storage.reader.ReaderFactory;
import io.github.rawwise.engine.storage.reader.ReaderType;
import io.github.rawwise.engine.storage.spi.Storage;
import java.io.IOException;

public class ManifestFileReaderFactory implements ReaderFactory<ManifestFileReader> {

    @Override
    public ReaderType readerType() {
        return ReaderType.MANIFEST_FILE;
    }

    @Override
    public Class<ManifestFileReader> readerClass() {
        return ManifestFileReader.class;
    }

    @Override
    public ManifestFileReader create(ReaderConfig config, ReaderContext context)
            throws IOException {
        Storage storage = context.getRequired("storage", Storage.class);
        return new ManifestFileReader(storage);
    }
}
