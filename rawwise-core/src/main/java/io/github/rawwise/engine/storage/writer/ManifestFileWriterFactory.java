package io.github.rawwise.engine.storage.writer;

import io.github.rawwise.engine.storage.manifest.ManifestFileWriter;
import io.github.rawwise.engine.storage.spi.Storage;
import java.io.IOException;

public final class ManifestFileWriterFactory implements WriterFactory<ManifestFileWriter> {

    @Override
    public String writerType() {
        return "MANIFEST_FILE";
    }

    @Override
    public Class<ManifestFileWriter> writerClass() {
        return ManifestFileWriter.class;
    }

    @Override
    public ManifestFileWriter create(WriterConfig config, WriterContext context)
            throws IOException {
        Storage storage = context.getRequired("storage", Storage.class);
        return new ManifestFileWriter(storage);
    }
}
