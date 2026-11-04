package io.github.rawwise.engine.storage.writer;

import io.github.rawwise.engine.storage.manifest.ManifestListWriter;
import io.github.rawwise.engine.storage.spi.Storage;
import java.io.IOException;

public final class ManifestListWriterFactory implements WriterFactory<ManifestListWriter> {

    @Override
    public String writerType() {
        return "MANIFEST_LIST";
    }

    @Override
    public Class<ManifestListWriter> writerClass() {
        return ManifestListWriter.class;
    }

    @Override
    public ManifestListWriter create(WriterConfig config, WriterContext context)
            throws IOException {
        Storage storage = context.getRequired("storage", Storage.class);
        return new ManifestListWriter(storage);
    }
}
