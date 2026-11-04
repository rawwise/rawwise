package io.github.rawwise.engine.storage.manifest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.engine.storage.reader.AbstractReader;
import io.github.rawwise.engine.storage.spi.Storage;
import java.io.IOException;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ManifestListReader extends AbstractReader {

    private static final ObjectMapper MAPPER = ManifestJsonCodecs.createObjectMapper();

    private final Storage storage;

    public ManifestListReader(Storage storage) {
        this.storage = storage;
    }

    public ManifestList read(Storage.StoragePath path) throws IOException {
        ensureOpen();
        Storage.InputHandle in = storage.openRead(path);
        try {
            InputStream stream = in.stream();
            return MAPPER.readValue(stream, ManifestList.class);
        } finally {
            try {
                in.close();
            } catch (Exception e) {
                log.warn("Failed to close input handle for manifest list: {}", path, e);
            }
        }
    }
}
