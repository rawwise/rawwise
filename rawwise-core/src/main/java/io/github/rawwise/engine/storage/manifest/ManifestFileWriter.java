package io.github.rawwise.engine.storage.manifest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.engine.storage.writer.ManifestWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ManifestFileWriter implements ManifestWriter {

    private static final ObjectMapper MAPPER = ManifestJsonCodecs.createObjectMapper();

    private final Storage storage;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ManifestFileWriter(Storage storage) {
        this.storage = storage;
    }

    public void write(ManifestFile manifest, Storage.StoragePath path) throws IOException {
        if (closed.get()) throw new IllegalStateException("Writer is closed");
        storage.createDirectories(new Storage.StoragePath(path.scheme(), dirPath(path.location())));

        byte[] jsonBytes;
        try {
            jsonBytes = MAPPER.writeValueAsBytes(manifest);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IOException("Failed to serialize manifest file", e);
        }

        Storage.OutputHandle out = null;
        try {
            out = storage.openWrite(path, Storage.WriteMode.OVERWRITE);
            OutputStream stream = out.stream();
            stream.write(jsonBytes);
            stream.flush();
            out.sync();
        } catch (IOException e) {
            throw new IOException("Failed to write manifest file to " + path, e);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (Exception e) {
                    log.warn("Failed to close output handle for manifest file: {}", path, e);
                }
            }
        }
    }

    @Override
    public void writeManifest(Object manifest, Storage.StoragePath path) throws IOException {
        if (manifest instanceof ManifestFile mf) {
            write(mf, path);
        } else {
            throw new IllegalArgumentException(
                    "Expected ManifestFile, got: " + manifest.getClass().getName());
        }
    }

    @Override
    public void flush() throws IOException {}

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws IOException {
        closed.set(true);
    }

    private static String dirPath(String filePath) {
        int lastSlash = filePath.lastIndexOf('/');
        return lastSlash > 0 ? filePath.substring(0, lastSlash) : "";
    }
}
