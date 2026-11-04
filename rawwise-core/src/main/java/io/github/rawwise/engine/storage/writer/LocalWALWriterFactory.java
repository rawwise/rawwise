package io.github.rawwise.engine.storage.writer;

import io.github.rawwise.backend.wal.LocalWALWriter;
import java.io.IOException;

public final class LocalWALWriterFactory implements WriterFactory<LocalWALWriter> {

    @Override
    public String writerType() {
        return "WAL_LOCAL";
    }

    @Override
    public Class<LocalWALWriter> writerClass() {
        return LocalWALWriter.class;
    }

    @Override
    public LocalWALWriter create(WriterConfig config, WriterContext context) throws IOException {
        String storeId = config.extraProp("storeId");
        if (storeId == null) storeId = "default";
        String baseDir = config.filePath();
        if (baseDir == null || baseDir.isEmpty()) {
            throw new IllegalArgumentException("filePath (baseDir) is required for LocalWALWriter");
        }
        long maxSizeBytes = config.maxFileSize().orElse(100L * 1024 * 1024);
        long syncIntervalMs = config.syncIntervalMs().orElse(1000L);
        return new LocalWALWriter(storeId, baseDir, maxSizeBytes, syncIntervalMs);
    }
}
