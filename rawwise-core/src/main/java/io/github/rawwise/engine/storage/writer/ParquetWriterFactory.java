package io.github.rawwise.engine.storage.writer;

import java.io.IOException;

public final class ParquetWriterFactory implements WriterFactory<RawWiseParquetWriter> {

    @Override
    public String writerType() {
        return "PARQUET";
    }

    @Override
    public Class<RawWiseParquetWriter> writerClass() {
        return RawWiseParquetWriter.class;
    }

    @Override
    public RawWiseParquetWriter create(WriterConfig config, WriterContext context)
            throws IOException {
        String filePath = config.filePath();
        if (filePath == null || filePath.isEmpty()) {
            throw new IllegalArgumentException("filePath is required for ParquetWriter");
        }
        return new RawWiseParquetWriter(filePath, null);
    }
}
