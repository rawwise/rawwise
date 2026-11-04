package io.github.rawwise.engine.storage.block.read;

import io.github.rawwise.engine.storage.reader.ReaderConfig;
import io.github.rawwise.engine.storage.reader.ReaderContext;
import io.github.rawwise.engine.storage.reader.ReaderFactory;
import io.github.rawwise.engine.storage.reader.ReaderType;
import java.io.IOException;

public class OrcBlockReaderFactory implements ReaderFactory<OrcBlockReader> {

    @Override
    public ReaderType readerType() {
        return ReaderType.BLOCK_ORC;
    }

    @Override
    public Class<OrcBlockReader> readerClass() {
        return OrcBlockReader.class;
    }

    @Override
    public OrcBlockReader create(ReaderConfig config, ReaderContext context) throws IOException {
        return new OrcBlockReader(context.metrics());
    }
}
