package io.github.rawwise.backend.lazyupdate;

import io.github.rawwise.engine.storage.reader.ReaderConfig;
import io.github.rawwise.engine.storage.reader.ReaderContext;
import io.github.rawwise.engine.storage.reader.ReaderFactory;
import io.github.rawwise.engine.storage.reader.ReaderType;
import java.io.IOException;

public class UpdateLogReaderFactory implements ReaderFactory<UpdateLogReader> {

    @Override
    public ReaderType readerType() {
        return ReaderType.UPDATE_LOG;
    }

    @Override
    public Class<UpdateLogReader> readerClass() {
        return UpdateLogReader.class;
    }

    @Override
    public UpdateLogReader create(ReaderConfig config, ReaderContext context) throws IOException {
        return new UpdateLogReader();
    }
}
