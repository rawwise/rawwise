package io.github.rawwise.engine.storage.table.read;

import io.github.rawwise.engine.storage.block.read.LmdbDataBlockReader;
import io.github.rawwise.engine.storage.reader.ReaderConfig;
import io.github.rawwise.engine.storage.reader.ReaderContext;
import io.github.rawwise.engine.storage.reader.ReaderFactory;
import io.github.rawwise.engine.storage.reader.ReaderType;
import io.github.rawwise.engine.storage.table.TableStore;
import java.io.IOException;

public class LmdbTableReaderFactory implements ReaderFactory<LmdbTableReader> {

    @Override
    public ReaderType readerType() {
        return ReaderType.TABLE_LMDB;
    }

    @Override
    public Class<LmdbTableReader> readerClass() {
        return LmdbTableReader.class;
    }

    @Override
    public LmdbTableReader create(ReaderConfig config, ReaderContext context) throws IOException {
        TableStore tableStore = context.getRequired("tableStore", TableStore.class);
        LmdbDataBlockReader dataBlockReader =
                context.get("lmdbDataBlockReader", LmdbDataBlockReader.class);
        if (dataBlockReader != null) {
            return new LmdbTableReader(tableStore, dataBlockReader);
        }
        return new LmdbTableReader(tableStore);
    }
}
