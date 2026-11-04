package io.github.rawwise.engine.storage.table.read;

import io.github.rawwise.engine.storage.block.read.DataBlockReader;
import io.github.rawwise.engine.storage.reader.ReaderConfig;
import io.github.rawwise.engine.storage.reader.ReaderContext;
import io.github.rawwise.engine.storage.reader.ReaderFactory;
import io.github.rawwise.engine.storage.reader.ReaderType;
import io.github.rawwise.engine.storage.table.TableStore;
import java.io.IOException;
import java.util.List;

public class BlockRoutingTableReaderFactory implements ReaderFactory<BlockRoutingTableReader> {

    @Override
    public ReaderType readerType() {
        return ReaderType.TABLE_ROUTING;
    }

    @Override
    public Class<BlockRoutingTableReader> readerClass() {
        return BlockRoutingTableReader.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public BlockRoutingTableReader create(ReaderConfig config, ReaderContext context)
            throws IOException {
        TableStore tableStore = context.getRequired("tableStore", TableStore.class);
        List<DataBlockReader> readers = context.getRequired("dataBlockReaders", List.class);
        return new BlockRoutingTableReader(tableStore, readers);
    }
}
