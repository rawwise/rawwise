package io.github.rawwise.engine.storage.block.read;

import io.github.rawwise.engine.storage.reader.ReaderConfig;
import io.github.rawwise.engine.storage.reader.ReaderContext;
import io.github.rawwise.engine.storage.reader.ReaderFactory;
import io.github.rawwise.engine.storage.reader.ReaderFactoryBootstrap;
import io.github.rawwise.engine.storage.reader.ReaderFactoryRegistry;
import io.github.rawwise.engine.storage.reader.ReaderType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ColdDataBlockReaderFactory implements ReaderFactory<ColdDataBlockReader> {

    @Override
    public ReaderType readerType() {
        return ReaderType.BLOCK_COLD;
    }

    @Override
    public Class<ColdDataBlockReader> readerClass() {
        return ColdDataBlockReader.class;
    }

    @Override
    public ColdDataBlockReader create(ReaderConfig config, ReaderContext context)
            throws IOException {
        List<DataBlockReader> readers = context.get("coldReaders", List.class);
        if (readers != null) {
            return new ColdDataBlockReader(readers);
        }
        ReaderFactoryBootstrap.ensureInitialized();
        ReaderFactoryRegistry registry = ReaderFactoryRegistry.getInstance();
        List<DataBlockReader> defaultReaders = new ArrayList<>();
        defaultReaders.add(
                registry.create(
                        ReaderType.BLOCK_PARQUET,
                        ParquetBlockReader.class,
                        ReaderConfig.builder(ReaderType.BLOCK_PARQUET).build(),
                        context));
        defaultReaders.add(
                registry.create(
                        ReaderType.BLOCK_ORC,
                        OrcBlockReader.class,
                        ReaderConfig.builder(ReaderType.BLOCK_ORC).build(),
                        context));
        return new ColdDataBlockReader(defaultReaders);
    }
}
