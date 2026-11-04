package io.github.rawwise.engine.storage.reader;

import io.github.rawwise.backend.lazyupdate.UpdateLogReaderFactory;
import io.github.rawwise.backend.wal.NwlWALReaderFactory;
import io.github.rawwise.engine.compaction.ParquetSortedFileReaderFactory;
import io.github.rawwise.engine.storage.block.read.ColdDataBlockReaderFactory;
import io.github.rawwise.engine.storage.block.read.LmdbDataBlockReaderFactory;
import io.github.rawwise.engine.storage.block.read.OrcBlockReaderFactory;
import io.github.rawwise.engine.storage.block.read.ParquetBlockReaderFactory;
import io.github.rawwise.engine.storage.manifest.ManifestFileReaderFactory;
import io.github.rawwise.engine.storage.manifest.ManifestListReaderFactory;
import io.github.rawwise.engine.storage.table.read.BlockRoutingTableReaderFactory;
import io.github.rawwise.engine.storage.table.read.LmdbTableReaderFactory;

public final class ReaderFactoryBootstrap {

    private ReaderFactoryBootstrap() {}

    public static synchronized void ensureInitialized() {
        ReaderFactoryRegistry registry = ReaderFactoryRegistry.getInstance();
        if (registry.hasFactory(ReaderType.BLOCK_PARQUET)) {
            return;
        }
        registry.registerIfAbsent(new ParquetBlockReaderFactory());
        registry.registerIfAbsent(new OrcBlockReaderFactory());
        registry.registerIfAbsent(new ColdDataBlockReaderFactory());
        registry.registerIfAbsent(new LmdbDataBlockReaderFactory());
        registry.registerIfAbsent(new LmdbTableReaderFactory());
        registry.registerIfAbsent(new BlockRoutingTableReaderFactory());
        registry.registerIfAbsent(new ManifestFileReaderFactory());
        registry.registerIfAbsent(new ManifestListReaderFactory());
        registry.registerIfAbsent(new NwlWALReaderFactory());
        registry.registerIfAbsent(new UpdateLogReaderFactory());
        registry.registerIfAbsent(new ParquetSortedFileReaderFactory());
    }

    public static synchronized void reset() {
        ReaderFactoryRegistry.getInstance().clear();
    }

    public static synchronized boolean isInitialized() {
        return ReaderFactoryRegistry.getInstance().hasFactory(ReaderType.BLOCK_PARQUET);
    }
}
