package io.github.rawwise.engine.storage.block.read;

import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.storage.reader.ReaderConfig;
import io.github.rawwise.engine.storage.reader.ReaderContext;
import io.github.rawwise.engine.storage.reader.ReaderFactory;
import io.github.rawwise.engine.storage.reader.ReaderType;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import io.github.rawwise.model.record.RecordCodec;
import java.io.IOException;

public class ParquetBlockReaderFactory implements ReaderFactory<ParquetBlockReader> {

    @Override
    public ReaderType readerType() {
        return ReaderType.BLOCK_PARQUET;
    }

    @Override
    public Class<ParquetBlockReader> readerClass() {
        return ParquetBlockReader.class;
    }

    @Override
    public ParquetBlockReader create(ReaderConfig config, ReaderContext context)
            throws IOException {
        KeyCodec keyCodec = context.get("keyCodec", KeyCodec.class);
        ParquetBlockReader reader;
        if (keyCodec != null) {
            reader = new ParquetBlockReader(keyCodec, context.metrics());
        } else {
            reader = new ParquetBlockReader(context.metrics());
        }

        // Wire RecordCodec + SchemaResolver for body-codec-aware decode
        RecordCodec recordCodec = context.get("recordCodec", RecordCodec.class);
        RecordCodec.SchemaResolver schemaResolver =
                context.get("schemaResolver", RecordCodec.SchemaResolver.class);
        if (recordCodec != null && schemaResolver != null) {
            reader.withRecordCodec(recordCodec, schemaResolver);
        }

        // Wire SnapshotManager for historical Schema resolution from footer metadata
        SnapshotManager snapshotManager = context.get("snapshotManager", SnapshotManager.class);
        if (snapshotManager != null) {
            reader.setSnapshotManager(snapshotManager);
        }

        // Wire TableStore for lazy SnapshotManager access at read time
        TableStore tableStore = context.get("tableStore", TableStore.class);
        if (tableStore != null) {
            reader.setTableStore(tableStore);
        }

        return reader;
    }
}
