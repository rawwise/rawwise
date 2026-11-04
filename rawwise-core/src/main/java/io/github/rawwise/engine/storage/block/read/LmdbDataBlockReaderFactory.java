package io.github.rawwise.engine.storage.block.read;

import io.github.rawwise.engine.storage.codec.BodyCodec;
import io.github.rawwise.engine.storage.reader.ReaderConfig;
import io.github.rawwise.engine.storage.reader.ReaderContext;
import io.github.rawwise.engine.storage.reader.ReaderFactory;
import io.github.rawwise.engine.storage.reader.ReaderType;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import io.github.rawwise.model.record.RecordCodec;
import java.io.IOException;

public class LmdbDataBlockReaderFactory implements ReaderFactory<LmdbDataBlockReader> {

    @Override
    public ReaderType readerType() {
        return ReaderType.BLOCK_LMDB;
    }

    @Override
    public Class<LmdbDataBlockReader> readerClass() {
        return LmdbDataBlockReader.class;
    }

    @Override
    public LmdbDataBlockReader create(ReaderConfig config, ReaderContext context)
            throws IOException {
        TableStore tableStore = context.getRequired("tableStore", TableStore.class);
        KeyCodec keyCodec = context.get("keyCodec", KeyCodec.class);
        LmdbDataBlockReader reader;
        if (keyCodec != null) {
            reader = new LmdbDataBlockReader(tableStore, keyCodec);
        } else {
            reader = new LmdbDataBlockReader(tableStore);
        }
        // Wire BodyCodec + SchemaResolver for projection-aware decode
        BodyCodec bodyCodec = context.get("bodyCodec", BodyCodec.class);
        RecordCodec.SchemaResolver schemaResolver =
                context.get("schemaResolver", RecordCodec.SchemaResolver.class);
        if (bodyCodec != null && schemaResolver != null) {
            reader.setBodyCodec(bodyCodec, schemaResolver);
        }
        return reader;
    }
}
