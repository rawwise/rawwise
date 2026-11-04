package io.github.rawwise.engine.storage.writer;

import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.table.key.DefaultKeyCodec;
import io.github.rawwise.model.record.WriteEntry;
import java.io.IOException;

public final class LmdbWriterFactory implements WriterFactory<RecordWriter<WriteEntry>> {

    @Override
    public String writerType() {
        return "LMDB";
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Class<RecordWriter<WriteEntry>> writerClass() {
        return (Class) LmdbWriter.class;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public RecordWriter<WriteEntry> create(WriterConfig config, WriterContext context)
            throws IOException {
        LmdbInstance instance = context.getRequired("lmdbInstance", LmdbInstance.class);
        DynamicMmapStore store = context.get("dynamicMmapStore", DynamicMmapStore.class);
        return (RecordWriter) new LmdbWriter(instance, store, new DefaultKeyCodec());
    }
}
