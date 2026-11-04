package io.github.rawwise.model.op;

import io.github.rawwise.engine.storage.writer.RecordWriter;
import io.github.rawwise.model.record.RawWiseTable;
import java.util.Properties;

public interface WriteBuilder<W extends Write, B extends WriteBuilder<W, B>>
        extends TableOpBuilder<Write.Result, W> {
    B withTable(RawWiseTable table);

    @Override
    W build();

    B withWriter(RecordWriter<?> writer);

    default B withConfig(Properties config) {
        throw new UnsupportedOperationException("Config is not supported by this builder");
    }

    @Override
    RawWiseTable table();
}
