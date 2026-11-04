package io.github.rawwise.model.op;

import io.github.rawwise.engine.storage.writer.RecordWriter;
import io.github.rawwise.model.record.WriteEntry;
import java.util.Properties;

public class AppendOnlyWrite extends BatchWrite {
    public static final OpType TYPE = OpType.APPEND_ONLY_WRITE;

    public AppendOnlyWrite(
            java.util.List<WriteEntry> records, RecordWriter<?> writer, Properties config) {
        super(records, writer, config);
    }

    @Override
    public OpType type() {
        return TYPE;
    }
}
